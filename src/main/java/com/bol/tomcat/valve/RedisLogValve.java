package com.bol.tomcat.valve;

import org.apache.catalina.LifecycleState;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.Valve;
import org.apache.catalina.connector.Request;
import org.apache.catalina.connector.Response;
import org.apache.catalina.valves.ValveBase;
import org.apache.juli.logging.Log;
import org.json.simple.JSONObject;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.util.SafeEncoder;

import javax.servlet.ServletException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;

import java.util.Arrays;
import java.util.Queue;
import java.util.Map;
import java.util.HashMap;
import java.util.Enumeration;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;


public class RedisLogValve extends ValveBase implements Runnable, org.apache.catalina.Valve {

    protected boolean enabled = true;

    // configs
    private String redisHost = "localhost";
    private int redisPort = 6379;
    private String key;
    private String password;
    private Map<String, String> userFields = new HashMap<>();
    private int batchSize = 100;
    private int queueSize = 5000;
    private long flushInterval = 500;
    private boolean alwaysBatch = true;
    private boolean purgeOnFailure = true;
    private long waitTerminate = 1000;

    // runtime
    private String hostname;
    private String version;
    private Queue<JSONObject> events;
    private int messageIndex;
    private byte[][] batch;
    private Jedis jedis;
    private ScheduledExecutorService executor;
    private ScheduledFuture<?> task;

    // metrics
    private int eventCounter = 0;
    private int eventsDroppedInQueueing = 0;
    private int eventsDroppedInPush = 0;
    private int connectCounter = 0;
    private int connectFailures = 0;
    private int batchPurges = 0;
    private int eventsPushed = 0;


    @Override
    public Valve getNext() {
        return this.next;
    }

    @Override
    public void invoke(Request req, Response resp) throws IOException, ServletException {

        // Copied from org/apache/catalina/valves/AccessLogValve.java (7.0.0)

        final String t1Name = RedisLogValve.class.getName()+".t1";
        if (getState().isAvailable() && getEnabled()) {
            // Pass this request on to the next valve in our pipeline
            long t1 = System.currentTimeMillis();
            boolean asyncdispatch = req.isAsyncDispatching();
            if (!asyncdispatch) {
                req.setAttribute(t1Name, new Long(t1));
            }

            getNext().invoke(req, resp);

            //we're not done with the request
            if (req.isAsyncDispatching()) {
                return;
            } else if (asyncdispatch && req.getAttribute(t1Name)!=null) {
                t1 = ((Long)req.getAttribute(t1Name)).longValue();
            }

            long t2 = System.currentTimeMillis();
            long elapsed = t2 - t1;

            try {
                eventCounter++;
                int size = events.size();
                if (size < queueSize) {
                    JSONObject event = getRequestData(req, resp, t1, elapsed);
                    try {
                        events.add(event);
                    } catch (IllegalStateException e) {
                        // safeguard in case multiple threads raced on almost full queue
                        eventsDroppedInQueueing++;
                    }
                } else {
                    eventsDroppedInQueueing++;
                }
            } catch (Exception e) {
                containerLog.error("Error populating event and adding to queue", e);
            }
        } else
            getNext().invoke(req, resp);

    }

    @Override
    protected synchronized void startInternal() throws LifecycleException {
        try {
            Properties pom = new Properties();
            try {
                InputStream pomInputStream = getClass().getResourceAsStream("/META-INF/maven/com.bol.tomcat.valve/RedisLogValve/pom.properties");
                pom.load(pomInputStream);
                pomInputStream.close();
            }catch (Exception e){
                containerLog.warn("Couldn't load pom.properties from classpath.");
            }

            version = pom.getProperty("version","");
            containerLog.info("Start initialization of RedisLogValve" + (version.equals("") ? ".": " with version: " + version));

            this.hostname = getHostname();

            if (key == null || key.length() == 0)
                throw new IllegalStateException("Must set 'key'");
            if (redisHost == null || redisHost.length() == 0)
                throw new IllegalStateException("Must set 'redisHost'");
            if (redisPort == 0)
                throw new IllegalStateException("Must set 'redisPort'");
            if (!(flushInterval > 0))
                throw new IllegalStateException("FlushInterval must be > 0. Configured value: " + flushInterval);
            if (!(queueSize > 0))
                throw new IllegalStateException("QueueSize must be > 0. Configured value: " + queueSize);
            if (!(batchSize > 0))
                throw new IllegalStateException("BatchSize must be > 0. Configured value: " + batchSize);

            containerLog.info("RedisLogValve config: redisAddr: " + getRedisAddress() + ", key: " + this.key + ", alwaysBatch: " + isAlwaysBatch() + ", flushInterval: " + getFlushInterval() + ", batchSize: " + getBatchSize() + ", hostname: " + this.hostname);


            if (executor == null) {
                executor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory(this.getClass().getSimpleName(), true));
            }

            if (task != null && !task.isDone()) task.cancel(true);

            events = new ArrayBlockingQueue<JSONObject>(queueSize);
            batch = new byte[batchSize][];
            messageIndex = 0;

            createJedis();

            task = executor.scheduleWithFixedDelay(this, flushInterval, flushInterval, TimeUnit.MILLISECONDS);

            containerLog.info("Finished initialization of RedisLogValve");
        } catch (Exception ex) {
            containerLog.error("Error starting RedisLogValve ", ex);
        }
        setState(LifecycleState.STARTING);
    }

    protected synchronized void stopInternal() throws LifecycleException {
        setState(LifecycleState.STOPPING);
        try {
            if (task != null) {
                task.cancel(false);
            }
            if (executor != null) {
                executor.shutdown();

                boolean finished = executor.awaitTermination(waitTerminate, TimeUnit.MILLISECONDS);
                if (finished) {
                    if (!events.isEmpty())
                        run();
                    if (messageIndex > 0)
                        push();
                } else {
                    containerLog.warn("Executor did not complete in " + waitTerminate + " milliseconds. Log entries may be lost.");
                }

            }
            safeDisconnect();
            containerLog.info("Stopped RedisLogValve");
        } catch (Exception e) {
            containerLog.error(e.getMessage(), e);
        }
    }

    public String getHostname() {
        InetAddress ip;
        String hostname;
        try {
            ip = InetAddress.getLocalHost();
            hostname = ip.getCanonicalHostName();
            int dotPos = hostname.indexOf('.');
            if (dotPos > 0) {
                hostname = hostname.substring(0, dotPos);
            }
        } catch (UnknownHostException e) {
            hostname = "localhost";
        }
        return hostname;
    }

    public static String getFullURL(Request req) {
        String requestURI = req.getRequestURI();
        String queryString = req.getQueryString();

        if (queryString == null) {
            return requestURI;
        } else {
            return new StringBuilder(requestURI).append("?").append(queryString).toString();
        }
    }

    public JSONObject getRequestData(Request req, Response resp, long start_ts, long elapsed) {
        Map<String, Object> requestData = new HashMap<String, Object>();
        Map<String, Object> requestFields = new HashMap<String, Object>();

        JSONObject json = new JSONObject();

        String host      = req.getHeader("Host");
        String userAgent = req.getHeader("User-Agent");
        String auth      = req.getRemoteUser();
        String referrer  = req.getHeader("Referer");
        String unique_id = req.getHeader("X-RID");  // very bol.com specific
        String fwd_for   = req.getHeader("X-Forwarded-For");
        String ssl       = req.getHeader("X-SSL");

        addField(requestFields, "valve_version", version);
        addField(requestFields, "host_header", host);
        addField(requestFields, "agent", userAgent);
        addField(requestFields, "client", req.getRemoteAddr());
        addField(requestFields, "forwarded_for", fwd_for);
        addField(requestFields, "auth", auth);
        addField(requestFields, "verb", req.getMethod());
        addField(requestFields, "httpversion", req.getProtocol());
        addField(requestFields, "referrer", referrer);
        addField(requestFields, "unique_id", unique_id);
        addField(requestFields, "ssl", ssl);

        addField(requestFields, "timestamp", start_ts);
        addField(requestFields, "time_in_msec", elapsed);
        addField(requestFields, "time_in_sec", elapsed / 1000);
        addField(requestFields, "response", resp.getStatus());

        long length = resp.getBytesWritten(false);
        if (length > 0) {
            addField(requestFields, "bytes", length);
        }

        Enumeration cookies_e = req.getHeaders("Cookie");
        String cookies = null;
        while(cookies_e.hasMoreElements()) {
            if (cookies == null) {
                cookies = (String)cookies_e.nextElement();
            } else {
                cookies += "; " + (String)cookies_e.nextElement();
            }
        }
        addField(requestFields, "cookies", cookies);

        // Why is this always null?
        addField(requestFields, "mime", resp.getContentType());

        addUserFields(requestFields, getUserFields());

        requestData.put("@fields", requestFields);
        requestData.put("@message", getFullURL(req));
        requestData.put("@source_host", this.hostname);

        json.putAll(requestData);
        return json;
    }

    private void addUserFields(Map<String, Object> requestFields, Map<String, String> userFields) {
        for (Map.Entry<String, String> entry : userFields.entrySet()){
            addField(requestFields, entry.getKey(), entry.getValue());
        }
    }

    private void addField(Map<String, Object> fieldsMap, final String field, final int value) {
        fieldsMap.put(field, value);
    }

    private void addField(Map<String, Object> fieldsMap, final String field, final long value) {
        fieldsMap.put(field, value);
    }

    private void addField(Map<String, Object> fieldsMap, final String field, final String value) {
        if (field!= null && value != null && !"-".equals(value)) {
            fieldsMap.put(field, value);
        }
    }

    public void setKey(String key) {
        if (key.length() == 0) {
            throw new IllegalArgumentException("key cannot be empty");
        }
        this.key = key;
    }
    public String getKey() {
        return key;
    }

    public void setRedisHost(String redisHost) {
        if (redisHost.length() == 0) {
            throw new IllegalArgumentException("redisHost cannot be empty");
        }
        this.redisHost = redisHost;
    }

    public String getRedisAddress() {
        return redisHost + ":" + redisPort;
    }

    public String getRedisHost() {
        return redisHost;
    }

    public void setRedisPort(int redisPort) {
        if (redisPort <= 0) {
            throw new IllegalArgumentException("redisPort must be positive");
        }
        this.redisPort = redisPort;
    }
    public int getRedisPort() {
        return redisPort;
    }

    public Log getContainerLog() {
        return containerLog;
    }

    public boolean isPurgeOnFailure() {
        return purgeOnFailure;
    }

    public void setPurgeOnFailure(boolean purgeOnFailure) {
        this.purgeOnFailure = purgeOnFailure;
    }

    public Queue<JSONObject> getEvents() {
        return events;
    }

    public boolean isAlwaysBatch() {
        return alwaysBatch;
    }

    public void setAlwaysBatch(boolean alwaysBatch) {
        this.alwaysBatch = alwaysBatch;
    }

    public void setQueueSize(int queueSize) {
        this.queueSize = queueSize;
    }
    public int getQueueSize() {
        return queueSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }
    public int getBatchSize() {
        return batchSize;
    }

    public void setFlushInterval(long flushInterval) {
        this.flushInterval = flushInterval;
    }
    public long getFlushInterval() {
        return flushInterval;
    }

    public boolean getEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public Map<String, String> getUserFields() {
        return userFields;
    }

    public void setUserFields(String configurationUserFields) {
        if (configurationUserFields != null && configurationUserFields.indexOf(":") > -1) {
            if (configurationUserFields.indexOf(",") > -1) {
                String[] entries = configurationUserFields.split(",");
                for (String entry: entries) {
                    addEntry(userFields, entry);
                }
            } else {
                addEntry(userFields, configurationUserFields);
            }
        }

    }
    private void addEntry(Map<String, String> entries, String entry) {
        if (entry.indexOf(":") != -1) {
            String key = entry.substring(0, entry.indexOf(":"));
            String value = entry.substring(entry.indexOf(":") + 1, entry.length());
            entries.put(key, value);
        }
    }

    protected void createJedis() {
        if (jedis != null && jedis.isConnected()) {
            jedis.disconnect();
        }
        jedis = new Jedis(this.redisHost, this.redisPort);
    }

    protected void safeDisconnect() {
        try {
            jedis.disconnect();
        } catch (Exception e) {
            containerLog.warn("Disconnect failed to Redis at " + getRedisAddress());
        }
    }

    protected boolean connect() {
        try {
            if (!jedis.isConnected()) {
                containerLog.debug("Connecting to Redis at " + getRedisAddress());
                connectCounter++;
                jedis.connect();

                if (password != null) {
                    String result = jedis.auth(password);
                    if (!"OK".equals(result)) {
                        containerLog.error("Error authenticating with Redis at " + getRedisAddress());
                    }
                }

                // make sure we got a live connection
                jedis.ping();
            }
            return true;
        } catch (Exception e) {
            connectFailures++;
            containerLog.error("Error connecting to Redis at " + getRedisAddress() + ": " + e.getMessage());
            return false;
        }
    }

    public void run() {

        if (!connect()) {
            return;
        }

        try {
            if (messageIndex == batchSize) push();

            JSONObject event;
            while ((event = events.poll()) != null) {
                try {
                    batch[messageIndex++] = SafeEncoder.encode(event.toString());
                } catch (Exception e) {
                    containerLog.error(event, e);
                }

                if (messageIndex == batchSize) push();
            }

            if (!alwaysBatch && messageIndex > 0) {
                // push incomplete batches
                push();
            }

        } catch (JedisException je) {

            containerLog.debug("Can't push " + messageIndex + " events to Redis. Reconnecting for retry.", je);
            safeDisconnect();

        } catch (Exception e) {
            containerLog.error("Can't push events to Redis", e);
        }
    }

    protected boolean push() {
        containerLog.debug("Sending " + messageIndex + " log messages to Redis at " + getRedisAddress());
        try {

            jedis.rpush(SafeEncoder.encode(key),
                batchSize == messageIndex
                    ? batch
                    : Arrays.copyOf(batch, messageIndex));

            eventsPushed += messageIndex;
            messageIndex = 0;
            return true;

        } catch (JedisDataException jde) {
            // Handling stuff like OOM's on Redis' side
            if (purgeOnFailure) {
                containerLog.error("Can't push events to Redis: " + jde.getMessage());
                eventsDroppedInPush += messageIndex;
                batchPurges++;
                messageIndex = 0;
            }
            return false;
        }
    }



    public int getEventCounter() { return eventCounter; }
    public int getEventsDroppedInQueueing() { return eventsDroppedInQueueing; }
    public int getEventsDroppedInPush() { return eventsDroppedInPush; }
    public int getConnectCounter() { return connectCounter; }
    public int getConnectFailures() { return connectFailures; }
    public int getBatchPurges() { return batchPurges; }
    public int getEventsPushed() { return eventsPushed; }
    public int getEventQueueSize() { return events.size(); }

}
