package com.bol.tomcat.valve;

import org.junit.Assert;
import org.junit.Test;
import redis.clients.util.SafeEncoder;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class RedisLogValveTest {


    private RedisLogValve valve = new RedisLogValve();

    @Test
    public void checkUserFieldsTwoElements(){
        valve.setUserFields("field1:value1,field2:value2");
        Assert.assertEquals(valve.getUserFields().size(), 2);
    }

    @Test
    public void checkOneUserField(){
        valve.setUserFields("field1:value1");
        Assert.assertEquals(valve.getUserFields().size(), 1);
        Map<String, String> userFields = valve.getUserFields();
        Assert.assertEquals(userFields.get("field1"), "value1");
    }

    @Test
    public void checkNoUserFields(){
        valve.setUserFields(null);
        Assert.assertEquals(valve.getUserFields().size(), 0);
        Map<String, String> userFields = valve.getUserFields();
        Assert.assertEquals(userFields.get("field1"), null);
    }

    @Test
    public void testLoop() {
        List<Integer> list = Arrays.asList(1,2,3,4,5,6,7,8,9,10,11);
        int batchSize = 10;
        byte[][] batch = new byte[batchSize][];
        AtomicInteger messageIndex = new AtomicInteger(0);
        Iterator<Integer> it = list.iterator();
        while (it.hasNext()) {
            try {
                batch[messageIndex.getAndIncrement()] = new byte[]{it.next().byteValue()};
            } catch (Exception e) {
            }

            if (messageIndex.get() == batchSize) {
                messageIndex.set(0);
            }
        }
        Assert.assertEquals(messageIndex.get(), 1);
    }
}
