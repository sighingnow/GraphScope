package com.alibaba.graphscope.utils;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.graphscope.graphx.FragmentRDD;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FragmentRegistry {
    private static Logger logger = LoggerFactory.getLogger(FragmentRegistry.class.getName());
    private static AtomicInteger partition = new AtomicInteger(0);
    private static String hostName;
    private static String fragId;
    private static ReentrantLock lock = new ReentrantLock();
    private static FragmentRDD fragmentRDD;

    static {
        try {
            hostName = getSelfHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    public static int registFragment(String fragIds){
        JSONObject jsonObject = JSONObject.parseObject(fragIds);

        synchronized (FragmentRegistry.class){
            if (fragId == null){
                if (jsonObject.containsKey(hostName)){
                    FragmentRegistry.fragId = jsonObject.getString(hostName);
                    logger.info("on host {} get frag id{}", hostName, fragId);
                }
            }
        }
        int partitionId = partition.getAndAdd(1);
        return partitionId;
    }

    /**
     * For each partition/thread, this function should only run once.
     */
    public static void constructFragment(int pid, String fragName){
        if (!lock.isLocked()){
            if (lock.tryLock()){
                logger.info("partition {} successfully got lock", pid);
                if (fragmentRDD != null){
                    throw new IllegalStateException("Impossible: fragment rdd has been constructed" + fragmentRDD);
                }
                if (fragId == null || fragId.isEmpty()){
                    throw new IllegalStateException("Please register fragment first");
                }
                fragmentRDD = FragmentRDD.create(fragId, fragName, partition.get());
                logger.info("Successfully create fragment RDD");
            }
            else {
                logger.info("partition {} try to get lock failed", pid);
            }
        }
        else {
            logger.info("lock has been acquired when partition {} arrived", pid);
        }
    }

    private static String getSelfHostName() throws UnknownHostException {
        return InetAddress.getLocalHost().getHostName();
    }
}
