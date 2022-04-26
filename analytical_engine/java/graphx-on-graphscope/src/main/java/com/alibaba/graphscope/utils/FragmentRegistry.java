package com.alibaba.graphscope.utils;

import com.alibaba.graphscope.graphx.FragmentRDD;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class FragmentRegistry {

    //    private static Logger logger = LoggerFactory.getLogger(FragmentRegistry.class.getName());
    private static BufferedWriter writer;

    static {
        try {
            writer = new BufferedWriter(new FileWriter("/tmp/fragment-registry"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

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

    public static int registFragment(String fragIds) throws IOException {
        String[] host2frag = fragIds.split(",");

        synchronized (FragmentRegistry.class) {
            if (fragId == null) {
                for (String val : host2frag) {
                    writer.write("test: " + val + " start with " + hostName + ", matches: " + val.startsWith(hostName) + "\n");
                    if (val.startsWith(hostName)) {
                        FragmentRegistry.fragId = val.split(":")[1];
                        writer.write("on host " + hostName + " get frag id " + fragId + "\n");
                    }
                }
            }
        }
        int partitionId = partition.getAndAdd(1);
        return partitionId;
    }

    /**
     * For each partition/thread, this function should only run once.
     */
    public static void constructFragment(int pid, String fragName) throws IOException {
        if (!lock.isLocked()) {
            if (lock.tryLock()) {
                writer.write("partition " + pid + " successfully got lock\n");
                if (fragmentRDD != null) {
                    throw new IllegalStateException(
                        "Impossible: fragment rdd has been constructed" + fragmentRDD);
                }
                if (fragId == null || fragId.isEmpty()) {
                    throw new IllegalStateException("Please register fragment first");
                }
                fragmentRDD = FragmentRDD.create(fragId, fragName, partition.get());
                writer.write("Successfully create fragment RDD\n");
            } else {
                writer.write("partition " + pid + " try to get lock failed");
            }
        } else {
            writer.write("lock has been acquired when partition " + pid + "arrived\n");
        }
    }

    private static String getSelfHostName() throws UnknownHostException {
        return InetAddress.getLocalHost().getHostName();
    }
}
