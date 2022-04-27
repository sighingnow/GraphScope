package com.alibaba.graphscope.utils;

import com.alibaba.graphscope.conf.GraphXConf;
import com.alibaba.graphscope.factory.GraphXFactory;
import com.alibaba.graphscope.graph.GraphXVertexIdManager;
import com.alibaba.graphscope.graph.GraphxEdgeManager;
import com.alibaba.graphscope.graph.VertexDataManager;
import com.alibaba.graphscope.graphx.FragmentHolder;
import com.alibaba.graphscope.graphx.JavaEdgePartition;
import com.alibaba.graphscope.graphx.JavaVertexPartition;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FragmentRegistry {

        private static Logger logger = LoggerFactory.getLogger(FragmentRegistry.class.getName());
//    private static BufferedWriter writer;

    private static AtomicInteger partition = new AtomicInteger(0);
    private static String hostName;
    private static String fragId;
    private static ReentrantLock lock = new ReentrantLock();
    private static FragmentHolder fragmentHolder;
//    private static GraphXConf conf;
    private static List<JavaVertexPartition> vertexPartitions;
    private static List<JavaEdgePartition> edgePartitions;

    static {
        try {
            hostName = getSelfHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    public static<VD,ED> GraphXConf<VD,ED,?> initConf(Class<?> vdClass, Class<?> edClass){
        GraphXConf<VD,ED,?> conf = new GraphXConf<>();
        if (vdClass.equals(scala.Long.class) || vdClass.equals(Long.class)){
            conf.setVdataClass((Class<? extends VD>) Long.class);
        }
        else if (vdClass.equals(scala.Int.class) || vdClass.equals(Integer.class)){
            conf.setVdataClass((Class<? extends VD>) Integer.class);
        }
        else if (vdClass.equals(scala.Double.class) || vdClass.equals(Double.class)){
            conf.setVdataClass((Class<? extends VD>) Double.class);
        }
        else {
            throw new IllegalStateException("Error vd class: " + vdClass.getName());
        }

        if (edClass.equals(scala.Long.class) || edClass.equals(Long.class)){
            conf.setEdataClass((Class<? extends ED>) Long.class);
        }
        else if (edClass.equals(scala.Int.class) || edClass.equals(Integer.class)){
            conf.setEdataClass((Class<? extends ED>) Integer.class);
        }
        else if (edClass.equals(scala.Double.class) || edClass.equals(Double.class)){
            conf.setEdataClass((Class<? extends ED>) Double.class);
        }
        else {
            throw new IllegalStateException("Error ed class: " + edClass.getName());
        }
        return conf;
    }

    public static int registFragment(String fragIds) throws IOException {
        String[] host2frag = fragIds.split(",");

        synchronized (FragmentRegistry.class) {
            if (fragId == null) {
                for (String val : host2frag) {
                    logger.info("test: " + val + " start with " + hostName + ", matches: " + val.startsWith(hostName));
                    if (val.startsWith(hostName)) {
                        FragmentRegistry.fragId = val.split(":")[1];
                        logger.info("on host " + hostName + " get frag id " + fragId + "\n");
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
    public static <VD,ED>void constructFragment(int pid, String fragName, Class<? extends VD> vdClass, Class<? extends ED> edClass, int numCores) throws IOException {
        if (!lock.isLocked()) {
            if (lock.tryLock()) {
                logger.info("partition " + pid + " successfully got lock");
                if (fragmentHolder != null) {
                    throw new IllegalStateException(
                        "Impossible: fragment rdd has been constructed" + fragmentHolder);
                }
                if (fragId == null || fragId.isEmpty()) {
                    throw new IllegalStateException("Please register fragment first");
                }
                fragmentHolder = FragmentHolder.create(fragId, fragName, partition.get());
                GraphXConf<VD,ED,?> conf = initConf(vdClass, edClass);
                GraphXVertexIdManager idManager = GraphXFactory.createIdManager(conf);
                VertexDataManager<VD> vertexDataManager = GraphXFactory.createVertexDataManager(conf);
                idManager.init(fragmentHolder.getIFragment());
                vertexDataManager.init(fragmentHolder.getIFragment());
                logger.info("create id Manager: {}",idManager);
                logger.info("create vdata manager: {}", vertexDataManager);
                logger.info("Successfully create fragment RDD");
                //Now create vertexPartitions and edge partitions.
                createVertexPartitions(conf, idManager, vertexDataManager);
                createEdgePartitions(conf, idManager,vertexDataManager,numCores);

                lock.unlock();
            } else {
                logger.info("partition " + pid + " try to get lock failed");
            }
        } else {
            logger.info("lock has been acquired when partition " + pid + "arrived");
        }
    }

    public static <VD,ED> void createVertexPartitions(GraphXConf<VD,ED,?> conf,GraphXVertexIdManager idManager, VertexDataManager<VD> vertexDataManager){
        if (Objects.nonNull(vertexPartitions)){
            logger.error("Recreating vertex partitions is not expected");
            return ;
        }
        vertexPartitions = new ArrayList<>(partition.get());
        int numPartitions = partition.get();
        for (int i = 0; i < numPartitions; ++i){
            vertexPartitions.add(new JavaVertexPartition(i,numPartitions, idManager,vertexDataManager));
        }
        logger.info("Finish creating javaVertexPartitions of size {}", numPartitions);
    }

    public static <VD,ED>void createEdgePartitions(GraphXConf<VD,ED,?> conf,GraphXVertexIdManager idManager, VertexDataManager<VD> vertexDataManager, int numCores){
        if (Objects.nonNull(edgePartitions)){
            logger.error("Recreating edge partitions is not expected");
            return ;
        }
        GraphxEdgeManager<VD,ED,?> edgeManager = GraphXFactory.createEdgeManager(conf,idManager, vertexDataManager);
        edgeManager.init(fragmentHolder.getIFragment(), numCores);
        edgePartitions = new ArrayList<>(partition.get());
        int numPartitions = partition.get();
        for (int i = 0; i < numPartitions; ++i){
            edgePartitions.add(new JavaEdgePartition(i,numPartitions, idManager,edgeManager));
        }
        logger.info("Finish creating javaEdgePartitions of size {}", numPartitions);
    }

    public static <VD> JavaVertexPartition<VD> getVertexPartition(int pid){
        return vertexPartitions.get(pid);
    }

    public static <VD,ED> JavaEdgePartition<VD,ED> getEdgePartition(int pid){
        return edgePartitions.get(pid);
    }

    private static String getSelfHostName() throws UnknownHostException {
        return InetAddress.getLocalHost().getHostName();
    }
}
