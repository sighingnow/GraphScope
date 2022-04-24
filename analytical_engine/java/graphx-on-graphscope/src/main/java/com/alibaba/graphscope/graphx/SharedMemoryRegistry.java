package com.alibaba.graphscope.graphx;

import com.alibaba.fastffi.FFIByteString;
import com.alibaba.fastffi.FFITypeFactory;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import com.alibaba.graphscope.utils.MappedBuffer;
import java.util.Enumeration;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The graphx executor main have many partitions, but they runs in one executor definitely. So we
 * user this sharedMemoryRegistry, to manages the shared memory in a thread-safe manner.
 */
public class SharedMemoryRegistry {
    private static Logger logger = LoggerFactory.getLogger(SharedMemoryRegistry.class.getName());
    static{
        try {
            System.loadLibrary("grape-jni");
            logger.info("load grape-jni success");
        }
        catch (Exception e){
            throw new IllegalStateException("Fail to load library: grape-jni");
        }
    }
    private static SharedMemoryRegistry registry;

//    private ConcurrentHashMap<String, MemoryMappedBuffer> key2MappedBuffer;
    private ConcurrentHashMap<String, MappedBuffer> key2MappedBuffer;

    private SharedMemoryRegistry(){
        key2MappedBuffer = new ConcurrentHashMap<>();
        logger.info("Creating default sharedMemoryRegistry");
    }

//    public MemoryMappedBuffer mapFor(String key, long size){
    public MappedBuffer mapFor(String key, long size){
        if (key2MappedBuffer.contains(key)){
            throw new IllegalStateException("Mapping to an existing key: " + key);
        }
        FFIByteString byteString = FFITypeFactory.newByteString();
        byteString.copyFrom(key);
        logger.info("MemoryMappedBuffer class loader: " + MappedBuffer.class.getClassLoader());
        logger.info("Set context class loader : " + SharedMemoryRegistry.class.getClassLoader());
        Thread.currentThread().setContextClassLoader(SharedMemoryRegistry.class.getClassLoader());
        System.loadLibrary("grape-jni");
        Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
//        MemoryMappedBuffer res = MemoryMappedBuffer.factory.create(byteString, size);
        MappedBuffer res = MappedBuffer.mapToFile(key, size);
        logger.info("mapping for {}: buffer {} of size: {}", key, res, size);
        key2MappedBuffer.put(key, res);
        return res;
    }

    public void unMapFor(String key, long size){
        if (key2MappedBuffer.contains(key)){
            MappedBuffer memoryMappedBuffer = key2MappedBuffer.get(key);
            logger.info("Start unmapping: {} , buffer {}", key, memoryMappedBuffer);
//            memoryMappedBuffer.unMap();
            key2MappedBuffer.remove(key);
        }
        else {
            logger.error("Try to unmap a non-existing mapping: " + key);
        }
    }

    /**
     * Get the all mapped files into one strings.
     * for example. "/tmp/vertex-partition-1;/tmp/edge-partition-2;"
     *
     * This string will be parsed by mpi processes.
     * @param prefix The prefix to fileter
     * @return result string.
     */
    public String getAllMappedFileNames(String prefix){
        StringBuilder sb = new StringBuilder();
        Enumeration<String> set = key2MappedBuffer.keys();
        while (set.hasMoreElements()){
            String fileName = set.nextElement();
            if (fileName.startsWith(prefix)){
                sb.append(fileName);
                sb.append(";");
            }
        }
        String res = sb.toString();
        logger.info("all Mapped file names: {}", res);
        return res;
    }

    public static synchronized SharedMemoryRegistry getOrCreate(){
        if (registry == null){
            synchronized (SharedMemoryRegistry.class){
                if (registry == null){
                    registry = new SharedMemoryRegistry();
                }
            }
        }
        return registry;
    }
}
