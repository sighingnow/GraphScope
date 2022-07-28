package com.alibaba.graphscope.utils;

import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.BaseGraphXFragment;
import com.alibaba.graphscope.graphx.graph.GSEdgeTripletImpl;
import com.alibaba.graphscope.graphx.utils.IdParser;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import com.alibaba.graphscope.serialization.FFIByteVectorInputStream;
import com.alibaba.graphscope.serialization.FFIByteVectorOutputStream;
import com.alibaba.graphscope.stdcxx.FFIByteVector;
import java.io.IOException;
import java.util.BitSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Iterator;
import scala.Function2;
import scala.Tuple2;

public class DoubleMessageStore implements MessageStore<Double> {
    public class LongDouble{
        public long v;
        public double u;
        public LongDouble(long a, double b){
            v = a;
            u = b;
        }
    }
    private final Logger logger = LoggerFactory.getLogger(DoubleMessageStore.class.getName());

    private static int QUEUE_CAPACITY = 10000000;
    private double[] values;
//    private AtomicDoubleArrayWrapper values;
    private Function2<Double,Double,Double> mergeMessage;
    private Vertex<Long> tmpVertex[];
    private FFIByteVectorOutputStream[] outputStream;
    private IdParser idParser;
    private final BlockingQueue<LongDouble> msgQueue;
    private Thread consumer;
    private BitSet nextSet;

    public DoubleMessageStore(int len, int fnum,int numCores, Function2<Double,Double,Double> function2,BitSet nextSet) {
        values = new double[len];
//        values = new AtomicDoubleArrayWrapper(len);
        mergeMessage = function2;
        tmpVertex = new Vertex[numCores];
        for (int i = 0; i < numCores; ++i){
            tmpVertex[i]= FFITypeFactoryhelper.newVertexLong();
        }
        outputStream = new FFIByteVectorOutputStream[fnum];
        for (int i = 0; i < fnum; ++i){
            outputStream[i] = new FFIByteVectorOutputStream();
        }
        idParser = new IdParser(fnum);
        this.nextSet = nextSet;
        msgQueue = new ArrayBlockingQueue<>(QUEUE_CAPACITY);
    }
    public void startConsumer(){
        consumer = new Thread(){
            @Override
            public void run(){
                try {
                    while (true) {
                        LongDouble tuple = msgQueue.poll(1, TimeUnit.MICROSECONDS);
                        while (tuple == null) {
                            if (msgQueue.size() <= 0) {
                                synchronized (msgQueue) {
                                    msgQueue.wait();
                                }
                                logger.info("after wait msgQueue");
                            }
                            if (msgQueue.size() > 0) {
                                tuple = msgQueue.poll(1, TimeUnit.MICROSECONDS);
                            }
                        }

                        int lid = (int) tuple.v;
                        double newMsg = tuple.u;
                        if (nextSet.get(lid)) {
                            double original = values[lid];
                            if (original != newMsg) {
                                values[lid] = mergeMessage.apply(original, newMsg);
                            }
                        } else {
                            values[lid] = newMsg;
                            nextSet.set(lid);
                        }
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };
        consumer.run();
    }

    @Override
    public Double get(int index) {
        return values[index];
//        return values.get(index);
    }

    @Override
    public void set(int index, Double value) {
        values[index] =  value;
//        values.set(index,value);
    }

    @Override
    public int size() {
        return values.length;
//        return values.getSize();
    }


    @Override
    public void addMessages(
        Iterator<Tuple2<Long, Double>> msgs, BaseGraphXFragment<Long,Long,?,?> fragment, int threadId, GSEdgeTripletImpl edgeTriplet, int srcLid, int dstLid)
        throws InterruptedException {
        while (msgs.hasNext()){
            Tuple2<Long, Double> msg = msgs.next();
            //the oid must from src or dst, we first find with lid.
            long oid = msg._1();
            int lid;
            if (oid == edgeTriplet.dstId()){
                lid = dstLid;
            }
            else {
                lid = srcLid;
            }
            LongDouble newMsg =new LongDouble(lid,msg._2());
            logger.info("adding msg {}{} to queue ", newMsg.v, newMsg.u);
            if (!msgQueue.offer(newMsg)){
                throw new IllegalStateException("msg not accepted");
            }
        }
        synchronized (msgQueue) {
            msgQueue.notify();
        }
    }

    @Override
    public void flushMessages(BitSet nextSet, DefaultMessageManager messageManager,
        BaseGraphXFragment<Long, Long, ?, ?> fragment, int[] fid2WorkerId) throws IOException {
        while (msgQueue.size() > 0){
            logger.info("still {} msg in queue ",msgQueue.size());
            synchronized (msgQueue) {
                msgQueue.notify();
            }
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        if (msgQueue.size() > 0){
            throw new IllegalStateException("not possible");
        }
        logger.info("all message are processed");

        int ivnum = (int) fragment.getInnerVerticesNum();
        int cnt = 0;
        Vertex<Long> vertex = tmpVertex[0];

        for (int i = nextSet.nextSetBit(ivnum); i >= 0; i = nextSet.nextSetBit(i + 1)) {
            vertex.SetValue((long) i);
            int dstFid = fragment.getFragId(vertex);
            outputStream[dstFid].writeLong(fragment.getOuterVertexGid(vertex));
            outputStream[dstFid].writeDouble(values[i]);
            cnt += 1;
        }
        logger.debug("Frag [{}] try to send {} msg to outer vertices", fragment.fid(), cnt);
        //finish stream
        for (int i = 0; i < fragment.fnum(); ++i){
            if (i != fragment.fid()){
                outputStream[i].finishSetting();
                if (outputStream[i].getVector().size() > 0){
                    int workerId = fid2WorkerId[i];
                    messageManager.sendToFragment(workerId, outputStream[i].getVector());
//                    logger.info("fragment [{}] send {} bytes to [{}]", fragment.fid(), outputStream[i].getVector().size(), i);
                }
                outputStream[i].reset();
            }
        }
    }

    @Override
    public void digest(FFIByteVector vector, BaseGraphXFragment<Long,Long,?,?> fragment, BitSet curSet) {
        FFIByteVectorInputStream inputStream = new FFIByteVectorInputStream(vector);
        int size = (int) vector.size();
        if (size <= 0) {
            throw new IllegalStateException("The received vector can not be empty");
        }
        Vertex<Long> vertex = tmpVertex[0];

//        logger.debug("DefaultMessageStore digest FFIVector size {}", size);
        try {
            while (inputStream.available() > 0) {
                long gid = inputStream.readLong();
                double msg = inputStream.readDouble();
                int fid = idParser.getFragId(gid);
                if (fid != fragment.fid()){
                    throw new IllegalStateException("receive a non inner vertex gid: "+ gid + ", parsed fid: " + fid + ", our fid: " + fragment.fid());
                }
                if (!fragment.innerVertexGid2Vertex(gid, vertex)){
                    throw new IllegalStateException("inner vertex gid 2 vertex failed");
                }
                int lid = vertex.GetValue().intValue();
                if (curSet.get(lid)){
//                    values[lid] = mergeMessage.apply(values[lid], msg);
                    values[lid] =  mergeMessage.apply(values[lid], msg);
                }
                else {
                    //no update in curSet when the message store is not changed, although we receive vertices.
//                    if (values[lid] != msg){
//                        values[lid] =  msg;
//                        curSet.set(lid);
//                    }
                    if (values[lid] != msg){
                        values[lid] = msg;
                        curSet.set(lid);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
