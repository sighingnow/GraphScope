package com.alibaba.graphscope.context;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.graphx.GraphXPIE;
import com.alibaba.graphscope.graphx.GraphXConf;
import com.alibaba.graphscope.graphx.utils.SerializationUtils;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import com.alibaba.graphscope.utils.array.PrimitiveArray;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URLClassLoader;
import java.time.LocalDateTime;
import org.apache.spark.graphx.EdgeDirection;
import org.apache.spark.graphx.EdgeTriplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;
import scala.Function2;
import scala.Function3;
import scala.Tuple2;
import scala.collection.Iterator;

public class GraphXAdaptorContext<VDATA_T, EDATA_T, MSG>
    extends VertexDataContext<IFragment<Long, Long, VDATA_T, EDATA_T>, VDATA_T>
    implements DefaultContextBase<Long, Long, VDATA_T, EDATA_T> {

    public static <VD, ED, M> GraphXAdaptorContext<VD, ED, M> createImpl(
        Class<? extends VD> vdClass, Class<? extends ED> edClass,
        Class<? extends M> msgClass, Function3 vprog, Function1 sendMsg, Function2 mergeMsg,
        Object initMsg, String appName, EdgeDirection direction) {
        return new GraphXAdaptorContext<VD, ED, M>(vdClass, edClass, msgClass,
            (Function3<Long, VD, M, VD>) vprog,
            (Function1<EdgeTriplet<VD, ED>, Iterator<Tuple2<Long, M>>>) sendMsg,
            (Function2<M, M, M>) mergeMsg, (M) initMsg, appName, direction);
    }

    public static <VD, ED, M> GraphXAdaptorContext<VD, ED, M> create(URLClassLoader classLoader,
        String serialPath)
        throws ClassNotFoundException {
        Object[] objects = SerializationUtils.read(classLoader, serialPath);
        if (objects.length != 9) {
            throw new IllegalStateException(
                "Expect 8 deserialzed object, but only got " + objects.length);
        }
        Class<?> vdClass = (Class<?>) objects[0];
        Class<?> edClass = (Class<?>) objects[1];
        Class<?> msgClass = (Class<?>) objects[2];
        Function3 vprog = (Function3) objects[3];
        Function1 sendMsg = (Function1) objects[4];
        Function2 mergeMsg = (Function2) objects[5];
        Object initMsg = objects[6];
        String appName = (String) objects[7];
        EdgeDirection direction = (EdgeDirection) objects[8];
        return (GraphXAdaptorContext<VD, ED, M>) createImpl(vdClass, edClass, msgClass, vprog,
            sendMsg, mergeMsg, initMsg, appName, direction);
    }

    private static Logger logger = LoggerFactory.getLogger(GraphXAdaptorContext.class.getName());
    private GraphXConf<VDATA_T, EDATA_T, MSG> conf;
    private GraphXPIE<VDATA_T, EDATA_T, MSG> graphXProxy;
    private String appName;

    public String getAppName() {
        return appName;
    }

    public GraphXConf getConf() {
        return conf;
    }

    public GraphXPIE<VDATA_T, EDATA_T, MSG> getGraphXProxy() {
        return graphXProxy;
    }

    public GraphXAdaptorContext(Class<? extends VDATA_T> vdClass, Class<? extends EDATA_T> edClass,
        Class<? extends MSG> msgClass,
        Function3<Long, VDATA_T, MSG, VDATA_T> vprog,
        Function1<EdgeTriplet<VDATA_T, EDATA_T>, Iterator<Tuple2<Long, MSG>>> sendMsg,
        Function2<MSG, MSG, MSG> mergeMsg, MSG initialMsg, String appName, EdgeDirection edgeDirection) {
        this.conf = new GraphXConf<>(vdClass, edClass, msgClass);
        this.graphXProxy = new GraphXPIE<VDATA_T, EDATA_T, MSG>(conf, vprog, sendMsg, mergeMsg,
            initialMsg,edgeDirection);
        this.appName = appName;
    }

    /**
     * We need to deserialize `vprog`, `sendMsg` and `mergeMsg` from file.
     *
     * @param frag           The graph fragment providing accesses to graph data.
     * @param messageManager The message manger which manages messages between fragments.
     * @param jsonObject     String args from cmdline.
     */
    @Override
    public void Init(IFragment<Long, Long, VDATA_T, EDATA_T> frag,
        DefaultMessageManager messageManager, JSONObject jsonObject) {

        int maxIterations = jsonObject.getInteger("max_iterations");
        logger.info("Max iterations: " + maxIterations);
        int numPart = jsonObject.getInteger("num_part");
        int fnum = frag.fnum();
        int splitSize = (numPart + fnum - 1) / fnum;
        int myParallelism = calcMyParallelism(numPart,splitSize, frag.fid());
        String workerIdToFidStr = jsonObject.getString("worker_id_to_fid");
        if (workerIdToFidStr == null || workerIdToFidStr.isEmpty()){
            throw new IllegalStateException("expect worker id to fid mapping");
        }

        try {
            graphXProxy.init(frag, messageManager, maxIterations,myParallelism, workerIdToFidStr);
        }
        catch (Exception e){
            e.printStackTrace();
            throw new IllegalStateException("initialization error");
        }
        logger.info("create graphx proxy: {}", graphXProxy);
        //FIXME: Currently we don't use this context provided vdata array, just use long class as
        //default vdata class, and we don't use it.
        createFFIContext(frag, (Class<? extends VDATA_T>) Long.class, false);
        //System.gc();
    }

    @Override
    public void Output(IFragment<Long, Long, VDATA_T, EDATA_T> frag) {
        String prefix = "/home/graphscope/spark-res/" + appName + "_" + LocalDateTime.now();
        String filePath = prefix + "_frag_" + frag.fid();
        PrimitiveArray<VDATA_T> vdArray = graphXProxy.getNewVdataArray();

        try {
            FileWriter fileWritter = new FileWriter(filePath);
            BufferedWriter bufferedWriter = new BufferedWriter(fileWritter);

            Vertex<Long> cur = FFITypeFactoryhelper.newVertexLong();
            for (long index = 0; index < frag.getInnerVerticesNum(); ++index) {
                cur.SetValue(index);
                Long oid = frag.getId(cur);
                bufferedWriter.write(
                    cur.GetValue() + "\t" + oid + "\t" + vdArray.get(index) + "\n");
            }
            bufferedWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    int calcMyParallelism(int limit, int splitSize, int fid){
        int begin = Math.min(limit, splitSize * fid);
        int end = Math.min(limit, begin + splitSize);
        return end - begin;
    }
}
