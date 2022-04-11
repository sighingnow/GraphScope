package com.alibaba.graphscope.context;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.graphscope.conf.GraphXConf;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.VertexRange;
import com.alibaba.graphscope.factory.GraphXFactory;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.graph.VertexDataManager;
import com.alibaba.graphscope.graphx.SerializationUtils;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import com.alibaba.graphscope.utils.GraphXProxy;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import org.apache.spark.graphx.EdgeTriplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;
import scala.Function2;
import scala.Function3;
import scala.Tuple2;
import scala.collection.Iterator;

public class GraphXAdaptorContext<VDATA_T, EDATA_T> extends
    VertexDataContext<IFragment<Long, Long, VDATA_T, EDATA_T>, VDATA_T> implements
    DefaultContextBase<Long, Long, VDATA_T, EDATA_T> {

    private static Logger logger = LoggerFactory.getLogger(GraphXAdaptorContext.class.getName());
    private static String USER_CLASS = "user_class";
    private static String VPROG_SERIALIZATION = "vprog_serialization";
    private static String SEND_MSG_SERIALIZATION = "send_msg_serialization";
    private static String MERGE_MSG_SERIALIZATION = "merge_msg_serialization";
    private static String VD_CLASS = "vd_class";
    private static String ED_CLASS = "ed_class";
    private static String MSG_CLASS = "msg_class";
    private static String INITIAL_MSG = "initial_msg";
    private String userClassName;
    private String vprogFilePath, sendMsgFilePath, mergeMsgFilePath;
    private Class<?> vdClass, edClass, msgClass;
    private GraphXConf conf;
    private GraphXProxy graphXProxy;
    private Object initialMsg;

    public String getUserClassName() {
        return userClassName;
    }
    public GraphXConf getConf(){
        return conf;
    }

    public GraphXProxy getGraphXProxy(){
        return graphXProxy;
    }

    public Object getInitialMsg(){
        return initialMsg;
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
        String vdClassStr = jsonObject.getString(VD_CLASS);
        String edClassStr = jsonObject.getString(ED_CLASS);
        String msgClassStr = jsonObject.getString(MSG_CLASS);
        logger.info("received vd {} ed {} msg {}", vdClassStr, edClassStr, msgClassStr);
        vdClass = loadClassWithName(this.getClass().getClassLoader(), vdClassStr);
        edClass = loadClassWithName(this.getClass().getClassLoader(), edClassStr);
        msgClass = loadClassWithName(this.getClass().getClassLoader(), msgClassStr);
        conf = GraphXFactory.createGraphXConf(vdClass,edClass,msgClass);

        //TODO: get vdata class from conf
        createFFIContext(frag, (Class<? extends VDATA_T>) conf.getVdataClass(), false);

        if (jsonObject.containsKey(USER_CLASS) && !jsonObject.getString(USER_CLASS).isEmpty()) {
            logger.info("Parse user app class {} from json str", jsonObject.getString(USER_CLASS));
            userClassName = jsonObject.getString(USER_CLASS);
//        }
        } else {
            throw new IllegalStateException("user class required");
        }

        this.vprogFilePath = jsonObject.getString(VPROG_SERIALIZATION);
        this.sendMsgFilePath = jsonObject.getString(SEND_MSG_SERIALIZATION);
        this.mergeMsgFilePath = jsonObject.getString(MERGE_MSG_SERIALIZATION);
        if (this.vprogFilePath == null || this.vprogFilePath.isEmpty() ||
            this.sendMsgFilePath == null || this.sendMsgFilePath.isEmpty() ||
        this.mergeMsgFilePath == null || this.mergeMsgFilePath.isEmpty()){
            throw new IllegalStateException("file path empty " + vprogFilePath + ", " + sendMsgFilePath + "," + mergeMsgFilePath);
        }

        graphXProxy = GraphXFactory.createGraphXProxy(conf, vprogFilePath, sendMsgFilePath, mergeMsgFilePath);
        String msgStr = jsonObject.getString(INITIAL_MSG);
        logger.info("Initial msg in str: " + msgStr);
        //get initial msg
        if (msgClass.equals(Long.class)){
            this.initialMsg = Long.valueOf(msgStr);
        }
        else if (msgClass.equals(Double.class)){
            this.initialMsg = Double.valueOf(msgStr);
        }
        else if (msgClass.equals(Integer.class)){
            this.initialMsg = Integer.valueOf(msgStr);
        }
        else {
            throw new IllegalStateException("unmatched msg class " + msgClass.getName());
        }

    }

    @Override
    public void Output(IFragment<Long, Long, VDATA_T, EDATA_T> frag) {
        String prefix = "/tmp/graphx_output";
        String filePath = prefix + "_frag_" + String.valueOf(frag.fid());
        VertexDataManager<VDATA_T> vertexDataManager = graphXProxy.getVertexDataManager();
        try {
            FileWriter fileWritter = new FileWriter(new File(filePath));
            BufferedWriter bufferedWriter = new BufferedWriter(fileWritter);
            VertexRange<Long> innerNodes = frag.innerVertices();

            Vertex<Long> cur = FFITypeFactoryhelper.newVertexLong();
            for (long index = 0; index < frag.getInnerVerticesNum(); ++index) {
                cur.SetValue(index);
                Long oid = frag.getId(cur);
                bufferedWriter.write(
                    cur.GetValue() + "\t" + oid + "\t" + vertexDataManager.getVertexData(index) + "\n");
            }
            bufferedWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private Class<?> loadClassWithName(ClassLoader cl, String name){
        if (name.equals("int")){
            return Integer.class;
        }
        else if (name.equals("long")){
            return Long.class;
        }
        else if (name.equals("double")){
            return Double.class;
        }
        throw new IllegalStateException("Unrecoginizable :" + name);
    }

}
