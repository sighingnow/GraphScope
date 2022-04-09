package com.alibaba.graphscope.context;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.graphscope.conf.GraphXConf;
import com.alibaba.graphscope.factory.GraphXFactory;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.graphx.SerializationUtils;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import com.alibaba.graphscope.utils.GraphXProxy;
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
        if (vdClassStr.equals("long") && edClassStr.equals("long") && msgClassStr.equals("long")){
            Function3<Long, Long, Long, Long> vprog = deserializeVprog(vprogFilePath, conf);
            Function1<EdgeTriplet<Long, Long>, Iterator<Tuple2<Long, Long>>> sendMsg = deserializeSendMsg(sendMsgFilePath, conf);
            Function2<Long, Long, Long> mergeMsg = deserializeMergeMsg(mergeMsgFilePath, conf);
            logger.info("deserialization success: {}, {}, {}", vprog, sendMsg, mergeMsg);

            graphXProxy = GraphXFactory.createGraphXProxy(conf, vprog, sendMsg, mergeMsg);
            logger.info("Construct graphx proxy: " + graphXProxy);
        }
        String msgStr = jsonObject.getString(INITIAL_MSG);
        logger.info("Initial msg in str: " + msgStr);
        //get initial msg
        if (msgClass.equals(Long.class)){
            initialMsg = Long.valueOf(msgStr);
        }
        else if (msgClass.equals(Double.class)){
            initialMsg = Double.valueOf(msgStr);
        }
        else if (msgClass.equals(Integer.class)){
            initialMsg = Integer.valueOf(msgStr);
        }

    }

    @Override
    public void Output(IFragment<Long, Long, VDATA_T, EDATA_T> frag) {

    }

    private static <VD,ED,MSG> Function3<Long, VD, ED, MSG> deserializeVprog(String vprogFilePath, GraphXConf<VD,ED,MSG> conf){
        try {
            Function3<Long,VD,ED,MSG> res = (Function3<Long, VD, ED, MSG>) SerializationUtils.read(vprogFilePath);
            return res;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new IllegalStateException("deserialization vprog failed");
        }
    }

    private static <VD,ED,MSG> Function1<EdgeTriplet<VD, ED>, Iterator<Tuple2<Long, MSG>>> deserializeSendMsg(String sendMsgFilePath, GraphXConf<VD,ED,MSG> conf){
        try {
            Function1<EdgeTriplet<VD, ED>, Iterator<Tuple2<Long, MSG>>> res = (Function1<EdgeTriplet<VD, ED>, Iterator<Tuple2<Long, MSG>>>) SerializationUtils.read(sendMsgFilePath);
            return res;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new IllegalStateException("deserialization send msg failed");
        }
    }

    private static <VD,ED,MSG> Function2<MSG, MSG, MSG> deserializeMergeMsg(String mergeMsgFilePath, GraphXConf<VD,ED,MSG> conf){
        try {
            Function2<MSG, MSG, MSG> res = (Function2<MSG, MSG, MSG>) SerializationUtils.read(mergeMsgFilePath);
            return res;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new IllegalStateException("deserialization merge msg failed");
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
