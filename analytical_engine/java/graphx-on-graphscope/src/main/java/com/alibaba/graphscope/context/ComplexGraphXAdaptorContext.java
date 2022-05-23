package com.alibaba.graphscope.context;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.VertexRange;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.graph.GraphXPIE;
import com.alibaba.graphscope.graphx.GraphXConf;
import com.alibaba.graphscope.graphx.SerializationUtils;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import com.alibaba.graphscope.utils.array.PrimitiveArray;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ComplexGraphXAdaptorContext<VDATA_T,EDATA_T,MSG_T> extends GraphXAdaptorContext<VDATA_T,EDATA_T,MSG_T> {
    private static Logger logger = LoggerFactory.getLogger(ComplexGraphXAdaptorContext.class.getName());

    private Class<?> vdClass,edClass,msgClass;
    private Object initialMsg;
    private GraphXConf conf;
    private GraphXPIE graphXProxy;

    @Override
    public void Init(IFragment<Long, Long, VDATA_T, EDATA_T> frag,
        DefaultMessageManager messageManager, JSONObject jsonObject) {
        String vdClassStr = jsonObject.getString(VD_CLASS);
        String edClassStr = jsonObject.getString(ED_CLASS);
        String msgClassStr = jsonObject.getString(MSG_CLASS);
        logger.info("received vd {} ed {} msg {}", vdClassStr, edClassStr, msgClassStr);
        logger.info("Parallelism: " + numCores);

        maxIterations = jsonObject.getInteger("max_iterations");
        logger.info("Max iterations: " + maxIterations);

        // TODO: support loading user specified class(in user_jar_path)
//        vdClass = (Class<? extends VDATA_T>) loadClassWithName(classLoader, vdClassStr);
        if (vdClassStr.equals("std::string")){
            String vdClassPath = jsonObject.getString(VD_SERIALIZATION);
            try {
                logger.info("Deserializing vd class from " + vdClassPath);
                vdClass = (Class<?>) SerializationUtils.read(classLoader, vdClassPath);
                logger.info("Deserilized vd class " + vdClass.getName());
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
        else {
            vdClass = loadClassWithName(classLoader, vdClassStr);
        }
        if (vdClassStr.equals("std::string")){
            String edClassPath = jsonObject.getString(ED_SERIALIZATION);
            try {
                logger.info("Deserializing ed class from " + edClassPath);
                edClass = (Class<?>) SerializationUtils.read(classLoader, edClassPath);
                logger.info("Deserilized ed class " + edClass.getName());
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
        else {
            edClass = loadClassWithName(classLoader, edClassStr);
        }
        if (vdClassStr.equals("std::string")){
            String msgClassPath = jsonObject.getString(MSG_SERIALIZATION);
            try {
                logger.info("Deserializing msg class from " + msgClassPath);
                msgClass = (Class<?>) SerializationUtils.read(classLoader, msgClassPath);
                logger.info("Deserilized msg class " + msgClass.getName());
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
        else {
            msgClass = loadClassWithName(classLoader, msgClassStr);
        }

        conf = new GraphXConf(vdClass, edClass, msgClass);
        // TODO: get vdata class from conf
//    createFFIContext(frag, conf.getVdClass(), false);

        this.vprogFilePath = jsonObject.getString(VPROG_SERIALIZATION);
        this.sendMsgFilePath = jsonObject.getString(SEND_MSG_SERIALIZATION);
        this.mergeMsgFilePath = jsonObject.getString(MERGE_MSG_SERIALIZATION);
        if (this.vprogFilePath == null || this.vprogFilePath.isEmpty()
            || this.sendMsgFilePath == null
            || this.sendMsgFilePath.isEmpty() || this.mergeMsgFilePath == null
            || this.mergeMsgFilePath.isEmpty()) {
            throw new IllegalStateException(
                "file path empty " + vprogFilePath + ", " + sendMsgFilePath
                    + "," + mergeMsgFilePath);
        }
        String msgStr = jsonObject.getString(INITIAL_MSG);
        logger.info("Initial msg in str: " + msgStr);
        if (msgStr.equals("std::string")){
            try {
                logger.info("Deserializing initial msg");
                initialMsg = SerializationUtils.read(classLoader, INITIAL_MSG_SERIALIZATION);
                logger.info("Deserilized initial msg" + initialMsg);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
        else {
            // get initial msg
            if (msgClass.equals(Long.class)) {
                this.initialMsg = Long.valueOf(msgStr);
            } else if (msgClass.equals(Double.class)) {
                this.initialMsg = Double.valueOf(msgStr);
            } else if (msgClass.equals(Integer.class)) {
                this.initialMsg = Integer.valueOf(msgStr);
            } else {
                throw new IllegalStateException("unmatched msg class " + msgClass.getName());
            }
        }
        graphXProxy =
            new GraphXPIE<>(conf, vprogFilePath, sendMsgFilePath, mergeMsgFilePath, classLoader);
        graphXProxy.init(frag, messageManager, initialMsg, maxIterations);
        logger.info("create graphx proxy: {}", graphXProxy);
        System.gc();
    }


    @Override
    public void Output(IFragment<Long, Long, VDATA_T, EDATA_T> frag) {
        String prefix = "/tmp/graphx_output";
        String filePath = prefix + "_frag_" + String.valueOf(frag.fid());
        PrimitiveArray vdArray = graphXProxy.getNewVdataArray();

        try {
            FileWriter fileWritter = new FileWriter(new File(filePath));
            BufferedWriter bufferedWriter = new BufferedWriter(fileWritter);
            VertexRange<Long> innerNodes = frag.innerVertices();

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
}
