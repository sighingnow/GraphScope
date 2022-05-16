package com.alibaba.graphscope.context;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.graphscope.ds.ImmutableTypedArray;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.VertexRange;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.graph.GraphXPIE;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import com.alibaba.graphscope.utils.array.PrimitiveArray;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URLClassLoader;
import java.util.concurrent.ExecutorService;
import org.apache.spark.graphx.GraphXConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphXAdaptorContext<VDATA_T, EDATA_T, MSG>
    extends VertexDataContext<IFragment<Long, Long, VDATA_T, EDATA_T>, VDATA_T>
    implements DefaultContextBase<Long, Long, VDATA_T, EDATA_T> {
  public static <VD, ED, M> GraphXAdaptorContext<VD, ED, M> create(String vdClass, String edClass,
                                                                   String msgClass) {
    if (vdClass.equals("int64_t") && edClass.equals("int64_t") && msgClass.equals("int64_t")) {
      return (GraphXAdaptorContext<VD, ED, M>) new GraphXAdaptorContext<Long, Long, Long>();
    } else if (vdClass.equals("int64_t") && edClass.equals("int32_t")
               && msgClass.equals("int64_t")) {
      return (GraphXAdaptorContext<VD, ED, M>) new GraphXAdaptorContext<Long, Integer, Long>();
    } else if (vdClass.equals("double") && edClass.equals("int32_t") && msgClass.equals("double")) {
      return (GraphXAdaptorContext<VD, ED, M>) new GraphXAdaptorContext<Double, Integer, Double>();
    } else if (vdClass.equals("double") && edClass.equals("double") && msgClass.equals("double")) {
      return (GraphXAdaptorContext<VD, ED, M>) new GraphXAdaptorContext<Double, Double, Double>();
    } else {
      throw new IllegalStateException("not supported classes: " + vdClass + "," + edClass + ","
                                      + msgClass);
    }
  private static Logger logger = LoggerFactory.getLogger(GraphXAdaptorContext.class.getName());
  private static String VPROG_SERIALIZATION = "vprog_path";
  private static String SEND_MSG_SERIALIZATION = "send_msg_path";
  private static String MERGE_MSG_SERIALIZATION = "merge_msg_path";
  private static String VD_CLASS = "vd_class";
  private static String ED_CLASS = "ed_class";
  private static String MSG_CLASS = "msg_class";
  private static String INITIAL_MSG = "initial_msg";
  private static int numCores = 8;
  private String vprogFilePath, sendMsgFilePath, mergeMsgFilePath;
  private Class<? extends VDATA_T> vdClass;
  private Class<? extends EDATA_T> edClass;
  private Class<? extends MSG> msgClass;
  private GraphXConf<VDATA_T, EDATA_T, MSG> conf;
  private GraphXPIE<VDATA_T, EDATA_T, MSG> graphXProxy;
  private MSG initialMsg;
  private int maxIterations;
  public ExecutorService executor;
  private URLClassLoader classLoader;
  public void setClassLoader(URLClassLoader classLoader) {
    this.classLoader = classLoader;
  }

  public GraphXConf getConf() {
    return conf;
  }

    public GraphXPIE<VDATA_T, EDATA_T, MSG> getGraphXProxy() {
        return graphXProxy;
    }

    public MSG getInitialMsg() {
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
        logger.info("Parallelism: " + numCores);

        maxIterations = jsonObject.getInteger("max_iterations");
        logger.info("Max iterations: " + maxIterations);

        //TODO: support loading user specified class(in user_jar_path)
        vdClass = (Class<? extends VDATA_T>) loadClassWithName(classLoader,
            vdClassStr);
        edClass = (Class<? extends EDATA_T>) loadClassWithName(classLoader,
            edClassStr);
        msgClass = (Class<? extends MSG>) loadClassWithName(classLoader,
            msgClassStr);
        //FIXME: create conf

        conf = new GraphXConf<>(
            scala.reflect.ClassTag.apply(vdClass),
            scala.reflect.ClassTag.apply(edClass),
            scala.reflect.ClassTag.apply(msgClass));
        //TODO: get vdata class from conf
        createFFIContext(frag, conf.getVdClass(), false);

        this.vprogFilePath = jsonObject.getString(VPROG_SERIALIZATION);
        this.sendMsgFilePath = jsonObject.getString(SEND_MSG_SERIALIZATION);
        this.mergeMsgFilePath = jsonObject.getString(MERGE_MSG_SERIALIZATION);
        if (this.vprogFilePath == null || this.vprogFilePath.isEmpty() ||
            this.sendMsgFilePath == null || this.sendMsgFilePath.isEmpty() ||
            this.mergeMsgFilePath == null || this.mergeMsgFilePath.isEmpty()) {
            throw new IllegalStateException(
                "file path empty " + vprogFilePath + ", " + sendMsgFilePath + ","
                    + mergeMsgFilePath);
        }
        String msgStr = jsonObject.getString(INITIAL_MSG);
        logger.info("Initial msg in str: " + msgStr);
        //get initial msg
        if (msgClass.equals(Long.class)) {
            this.initialMsg = (MSG) Long.valueOf(msgStr);
        } else if (msgClass.equals(Double.class)) {
            this.initialMsg = (MSG) Double.valueOf(msgStr);
        } else if (msgClass.equals(Integer.class)) {
            this.initialMsg = (MSG) Integer.valueOf(msgStr);
        } else {
            throw new IllegalStateException("unmatched msg class " + msgClass.getName());
        }
//        graphXProxy = create(messageManager, vdClass,  edClass, msgClass, (IFragment) frag, mergeMsgFilePath, vprogFilePath, sendMsgFilePath, maxIterations, numCores,initialMsg);
        graphXProxy = new GraphXPIE<>(conf, vprogFilePath, sendMsgFilePath, mergeMsgFilePath, classLoader);
        graphXProxy.init(frag, messageManager, initialMsg, maxIterations);
        logger.info("create graphx proxy: {}", graphXProxy);
        System.gc();
  }


  @Override
  public void Output(IFragment<Long, Long, VDATA_T, EDATA_T> frag) {
    String prefix = "/tmp/graphx_output";
    String filePath = prefix + "_frag_" + String.valueOf(frag.fid());
    PrimitiveArray<VDATA_T> vdArray = graphXProxy.getNewVdataArray();

    try {
      FileWriter fileWritter = new FileWriter(new File(filePath));
      BufferedWriter bufferedWriter = new BufferedWriter(fileWritter);
      VertexRange<Long> innerNodes = frag.innerVertices();

      Vertex<Long> cur = FFITypeFactoryhelper.newVertexLong();
      for (long index = 0; index < frag.getInnerVerticesNum(); ++index) {
        cur.SetValue(index);
        Long oid = frag.getId(cur);
        bufferedWriter.write(cur.GetValue() + "\t" + oid + "\t" + vdArray.get(index) + "\n");
      }
      bufferedWriter.close();
    } catch (IOException e) { e.printStackTrace(); }
  }

  private Class<?> loadClassWithName(ClassLoader cl, String name) {
    if (name.equals("int") || name.equals("int32_t")) {
      return Integer.class;
    } else if (name.equals("long") || name.equals("int64_t")) {
      return Long.class;
    } else if (name.equals("double")) {
      return Double.class;
    }
    throw new IllegalStateException("Unrecoginizable :" + name);
  }
}
