package com.alibaba.grape.jobConf;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ffi.FFITypeFactory;
import com.alibaba.grape.app.lineparser.EVLineParserBase;
import com.alibaba.grape.graph.loader.tableLoader.TableInfo;
import com.aliyun.odps.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

public class JobConf {
    private static Logger logger = LoggerFactory.getLogger(JobConf.class.getName());
    private JSONObject conf;
    private String confPath;

    public JobConf() {
        conf = new JSONObject();
        confPath = JOB_CONF.DEFAULT_JOB_CONF_PATH;
    }

    public JobConf(String str) {
        conf = new JSONObject();
        confPath = str;
    }

    public boolean submit() throws IOException {
        //since fileWriter and printWriter are all autoClosable,
        // we use this syntax to avoid finally usage.
        try (FileWriter fw = new FileWriter(confPath); PrintWriter pw = new PrintWriter(fw)) {
            pw.write(conf.toJSONString());
            logger.info("Persisting Job Configuration to " + confPath);
        } catch (IOException e) {
            return false;
        }
        return true;
    }

    public void run() {
        try {
            if (!submit()) {
                logger.error("Persisting configuration failed");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void clear() {
        conf.clear();
    }

    public void readConfig() {
        readConfig(confPath);
    }

    public void readConfig(String path) {
        try (FileReader fr = new FileReader(path); BufferedReader br = new BufferedReader(fr)) {
            StringBuilder sb = new StringBuilder();
            String tempString = null;
            while ((tempString = br.readLine()) != null) {
                sb.append(tempString);
            }
            conf = JSON.parseObject(sb.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int size() {
        return conf.size();
    }

    public void setDirected(Boolean directed) {
        this.set(JOB_CONF.DIRECTED, directed.toString());
    }

    /**
     * In default, a graph is directed
     *
     * @return A boolean indicate whether directed or not;
     */
    public boolean getDirected() {
        return Boolean.valueOf(this.get(JOB_CONF.DIRECTED, "true"));
    }

    public void setOidType(String oidType) {
        this.set(JOB_CONF.OID_TYPE, oidType);
    }

    public String getOidType() {
        return this.get(JOB_CONF.OID_TYPE, "null");
    }

    public void setVidType(String vidType) {
        this.set(JOB_CONF.VID_TYPE, vidType);
    }

    public String getVidType() {
        return this.get(JOB_CONF.VID_TYPE, "null");
    }

    public void setVdataType(String vdataType) {
        this.set(JOB_CONF.VERTEX_DATA_TYPE, vdataType);
    }

    public String getVdataType() {
        return this.get(JOB_CONF.VERTEX_DATA_TYPE, "null");
    }

    public void setEdataType(String edataType) {
        this.set(JOB_CONF.EDGE_DATA_TYPE, edataType);
    }

    public String getEdataType() {
        return this.get(JOB_CONF.EDGE_DATA_TYPE, "null");
    }

    public void setEfilePath(String efilePath) {
        this.conf.put(JOB_CONF.EDGE_FILE_PATH, efilePath);
    }

    public String getEfilePath() {
        return get(JOB_CONF.EDGE_FILE_PATH, "null");
    }

    public void setVfilePath(String vfilePath) {
        this.conf.put(JOB_CONF.VERTEX_FILE_PATH, vfilePath);
    }

    public String getVfilePath() {
        return get(JOB_CONF.VERTEX_FILE_PATH, "null");
    }
//  public void  setMessageTypes(String ...messageTypes){
//    if (messageTypes.length == 0) return ;
//    this.setMessageTypes(JOB_CONF.MESSAGE_TYPES, messageTypes);
//  }

    /**
     * Set the types of messages you want to use. PLS don't use primitive types,
     * since methods like @link{ com.alibaba.grape.message.messageManager#getMsg() }
     * expect you pass a reference to contain the received msg.
     *
     * @param clz Class you want to use as msg types.
     * @
     */
    public void setMessageTypes(Class<?>... clz) {
        List<String> messagesTuples = new ArrayList<>(clz.length);
        Arrays.stream(clz).forEach(c -> messagesTuples.add(FFITypeFactory.getFFITypeName(c, true) + "=" + c.getName()));
        setMessageTypes(JOB_CONF.MESSAGE_TYPES, String.join(",", messagesTuples));
    }

    public String[] getMessageTypesArray() {
        return this.get(JOB_CONF.MESSAGE_TYPES).split(",");
    }

    public String getMessageTypesString() {
        return this.get(JOB_CONF.MESSAGE_TYPES);
    }

    public void setAppClass(String appClasString) {
        this.set(JOB_CONF.APP_CLASS, appClasString);
    }

    /**
     * @param appContextClasString
     */
    public void setAppContextClass(String appContextClasString) {
        this.set(JOB_CONF.APP_CONTEXT_CLASS, appContextClasString);
    }

    public String get(String key, String value) {
        String res = conf.getString(key);
        if (res == null || res.length() <= 0) {
            return value;
        }
        return res;
    }

    public String get(String key) {
        return conf.getString(key);
    }

    public void set(String key, String value) {
        conf.put(key, value);
    }

    public void addInput(TableInfo tbl) throws IOException {
        addInput(tbl, tbl.getCols());
    }

    public void addInput(TableInfo tbl, String[] cols) throws IOException {
        tbl.validate();
        String inputDesc = get(JOB_CONF.INPUT_TABLES, "[]");
        JSONArray array = JSONObject.parseArray(inputDesc);
        array.add(toJson(tbl, cols));
        set(JOB_CONF.INPUT_TABLES, array.toJSONString());
    }

    public void addOutput(TableInfo tbl) throws IOException {
        addOutput(tbl, true);
    }

    public void addOutput(TableInfo tbl, boolean overwrite) throws IOException {
        if (!tbl.getLabel().equals(TableInfo.DEFAULT_LABEL)) {
            processOutput(tbl, tbl.getLabel(), overwrite);
        } else {
            processOutput(tbl, null, overwrite);
        }
    }

    public void setEVLineParserClassName(String value) {
        this.conf.put(JOB_CONF.EV_FILE_LINE_PARSER, value);
    }

    @SuppressWarnings("rawtypes")
    public Class<? extends EVLineParserBase> getEVLineParser() {
        if (StringUtils.isEmpty(this.get(JOB_CONF.EV_FILE_LINE_PARSER))) {
            return null;
        }
        Class<? extends EVLineParserBase> res = null;
        try {
            res = (Class<? extends EVLineParserBase>) Class.forName(this.get(JOB_CONF.EV_FILE_LINE_PARSER));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return res;
    }


    private void processOutput(TableInfo tbl, String label, boolean overwrite) {
        String outputDesc = get(JOB_CONF.OUTPUT_TABLES, "[]");
        JSONArray array = JSONObject.parseArray(outputDesc);
        array.add(toJson(tbl, label, overwrite));
        set(JOB_CONF.OUTPUT_TABLES, array.toJSONString());
    }

    /**
     * 设置最大迭代次数，默认为0
     *
     * @param maxIteration 最大迭代次数
     */
    public void setMaxIteration(int maxIteration) {
        conf.put(JOB_CONF.MAX_ITERATION, maxIteration);
    }

    /**
     * 获取指定的最大迭代次数，0
     *
     * @return 最大迭代次数
     */
    public int getMaxIteration() {
        return conf.getIntValue(JOB_CONF.MAX_ITERATION);
    }

    private static JSONObject toJson(TableInfo tbl, String[] cols) {
        JSONObject obj = new JSONObject();
        String projectName = tbl.getProjectName();
        if (projectName == null) {
            projectName = "";
        }
        obj.put("projName", projectName);
        obj.put("tblName", tbl.getTableName());
        JSONArray array = new JSONArray();
        LinkedHashMap<String, String> partSpec = tbl.getPartSpec();
        for (Map.Entry<String, String> entry : partSpec.entrySet()) {
            JSONObject tmp = new JSONObject();
            String key = StringUtils.strip(entry.getKey(), "'\"");
            String value = StringUtils.strip(entry.getValue(), "'\"");
            tmp.put(key, value);
            array.add(tmp);
        }
        obj.put("partSpec", array);
        obj.put("cols", cols == null ? "" : StringUtils.join(cols, ','));
        obj.put("loaderClassName", tbl.getLoaderClassName());
        // Gson gson = new GsonBuilder().disableHtmlEscaping().create(); // remove unicode characters
        // obj = new JsonParser().parse(gson.toJson(obj)).getAsJsonObject();
        return obj;
    }

    private JSONObject toJson(TableInfo tbl, String label, boolean overwrite) {
        JSONObject obj = new JSONObject();
        String projectName = tbl.getProjectName();
        if (projectName == null) {
            projectName = "";
        }
        obj.put("projName", projectName);
        obj.put("tblName", tbl.getTableName());
        JSONArray array = new JSONArray();
        LinkedHashMap<String, String> partSpec = tbl.getPartSpec();
        for (Map.Entry<String, String> entry : partSpec.entrySet()) {
            JSONObject tmp = new JSONObject();
            tmp.put(entry.getKey(), entry.getValue());
            array.add(tmp);
        }
        obj.put("partSpec", array);
        obj.put("label", label == null ? "" : label);
        obj.put("loaderClassName", tbl.getLoaderClassName());
        obj.put("overwrite", overwrite);
        return obj;
    }

    private void setMessageTypes(String key, String... msgTypes) {
        this.conf.put(key, String.join(",", msgTypes));
    }
}
