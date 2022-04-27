package com.alibaba.graphscope.conf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphXConf<VD, ED, MSG_T> {

    private static Logger logger = LoggerFactory.getLogger(GraphXConf.class.getName());

    private Class<? extends VD> vdataClass;
    private Class<? extends ED> edataClass;
    private Class<? extends MSG_T> msgClass;

    public GraphXConf(Class<? extends VD> vdataClass, Class<? extends ED> edataClass, Class<? extends MSG_T> msgClass){
        this.vdataClass = vdataClass;
        this.edataClass = edataClass;
        this.msgClass = msgClass;
    }
    public GraphXConf(){
    }

    public Class<? extends VD> getVdataClass() {
        return vdataClass;
    }

    public Class<? extends ED> getEdataClass() {
        return edataClass;
    }

    public Class<? extends MSG_T> getMsgClass() {
        return msgClass;
    }

    public void setVdataClass(Class<? extends VD> clz) {
        vdataClass = clz;
    }

    public void setVdataClass(String className) {
        this.vdataClass = loadClass(className);
    }

    public void setEdataClass(Class<? extends ED> clz) {
        edataClass = clz;
    }

    public void setEdataClass(String className) {
        this.edataClass = loadClass(className);
    }

    public void setMessageClass(Class<? extends MSG_T> clz) {
        msgClass = clz;
    }

    public void setMessageClass(String className) {
        this.msgClass = loadClass(className);
    }

    private <T> Class<T> loadClass(String vdataClass) {
        Class<T> res = null;
        try {
            res = (Class<T>) getClass().getClassLoader().loadClass(vdataClass);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        if (res == null) {
            throw new IllegalStateException("try to load class " + vdataClass + " failed");
        }
        return res;
    }

}
