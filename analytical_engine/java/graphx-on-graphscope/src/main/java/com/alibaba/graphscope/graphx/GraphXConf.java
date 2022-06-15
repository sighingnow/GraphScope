package com.alibaba.graphscope.graphx;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class GraphXConf<VD,ED,MSG> {
    private Set<Class<?>> primitiveClasses = new HashSet<Class<?>>(Arrays.asList(Long.class, long.class, Integer.class, int.class, Double.class, double.class));
    private Class<? extends VD> vdClass;
    private Class<? extends ED> edClass;
    private Class<? extends MSG> msgClass;
    public GraphXConf(Class<? extends VD> vdClass, Class<? extends ED> edClass, Class<? extends MSG> msgClass){
        this.vdClass = vdClass;
        this.edClass = edClass;
        this.msgClass = msgClass;
    }

    public void setVdClass(Class<? extends VD> vdClass) {
        this.vdClass = vdClass;
    }

    public void setMsgClass(Class<? extends MSG> msgClass) {
        this.msgClass = msgClass;
    }

    public void setEdClass(Class<? extends ED> edClass) {
        this.edClass = edClass;
    }

    public Class<? extends ED> getEdClass() {
        return edClass;
    }

    public Class<? extends MSG> getMsgClass() {
        return msgClass;
    }

    public Class<? extends VD> getVdClass() {
        return vdClass;
    }


    public boolean isVDPrimitive(){
        return primitiveClasses.contains(vdClass);
    }

    public boolean isEDPrimitive(){
        return primitiveClasses.contains(edClass);
    }
}
