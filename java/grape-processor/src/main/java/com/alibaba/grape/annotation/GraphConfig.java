package com.alibaba.grape.annotation;

public class GraphConfig {

    final String vidType;
    final String oidType;
    final String vdataType;
    final String edataType;
    String messageTypes;
    final String fragmentType;
    final String cppVidType;
    final String cppOidType;
    final String cppVdataType;
    final String cppEdataType;

    public GraphConfig(String oidType, String vidType, String vdataType, String edataType, String messageTypes, String fragmentType, String cppOidType, String cppVidType, String cppVdataType, String cppEdataType) {
        this.vidType = vidType;
        this.oidType = oidType;
        this.vdataType = vdataType;
        this.edataType = edataType;
        this.messageTypes = messageTypes;
        this.fragmentType = fragmentType;
        this.cppVidType = cppVidType;
        this.cppOidType = cppOidType;
        this.cppVdataType = cppVdataType;
        this.cppEdataType = cppEdataType;
        System.out.println("init graph config with: " + this.oidType + " " + this.vidType + " " + this.vdataType + " " + this.edataType + " " + this.cppOidType + " " + this.cppVidType + " " + this.cppVdataType + " " + this.cppEdataType + " ");
    }

}
