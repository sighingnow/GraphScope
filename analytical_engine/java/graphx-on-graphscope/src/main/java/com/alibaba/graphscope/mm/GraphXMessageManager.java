package com.alibaba.graphscope.mm;

public interface GraphXMessageManager<MSG_T> {

    void sendMessage(long dstOid,MSG_T msg);
}
