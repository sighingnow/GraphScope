package com.alibaba.graphscope.mm.impl;

import com.alibaba.graphscope.conf.GraphXConf;
import com.alibaba.graphscope.mm.MessageStore;

public class DefaultMessageStore<MSG_T> implements MessageStore<MSG_T> {

    public DefaultMessageStore(GraphXConf conf, long vertexRange){

    }

    @Override
    public boolean messageAvailable(long lid) {
        return false;
    }

    @Override
    public MSG_T getMessage(long lid) {
        return null;
    }

    @Override
    public void addLidMessage(long lid, MSG_T msg) {

    }
}
