package com.alibaba.graphscope.mm;

public interface MessageStore<MSG_T> {

    boolean messageAvailable(long lid);

    MSG_T getMessage(long lid);

    void addLidMessage(long lid, MSG_T msg);
}
