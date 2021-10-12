package com.alibaba.grape.sample.message;

import com.alibaba.fastffi.FFIByteString;
import com.alibaba.grape.app.DefaultContextBase;
import com.alibaba.grape.fragment.ImmutableEdgecutFragment;
import com.alibaba.grape.parallel.DefaultMessageManager;
import com.alibaba.grape.stdcxx.StdVector;

public class MessageDefaultContext implements DefaultContextBase<Long, Long, Long, Double> {
    public int step;
    public int maxStep;
    public long sendMsgTime;
    public long receiveMsgTime;
    public long numMsgSent;
    public long numMsgReceived;

    @Override
    public void Init(ImmutableEdgecutFragment<Long, Long, Long, Double> fragment, DefaultMessageManager messageManager, StdVector<FFIByteString> args) {
        maxStep = Integer.parseInt(args.get(2).toString());
        step = 0;
        sendMsgTime = 0;
        receiveMsgTime = 0;
        numMsgReceived = 0;
        numMsgSent = 0;
    }

    @Override
    public void Output(ImmutableEdgecutFragment<Long, Long, Long, Double> fragment) {
        System.out.println("Frag " + fragment.fid() + "send msg time " + sendMsgTime / 1000000000 + " receive msg time" + receiveMsgTime / 1000000000);
        System.out.println("Frag " + fragment.fid() + "sent msg number " + numMsgSent + " receive msg number" + numMsgReceived);
    }
}
