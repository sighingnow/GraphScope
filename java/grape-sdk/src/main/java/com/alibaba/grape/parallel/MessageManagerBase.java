package com.alibaba.grape.parallel;

import com.alibaba.fastffi.FFIPointer;

public interface MessageManagerBase extends FFIPointer {


    void Start();

    void StartARound();

    void FinishARound();

    void Finalize();

    boolean ToTerminate();

    long GetMsgSize();

    void ForceContinue();
    //void sumDouble(double msg_in, @CXXReference @FFITypeAlias("grape::DoubleMsg") DoubleMsg msg_out);
}