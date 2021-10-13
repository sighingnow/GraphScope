/*
 * Copyright 2021 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.grape.sample.message;

import com.alibaba.fastffi.FFIByteString;
import com.alibaba.grape.app.DefaultContextBase;
import com.alibaba.grape.fragment.ImmutableEdgecutFragment;
import com.alibaba.grape.parallel.DefaultMessageManager;
import com.alibaba.grape.sample.types.Edata;
import com.alibaba.grape.sample.types.Oid;
import com.alibaba.grape.sample.types.Vdata;
import com.alibaba.grape.stdcxx.StdVector;

public class MessageMirrorDefaultContext implements DefaultContextBase<Oid, Long, Vdata, Edata> {
    public int step;
    public int maxStep;
    public long sendMsgTime;
    public long receiveMsgTime;
    public long numMsgSent;
    public long numMsgReceived;

    @Override
    public void Init(ImmutableEdgecutFragment<Oid, Long, Vdata, Edata> fragment, DefaultMessageManager messageManager, StdVector<FFIByteString> args) {
        maxStep = Integer.parseInt(args.get(2).toString());
        step = 0;
        sendMsgTime = 0;
        receiveMsgTime = 0;
        numMsgReceived = 0;
        numMsgSent = 0;
    }

    @Override
    public void Output(ImmutableEdgecutFragment<Oid, Long, Vdata, Edata> fragment) {
        System.out.println("Frag " + fragment.fid() + "send msg time " + sendMsgTime / 1000000000 + " receive msg time" + receiveMsgTime / 1000000000);
        System.out.println("Frag " + fragment.fid() + "sent msg number " + numMsgSent + " receive msg number" + numMsgReceived);
    }
}
