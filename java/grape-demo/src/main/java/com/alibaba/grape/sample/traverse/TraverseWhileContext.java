package com.alibaba.grape.sample.traverse;

import com.alibaba.fastffi.FFIByteString;
import com.alibaba.grape.app.DefaultContextBase;
import com.alibaba.grape.fragment.ImmutableEdgecutFragment;
import com.alibaba.grape.parallel.DefaultMessageManager;
import com.alibaba.grape.stdcxx.StdVector;

/**
 * @author xiaolei.zl
 * @date 2021/06/14
 */
public class TraverseWhileContext implements DefaultContextBase<Long, Long, Long, Double> {

    public int step;
    public int maxStep;
    public long fake_vid;
    public double fake_edata;

    @Override
    public void Init(ImmutableEdgecutFragment<Long, Long, Long, Double> immutableEdgecutFragment,
                     DefaultMessageManager javaDefaultMessageManager, StdVector<FFIByteString> args) {
        maxStep = Integer.parseInt(args.get(0).toString());
        step = 0;
    }

    @Override
    public void Output(ImmutableEdgecutFragment<Long, Long, Long, Double> immutableEdgecutFragment) {

    }
}
