package com.alibaba.grape.sample.traverse;

import com.alibaba.ffi.FFIByteString;
import com.alibaba.grape.app.ParallelContextBase;
import com.alibaba.grape.fragment.ImmutableEdgecutFragment;
import com.alibaba.grape.parallel.ParallelMessageManager;
import com.alibaba.grape.stdcxx.StdVector;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author xiaolei.zl
 * @date 2021/06/14
 */
public class TraverseParallelContext implements ParallelContextBase<Long, Long, Long, Double> {

    public int step;
    public int maxStep;
    public long fake_vid;
    public double fake_edata;
    public ExecutorService executor;
    public int threadNum;
    public long chunkSize;

    @Override
    public void Init(ImmutableEdgecutFragment<Long, Long, Long, Double> immutableEdgecutFragment,
                     ParallelMessageManager javaDefaultMessageManager, StdVector<FFIByteString> args) {
        maxStep = Integer.parseInt(args.get(0).toString());
        threadNum = Integer.parseInt(args.get(1).toString());
        executor = Executors.newFixedThreadPool(threadNum);
        long innerVerticesNum = immutableEdgecutFragment.getInnerVerticesNum();
//        chunkSize = (innerVerticesNum + threadNum - 1) / threadNum;
        chunkSize = 1024;
        step = 0;
    }

    @Override
    public void Output(ImmutableEdgecutFragment<Long, Long, Long, Double> immutableEdgecutFragment) {

    }
}
