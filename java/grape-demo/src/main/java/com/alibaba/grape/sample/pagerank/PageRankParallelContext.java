package com.alibaba.grape.sample.pagerank;

import com.alibaba.fastffi.FFIByteString;
import com.alibaba.grape.app.ParallelContextBase;
import com.alibaba.grape.ds.Vertex;
import com.alibaba.grape.ds.VertexRange;
import com.alibaba.grape.fragment.ImmutableEdgecutFragment;
import com.alibaba.grape.parallel.MessageInBuffer;
import com.alibaba.grape.parallel.ParallelMessageManager;
import com.alibaba.grape.stdcxx.StdVector;
import com.alibaba.grape.utils.DoubleArrayWrapper;
import com.alibaba.grape.utils.FFITypeFactoryhelper;
import com.alibaba.grape.utils.IntArrayWrapper;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class PageRankParallelContext implements ParallelContextBase<Long, Long, Long, Double> {
    public double alpha;
    public int maxIteration;
    public int superStep;
    public double danglingSum;

    public DoubleArrayWrapper pagerank;
    public DoubleArrayWrapper nextResult;
    public IntArrayWrapper degree;
    public int thread_num;
    public ExecutorService executor;
    public MessageInBuffer.Factory bufferFactory;
    public int chunkSize;
    public double sumDoubleTime = 0.0;
    public double swapTime = 0.0;
    public int danglingVNum;

    @Override
    public void Init(ImmutableEdgecutFragment<Long, Long, Long, Double> frag, ParallelMessageManager javaParallelMessageManager, StdVector<FFIByteString> args) {
        bufferFactory = FFITypeFactoryhelper.newMessageInBuffer();
        alpha = Double.parseDouble(args.get(0).toString());
        maxIteration = Integer.parseInt(args.get(1).toString());
        thread_num = Integer.parseInt(args.get(2).toString());
        System.out.println("alpha: [" + alpha + "], max iteration: [" + maxIteration + "], thread num " + thread_num);
        pagerank = new DoubleArrayWrapper((frag.getVerticesNum().intValue()), 0.0);
        nextResult = new DoubleArrayWrapper((frag.getInnerVerticesNum().intValue()), 0.0);
        degree = new IntArrayWrapper(frag.getInnerVerticesNum().intValue(), 0);
        executor = Executors.newFixedThreadPool(thread_num());
        chunkSize = 1024;
        danglingVNum = 0;
    }

    @Override
    public void Output(ImmutableEdgecutFragment<Long, Long, Long, Double> frag) {
        String prefix = "/tmp/pagerank_parallel_output";
        System.out.println("sum double " + sumDoubleTime / 10e9 + " swap time " + swapTime / 10e9);
        String filePath = prefix + "_frag_" + String.valueOf(frag.fid());
        try {
            FileWriter fileWritter = new FileWriter(new File(filePath));
            BufferedWriter bufferedWriter = new BufferedWriter(fileWritter);
            VertexRange<Long> innerNodes = frag.innerVertices();

            Vertex<Long> cur = innerNodes.begin();
            for (long index = 0; index < frag.getInnerVerticesNum(); ++index) {
                cur.SetValue(index);
                Long oid = frag.getId(cur);
                bufferedWriter.write(cur.GetValue() + "\t" + oid + "\t" + pagerank.get(index) + "\n");
            }
            bufferedWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int thread_num() {
        return thread_num;
    }
}
