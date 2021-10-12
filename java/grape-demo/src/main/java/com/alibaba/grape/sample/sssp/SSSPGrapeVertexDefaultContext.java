package com.alibaba.grape.sample.sssp;

import com.alibaba.fastffi.FFIByteString;
import com.alibaba.fastffi.FFITypeFactory;
import com.alibaba.grape.app.DefaultContextBase;
import com.alibaba.grape.ds.DenseVertexSet;
import com.alibaba.grape.ds.Vertex;
import com.alibaba.grape.ds.VertexRange;
import com.alibaba.grape.fragment.ImmutableEdgecutFragment;
import com.alibaba.grape.parallel.DefaultMessageManager;
import com.alibaba.grape.stdcxx.StdVector;
import com.alibaba.grape.utils.DoubleArrayWrapper;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class SSSPGrapeVertexDefaultContext implements DefaultContextBase<Long, Long, Long, Double> {
    public DoubleArrayWrapper partialResults;
    // private BooleanArrayWrapper curModified;
    // private BooleanArrayWrapper nextModified;
//    public VertexSet curModified;
//    public VertexSet nextModified;
    public DenseVertexSet<Long> curModified;
    public DenseVertexSet<Long> nextModified;
    public DenseVertexSet.Factory vertexSetFactory =
            FFITypeFactory.getFactory(DenseVertexSet.class, "grape::DenseVertexSet<uint64_t>");
    public double execTime = 0.0;
    public double sendMessageTime = 0.0;
    public double receiveMessageTIme = 0.0;
    public double postProcessTime = 0.0;
    public long nbrSize = 0;
    public long numOfNbrs = 0;

    // public DoubleMessageAdaptor<Long, Long> messager;

    public Long sourceOid;

    public SSSPGrapeVertexDefaultContext() {
    }

    public DoubleArrayWrapper getPartialResults() {
        return partialResults;
    }

    public DenseVertexSet<Long> getCurModified() {
        return curModified;
    }

    public DenseVertexSet<Long> getNextModified() {
        return nextModified;
    }

    public Long getSourceOid() {
        return sourceOid;
    }

    @Override
    public void Init(ImmutableEdgecutFragment<Long, Long, Long, Double> frag,
                     DefaultMessageManager mm, StdVector<FFIByteString> args) {
        Long allVertexNum = frag.getVerticesNum();
        VertexRange<Long> vertices = frag.vertices();
        partialResults = new DoubleArrayWrapper(allVertexNum.intValue(), Double.MAX_VALUE);
//        curModified = new VertexSet(0, allVertexNum.intValue());
//        nextModified = new VertexSet(0, allVertexNum.intValue());
        curModified = vertexSetFactory.create();
        nextModified = vertexSetFactory.create();
        curModified.Init(vertices);
        nextModified.Init(vertices);

        // args 0, 1 are app class and app ctx class
        sourceOid = Long.valueOf(args.get(0).toString());
        // System.out.println("SSSPContext.init : source oid = " + sourceOid);

        // messager = new DoubleMessageAdaptor<Long, Long>(frag);
    }

    @Override
    public void Output(ImmutableEdgecutFragment<Long, Long, Long, Double> frag) {
        String prefix = "/tmp/sssp_output";
        System.out.println("frag: " + frag.fid() + " sendMessageTime: " + sendMessageTime / 1000000000);
        System.out.println("frag: " + frag.fid()
                + " receiveMessageTime: " + receiveMessageTIme / 1000000000);
        System.out.println("frag: " + frag.fid() + " execTime: " + execTime / 1000000000);
        System.out.println("frag: " + frag.fid() + " postProcessTime: " + postProcessTime / 1000000000);
        System.out.println("frag: " + frag.fid() + " number of neighbor: " + numOfNbrs);

        String filePath = prefix + "_frag_" + frag.fid();
        try {
            FileWriter fileWritter = new FileWriter(filePath);
            BufferedWriter bufferedWriter = new BufferedWriter(fileWritter);
            VertexRange<Long> innerNodes = frag.innerVertices();

            // ArrayListWrapper<Long> partialResults = this.getPartialResults();
            // System.out.println(frag.GetInnerVerticesNum() + " " + innerNodes.begin().GetValue() + " "
            //                    + innerNodes.end().GetValue());
            // for (Vertex<Long> cur = innerNodes.begin(); cur.GetValue() != innerNodes.end().GetValue();
            //      cur.inc()) {
            Vertex<Long> cur = innerNodes.begin();
            for (long index = 0; index < frag.getInnerVerticesNum(); ++index) {
                cur.SetValue(index);
                Long oid = frag.getId(cur);
                bufferedWriter.write(cur.GetValue() + "\t" + oid + "\t" + partialResults.get(index) + "\n");
            }
            bufferedWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}