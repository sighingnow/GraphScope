package com.alibaba.grape.sample.sssp;

import com.alibaba.ffi.FFIByteString;
import com.alibaba.grape.app.DefaultContextBase;
import com.alibaba.grape.ds.Vertex;
import com.alibaba.grape.ds.VertexRange;
import com.alibaba.grape.ds.VertexSet;
import com.alibaba.grape.fragment.ImmutableEdgecutFragment;
import com.alibaba.grape.parallel.DefaultMessageManager;
import com.alibaba.grape.sample.sssp.mirror.SSSPEdata;
import com.alibaba.grape.sample.sssp.mirror.SSSPOid;
import com.alibaba.grape.sample.sssp.mirror.SSSPVdata;
import com.alibaba.grape.stdcxx.StdVector;
import com.alibaba.grape.utils.DoubleArrayWrapper;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class SSSPMirrorDefaultContext implements DefaultContextBase<SSSPOid, Long, SSSPVdata, SSSPEdata> {
    public DoubleArrayWrapper partialResults;
    // private BooleanArrayWrapper curModified;
    // private BooleanArrayWrapper nextModified;
    public VertexSet curModified;
    public VertexSet nextModified;
    public double execTime = 0.0;
    public double sendMessageTime = 0.0;
    public double receiveMessageTIme = 0.0;
    public double postProcessTime = 0.0;
    public long nbrSize = 0;
    public long numOfNbrs = 0;

    // public DoubleMessageAdaptor<Long, Long> messager;

    public SSSPOid sourceOid;

    public DoubleArrayWrapper getPartialResults() {
        return partialResults;
    }

    public VertexSet getCurModified() {
        return curModified;
    }

    public VertexSet getNextModified() {
        return nextModified;
    }

    public SSSPOid getSourceOid() {
        return sourceOid;
    }

    @Override
    public void Init(ImmutableEdgecutFragment<SSSPOid, Long, SSSPVdata, SSSPEdata> frag,
                     DefaultMessageManager mm, StdVector<FFIByteString> args) {
        Long allVertexNum = frag.getVerticesNum();
        partialResults = new DoubleArrayWrapper(allVertexNum.intValue(), Double.MAX_VALUE);
        curModified = new VertexSet(0, allVertexNum.intValue());
        nextModified = new VertexSet(0, allVertexNum.intValue());

        // args 0, 1 are app class and app ctx class
        sourceOid = SSSPOid.create();
        sourceOid.value(Long.valueOf(args.get(0).toString()));
    }

    @Override
    public void Output(ImmutableEdgecutFragment<SSSPOid, Long, SSSPVdata, SSSPEdata> frag) {
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
                SSSPOid oid = frag.getId(cur);
                bufferedWriter.write(cur.GetValue() + "\t" + oid.value() + "\t" + partialResults.get(index) + "\n");
            }
            bufferedWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}