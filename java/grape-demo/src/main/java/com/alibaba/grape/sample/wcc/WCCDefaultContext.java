package com.alibaba.grape.sample.wcc;

import com.alibaba.fastffi.FFIByteString;
import com.alibaba.grape.app.DefaultContextBase;
import com.alibaba.grape.ds.Vertex;
import com.alibaba.grape.ds.VertexRange;
import com.alibaba.grape.ds.VertexSet;
import com.alibaba.grape.fragment.ImmutableEdgecutFragment;
import com.alibaba.grape.parallel.DefaultMessageManager;
import com.alibaba.grape.stdcxx.StdVector;
import com.alibaba.grape.utils.LongArrayWrapper;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class WCCDefaultContext implements DefaultContextBase<Long, Long, Long, Double> {
    public VertexSet currModified, nextModified;
    public LongArrayWrapper comp_id;
    public int innerVerticesNum;

    @Override
    public void Init(ImmutableEdgecutFragment<Long, Long, Long, Double> frag, DefaultMessageManager messageManager, StdVector<FFIByteString> args) {
        comp_id = new LongArrayWrapper(frag.vertices(), Long.MAX_VALUE);
        currModified = new VertexSet(frag.vertices());
        nextModified = new VertexSet(frag.vertices());
        innerVerticesNum = frag.getInnerVerticesNum().intValue();
    }

    @Override
    public void Output(ImmutableEdgecutFragment<Long, Long, Long, Double> frag) {
        String prefix = "/tmp/wcc_default_output";
        String filePath = prefix + "_frag_" + frag.fid();
        try {
            FileWriter fileWritter = new FileWriter(filePath);
            BufferedWriter bufferedWriter = new BufferedWriter(fileWritter);
            VertexRange<Long> innerNodes = frag.innerVertices();

            Vertex<Long> cur = innerNodes.begin();
            for (long index = 0; index < frag.getInnerVerticesNum(); ++index) {
                cur.SetValue(index);
                Long oid = frag.getId(cur);
                bufferedWriter.write(oid + "\t" + comp_id.get(index) + "\n");
            }
            bufferedWriter.close();
            System.out.println("writing output to " + filePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
