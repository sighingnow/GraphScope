package com.alibaba.grape.sample.lineparser;

import com.alibaba.fastffi.FFIByteString;
import com.alibaba.fastffi.FFITypeFactory;
import com.alibaba.grape.app.lineparser.EVLineParserBase;
import com.alibaba.grape.graph.context.MutationContext;
import com.alibaba.grape.sample.types.Edata;
import com.alibaba.grape.sample.types.Oid;
import com.alibaba.grape.sample.types.Vdata;

import java.io.IOException;

public class MirrorLineParser implements EVLineParserBase<Oid, Vdata, Edata> {
    public static Oid vertex = Oid.create();
    public static Oid from = Oid.create();
    public static Oid to = Oid.create();
    public static Vdata vdata = Vdata.create();
    public static Edata edata = Edata.create();
    @Override
    public void loadVertexLine(String s, MutationContext<Oid, Vdata, Edata> mutationContext) throws IOException {
        String[] splited = s.split("\\s+");
        vertex.id(Long.valueOf(splited[0]));
//        FFIByteString str = FFITypeFactory.newByteString();
//        str.copyFrom("0");
//        vertex.data(str);

//        FFIByteString data = FFITypeFactory.newByteString();
//        data.copyFrom("0");
//        vdata.strValue(data);
        vdata.data(1L);
        mutationContext.addVertexSimple(vertex,vdata);
    }

    @Override
    public void loadEdgeLine(String s, MutationContext<Oid, Vdata, Edata> mutationContext) throws IOException {
        String[] splited = s.split("\\s+");
        from.id(Long.valueOf(splited[0]));
        to.id(Long.valueOf(splited[1]));
//        from.data(data);
//        to.data(data);

        edata.data(Double.valueOf(splited[2]));
        mutationContext.addEdgeRequest(from, to ,edata);
    }
}
