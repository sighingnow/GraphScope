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
