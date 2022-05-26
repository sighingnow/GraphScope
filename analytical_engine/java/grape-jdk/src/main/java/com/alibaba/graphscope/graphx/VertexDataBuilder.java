package com.alibaba.graphscope.graphx;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.arrow.array.ArrowArrayBuilder;
import com.alibaba.graphscope.stdcxx.FFIByteVector;
import com.alibaba.graphscope.stdcxx.FFIIntVector;
import com.alibaba.graphscope.stdcxx.StdSharedPtr;
import com.alibaba.graphscope.stdcxx.StdVector;
import com.alibaba.graphscope.utils.CppClassName;
import com.alibaba.graphscope.utils.CppHeaderName;

@FFIGen(library = "grape-jni")
@CXXHead(CppHeaderName.CORE_JAVA_GRAPHX_VERTEX_DATA_H)
@FFITypeAlias(CppClassName.GS_VERTEX_DATA_BUILDER)
public interface VertexDataBuilder<VID,VD> extends FFIPointer {

    @FFINameAlias("Init")
    void init(long frag_vnums, VD initValue);

    @FFINameAlias("Init")
    void init(@CXXReference ArrowArrayBuilder<VD> newValues);

    @FFINameAlias("Init")
    void init(long frag_vnums, @CXXReference FFIByteVector vector, @CXXReference FFIIntVector offset);

    @FFINameAlias("MySeal")
    @CXXValue StdSharedPtr<VertexData<VID,VD>> seal(@CXXReference VineyardClient client);

    @FFIFactory
    interface Factory<VID,VD>{
        VertexDataBuilder<VID,VD> create();

        default VertexData<VID,VD> createAndBuild(VineyardClient client, ArrowArrayBuilder<VD> newValues){
            VertexDataBuilder<VID,VD> builder = create();
            builder.init(newValues);
            StdSharedPtr<VertexData<VID,VD>> res = builder.seal(client);
            return res.get();
        }
    }
}
