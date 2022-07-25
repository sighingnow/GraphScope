package com.alibaba.graphscope.graphx;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFISerializable;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.arrow.array.ArrowArrayBuilder;
import com.alibaba.graphscope.stdcxx.StdSharedPtr;
import com.alibaba.graphscope.stdcxx.StdVector;
import com.alibaba.graphscope.utils.CppClassName;
import com.alibaba.graphscope.utils.CppHeaderName;

@FFIGen(library = "grape-jni")
@CXXHead(CppHeaderName.CORE_JAVA_GRAPHX_EDGE_DATA_H)
@CXXHead(CppHeaderName.VINEYARD_ARRAY_BUILDER_H)
@FFITypeAlias(CppClassName.GS_EDGE_DATA_BUILDER)
public interface EdgeDataBuilder<VID, ED> extends FFISerializable {

//    @FFINameAlias("Init")
//    void init(@CXXReference VineyardArrayBuilder<ED> newValues);

    @FFINameAlias("MySeal")
    @CXXValue StdSharedPtr<EdgeData<VID, ED>> seal(@CXXReference VineyardClient client);

    @FFIFactory
    interface Factory<VID, ED> {

        EdgeDataBuilder<VID, ED> create(@CXXReference VineyardClient client,@CXXReference StdVector<ED> arrayBuilder);

        default EdgeData<VID, ED> createAndBuild(VineyardClient client,
            StdVector<ED> newValues) {
            EdgeDataBuilder<VID, ED> builder = create(client,newValues);
            StdSharedPtr<EdgeData<VID, ED>> res = builder.seal(client);
            return res.get();
        }
    }
}
