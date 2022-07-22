package com.alibaba.graphscope.graphx;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFISerializable;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.fragment.GraphXFragment;
import com.alibaba.graphscope.stdcxx.StdSharedPtr;
import com.alibaba.graphscope.utils.CppClassName;
import com.alibaba.graphscope.utils.CppHeaderName;

@FFIGen(library = "grape-jni")
@CXXHead(CppHeaderName.CORE_JAVA_GRAPHX_GRAPHX_FRAGMENT_H)
@FFITypeAlias(CppClassName.GRAPHX_FRAGMENT_BUILDER)
public interface GraphXFragmentBuilder<OID_T, VID_T, VD_T, ED_T> extends FFISerializable {

    @FFINameAlias("MySeal")
    @CXXValue StdSharedPtr<GraphXFragment<OID_T,VID_T,VD_T, ED_T>> seal(@CXXReference VineyardClient client);

    @FFIFactory
    interface Factory<OID_T, VID_T, VD_T, ED_T> {

        GraphXFragmentBuilder<OID_T, VID_T, VD_T, ED_T> create(
            @CXXReference VineyardClient vineyardClient, long vmId, long csrId, long vdataId, long edataId);
        GraphXFragmentBuilder<OID_T, VID_T, VD_T, ED_T> create(
            @CXXReference VineyardClient vineyardClient, @CXXReference GraphXVertexMap<OID_T,VID_T> vm, @CXXReference GraphXCSR<VID_T> csr,@CXXReference VertexData<VID_T,VD_T> vdata,@CXXReference EdgeData<VID_T,ED_T> edata);

    }


}
