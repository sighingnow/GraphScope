package com.alibaba.graphscope.graphx;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFISerializable;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.fragment.ArrowProjectedFragment;
import com.alibaba.graphscope.stdcxx.StdSharedPtr;
import com.alibaba.graphscope.utils.CppClassName;
import com.alibaba.graphscope.utils.CppHeaderName;

@FFIGen
@CXXHead(CppHeaderName.ARROW_FRAGMENT_H)
@CXXHead(CppHeaderName.CORE_JAVA_FRAGMENT_GETTER_H)
@FFITypeAlias(CppClassName.GS_ARROW_PROJECTED_FRAGMENT_GETTER)
public interface ArrowProjectedFragmentGetter<OID,VID,VD,ED> extends FFISerializable {

    @FFINameAlias("Get")
    @CXXValue StdSharedPtr<ArrowProjectedFragment<OID,VID,VD,ED>> get(@CXXReference VineyardClient client, long objectID);

    @FFIFactory
    interface Factory<OID,VID,VD,ED> {
        ArrowProjectedFragmentGetter<OID,VID,VD,ED> create();
    }
}
