package com.alibaba.graphscope.arrow;

import static com.alibaba.graphscope.utils.CppClassName.ARROW_STATUS;
import static com.alibaba.graphscope.utils.CppHeaderName.ARROW_STATUS_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFISerializable;
import com.alibaba.fastffi.FFITypeAlias;

@FFIGen
@CXXHead(ARROW_STATUS_H)
@FFITypeAlias(ARROW_STATUS)
public interface Status extends FFISerializable {

    boolean ok();
}
