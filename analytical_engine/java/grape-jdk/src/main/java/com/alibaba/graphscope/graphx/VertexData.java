package com.alibaba.graphscope.graphx;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFISerializable;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.ds.ImmutableTypedArray;
import com.alibaba.graphscope.utils.CppClassName;
import com.alibaba.graphscope.utils.CppHeaderName;

@FFIGen(library = "grape-jni")
@CXXHead(CppHeaderName.CORE_JAVA_GRAPHX_VERTEX_DATA_H)
@FFITypeAlias(CppClassName.GS_VERTEX_DATA)
public interface VertexData<VID, VD> extends FFISerializable {
  long id();
  /**
   * Could contain outer vertices data
   * @return nums
   */
  @FFINameAlias("VerticesNum") VID verticesNum();

  @FFINameAlias("GetData") VD getData(VID lid);

  @FFINameAlias("GetVdataArray") @CXXReference ImmutableTypedArray<VD> getVdataArray();

//  @FFINameAlias("GetWords") @CXXReference @FFITypeAlias("gs::graphx::ImmutableTypedArray<int64_t>") ImmutableTypedArray<Long> getWords();
}
