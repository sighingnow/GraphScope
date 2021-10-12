package io.graphscope.fragment;

import com.alibaba.fastffi.*;
import com.alibaba.grape.stdcxx.StdUnorderedMap;
import io.graphscope.utils.CppClassName;
import io.graphscope.utils.CppHeaderName;
import io.graphscope.utils.JNILibraryName;

import static com.alibaba.grape.utils.CppClassName.STD_UNORDERED_MAP;

@FFIGen(library = JNILibraryName.VINEYARD_JNI_LIBRARY)
@CXXHead(CppHeaderName.ARROW_FRAGMENT_GROUP_H)
@CXXHead(system = "stdint.h")
@FFITypeAlias(CppClassName.ARROW_FRAGMENT_GROUP)
public interface ArrowFragmentGroup extends CXXPointer {
    @FFINameAlias("total_frag_num")
    int totalFragNum();

    @FFINameAlias("edge_label_num")
    int edgeLabelNum();

    @FFINameAlias("vertex_label_num")
    int vertexLabelNum();

    //compiling error
    @FFINameAlias("Fragments")
    @CXXReference @FFITypeAlias(STD_UNORDERED_MAP + "<unsigned,uint64_t>") StdUnorderedMap<Integer, Long> fragments();

    @FFINameAlias("FragmentLocations")
    @CXXReference @FFITypeAlias(STD_UNORDERED_MAP + "<unsigned,uint64_t>") StdUnorderedMap<Integer, Long> fragmentLocations();
}
