package io.graphscope.fragment;

import com.alibaba.ffi.*;
import com.alibaba.grape.stdcxx.StdUnorderedMap;
import io.graphscope.utils.CPP_CLASS;
import io.graphscope.utils.CPP_HEADER;
import io.graphscope.utils.CPP_JNI_LIBRARY;

import static com.alibaba.grape.utils.CPP_CLASSES_STRINGS.STD_UNORDERED_MAP;

@FFIGen(library = CPP_JNI_LIBRARY.VINEYARD_JNI_LIBRARY)
@CXXHead(CPP_HEADER.ARROW_FRAGMENT_GROUP_H)
@CXXHead(system = "stdint.h")
@FFITypeAlias(CPP_CLASS.ARROW_FRAGMENT_GROUP)
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
