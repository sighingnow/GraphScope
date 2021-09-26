package io.v6d.modules.graph.fragment;

import com.alibaba.ffi.*;
import com.alibaba.grape.stdcxx.StdUnorderedMap;

import static com.alibaba.grape.utils.CPP_CLASSES_STRINGS.STD_UNORDERED_MAP;
import static io.v6d.modules.graph.utils.CPP_CLASS.ARROW_FRAGMENT_GROUP;
import static io.v6d.modules.graph.utils.CPP_HEADER.ARROW_FRAGMENT_GROUP_H;
import static io.v6d.modules.graph.utils.CPP_JNI_LIBRARY.VINEYARD_JNI_LIBRARY;

@FFIGen(library = VINEYARD_JNI_LIBRARY)
@CXXHead(ARROW_FRAGMENT_GROUP_H)
@CXXHead(system = "stdint.h")
@FFITypeAlias(ARROW_FRAGMENT_GROUP)
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
