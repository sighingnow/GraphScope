package io.v6d.modules.graph.ds;

import com.alibaba.ffi.*;

import java.util.Iterator;

import static io.v6d.modules.graph.utils.CPP_CLASS.PROPERTY_RAW_ADJ_LIST;
import static io.v6d.modules.graph.utils.CPP_HEADER.PROPERTY_GRAPH_UTILS_H;
import static io.v6d.modules.graph.utils.CPP_JNI_LIBRARY.VINEYARD_JNI_LIBRARY;

@FFIGen(library = VINEYARD_JNI_LIBRARY)
@CXXHead(PROPERTY_GRAPH_UTILS_H)
@FFITypeAlias(PROPERTY_RAW_ADJ_LIST)
@CXXTemplate(cxx = {"uint64_t"}, java = {"java.lang.Long"})
public interface PropertyRawAdjList<VID_T> extends FFIPointer, CXXPointer {
    @FFINameAlias("begin")
    PropertyNbrUnit<VID_T> begin();

    @FFINameAlias("end")
    PropertyNbrUnit<VID_T> end();

    @FFINameAlias("Size")
    int size();

    @FFINameAlias("Empty")
    boolean empty();

    default Iterable<PropertyNbrUnit<VID_T>> iterator() {
        return () -> {
            return new Iterator<PropertyNbrUnit<VID_T>>() {
                PropertyNbrUnit<VID_T> cur = begin().moveTo(begin().getAddress());
                long endAddr = end().getAddress();
                long elementSize = cur.elementSize();
                long curAddr = cur.getAddress();

                @Override
                public boolean hasNext() {
                    return curAddr != endAddr;
                }

                @Override
                public PropertyNbrUnit<VID_T> next() {
                    cur.setAddress(curAddr);
                    curAddr += elementSize;
                    return cur;
                }
            };
        };
    }


}
