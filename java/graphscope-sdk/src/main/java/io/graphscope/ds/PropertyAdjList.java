package io.graphscope.ds;

import com.alibaba.fastffi.*;
import io.graphscope.utils.CppClassName;
import io.graphscope.utils.CppHeaderName;
import io.graphscope.utils.JNILibraryName;

import java.util.Iterator;

@FFIGen(library = JNILibraryName.VINEYARD_JNI_LIBRARY)
@CXXHead(CppHeaderName.PROPERTY_GRAPH_UTILS_H)
@FFITypeAlias(CppClassName.PROPERTY_ADJ_LIST)
@CXXTemplate(cxx = {"uint64_t"}, java = {"java.lang.Long"})
public interface PropertyAdjList<VID_T>
        extends FFIPointer, CXXPointer {
    @FFINameAlias("begin")
    @CXXValue PropertyNbr<VID_T> begin();

    @FFINameAlias("end")
    @CXXValue PropertyNbr<VID_T> end();

    @FFINameAlias("begin_unit")
    PropertyNbrUnit<VID_T> beginUnit();

    @FFINameAlias("end_unit")
    PropertyNbrUnit<VID_T> endUnit();

    @FFINameAlias("Size")
    int size();

    @FFINameAlias("Empty")
    boolean empty();

    default Iterable<PropertyNbr<VID_T>> iterator() {
        return () -> new Iterator<PropertyNbr<VID_T>>() {
            PropertyNbr<VID_T> cur = begin().dec();
            PropertyNbr<VID_T> end = end();
            boolean flag = false;

            @Override
            public boolean hasNext() {
                if (!flag) {
                    cur = cur.inc();
                    flag = !cur.eq(end);
                }
                return flag;
            }

            @Override
            public PropertyNbr<VID_T> next() {
                flag = false;
                return cur;
            }
        };
    }
}

