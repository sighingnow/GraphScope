package io.graphscope.ds;

import com.alibaba.fastffi.*;
import io.graphscope.utils.CppClassName;
import io.graphscope.utils.CppHeaderName;
import io.graphscope.utils.JNILibraryName;

import java.util.Iterator;

@FFIGen(library = JNILibraryName.VINEYARD_JNI_LIBRARY)
@CXXHead(CppHeaderName.CORE_JAVA_TYPE_ALIAS_H)
@FFITypeAlias(CppClassName.PROPERTY_RAW_ADJ_LIST)
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
