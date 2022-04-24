package com.alibaba.graphscope.ds;

import static com.alibaba.graphscope.utils.CppClassName.GS_MEMORY_MAPPED_BUFFER;
import static com.alibaba.graphscope.utils.CppHeaderName.GS_MEMORY_MAPPED_BUFFER_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.FFIByteString;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.FFITypeFactory;

@FFIGen
@CXXHead(value = {GS_MEMORY_MAPPED_BUFFER_H})
@FFITypeAlias(GS_MEMORY_MAPPED_BUFFER)
public interface MemoryMappedBuffer extends FFIPointer {
    Factory factory = FFITypeFactory.getFactory(Factory.class, MemoryMappedBuffer.class);

    @FFINameAlias("Map")
    void map();

    @FFINameAlias("Unmap")
    void unMap();

    @FFINameAlias("GetAddr")
    long getAddr();

    @FFIFactory
    interface Factory {

        MemoryMappedBuffer create(@CXXValue FFIByteString string, long mappedSize);
    }
}
