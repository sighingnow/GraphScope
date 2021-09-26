package io.graphscope.context;


import com.alibaba.ffi.CXXEnum;
import com.alibaba.ffi.FFITypeAlias;
import com.alibaba.ffi.FFITypeRefiner;
import io.graphscope.utils.CPP_CLASS;

//@FFIGen(library = VINEYARD_JNI_LIBRARY)
//@CXXHead(CONTEXT_PROTOCOLS_H)
@FFITypeAlias(CPP_CLASS.CONTEXT_DATA_TYPE)
@FFITypeRefiner("io.graphscope.context.ContextDataType.get")
public enum ContextDataType implements CXXEnum {
    kBool,
    kInt32,
    kInt64,
    kUInt32,
    kUInt64,
    kFloat,
    kDouble,
    kString,
    kUndefined;

    @Override
    public int getValue() {
        return ordinal();
    }

    public static ContextDataType get(int value) {
        switch (value) {
            case 0:
                return kBool;
            case 1:
                return kInt32;
            case 2:
                return kInt64;
            case 3:
                return kUInt32;
            case 4:
                return kUInt64;
            case 5:
                return kFloat;
            case 6:
                return kDouble;
            case 7:
                return kString;
            case 8:
                return kUndefined;
            default:
                throw new IllegalStateException("Unknow value for Context data type: " + value);
        }
    }
}
