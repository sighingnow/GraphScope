package com.alibaba.grape.parallel.message;

import com.alibaba.ffi.*;

import static com.alibaba.grape.utils.CPP_CLASSES_STRINGS.LONG_MSG;
import static com.alibaba.grape.utils.CPP_HEADER_STRINGS.CORE_JAVA_JAVA_MESSAGES_H;
import static com.alibaba.grape.utils.CPP_LIBRARY_STRINGS.GRAPE_JNI_LIBRARY;

// @FFIGen(library = "grape-lite")
// @FFIMirror
// @FFINameSpace("grape")
// @FFITypeAlias("long_msg")
// public interface LongMsg extends MsgBase, CXXPointer {
//   static LongMsg create() {
//     return factory.create();
//   }

//   Factory factory = FFITypeFactory.getFactory(LongMsg.class);
//   @FFIFactory
//   interface Factory {
//     LongMsg create();
//   }

//   @FFIGetter long data();
//   @FFISetter void data(long value);
// }
@FFIGen(library = GRAPE_JNI_LIBRARY)
@CXXHead(CORE_JAVA_JAVA_MESSAGES_H)
@FFITypeAlias(LONG_MSG)
public interface LongMsg extends FFIPointer {
    Factory factory = FFITypeFactory.getFactory(Factory.class, LongMsg.class);

    @FFIFactory
    interface Factory {
        LongMsg create();

        LongMsg create(long inData);
    }

    void setData(long value);

    long getData();
}