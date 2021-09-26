package io.v6d.modules.graph.context;

import com.alibaba.ffi.FFIByteString;
import com.alibaba.ffi.FFITypeFactory;
import com.alibaba.grape.fragment.ArrowProjectedFragment;
import com.alibaba.grape.utils.FFITypeFactoryhelper;
import io.v6d.modules.graph.column.DoubleColumn;
import io.v6d.modules.graph.column.IntColumn;
import io.v6d.modules.graph.column.LongColumn;
import io.v6d.modules.graph.context.ffi.FFIVertexPropertyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import static io.v6d.modules.graph.utils.CPP_CLASS.VERTEX_PROPERTY_CONTEXT;

/**
 * VertexPropertyContext only compatible with simple graph, i.e. ArrowProjectedFragment
 *
 * @FRAG_T
 */
public abstract class VertexPropertyContext<FRAG_T extends ArrowProjectedFragment> implements ProjectedDefaultContextBase<FRAG_T> {
    private long ffiContextAddress;
    private FFIVertexPropertyContext<FRAG_T> ffiVertexPropertyContext;
    private FFIVertexPropertyContext.Factory factory;
    private static Logger logger = LoggerFactory.getLogger(VertexPropertyContext.class.getName());

    /**
     * Must be called by jni, to create ffi context.
     *
     * @param fragment
     */
    protected void createFFIContext(FRAG_T fragment) {
        String fragmentTemplateStr = FFITypeFactoryhelper.getForeignName(fragment.getClass());
        System.out.println("fragment str: " + fragmentTemplateStr);
        String contextName = FFITypeFactoryhelper.makeParameterize(VERTEX_PROPERTY_CONTEXT, fragmentTemplateStr);
        System.out.println("context name: " + contextName);
        factory = FFITypeFactory.getFactory(FFIVertexPropertyContext.class, contextName);
        ffiVertexPropertyContext = factory.create(fragment);
        ffiContextAddress = ffiVertexPropertyContext.getAddress();
        System.out.println("create vertex property Context: " + contextName + "@" + ffiContextAddress);
    }

    public long addColumn(String str, ContextDataType contextDataType) {
        if (Objects.nonNull(ffiVertexPropertyContext)) {
            FFIByteString byteString = FFITypeFactory.newByteString();
            byteString.copyFrom(str);
            return ffiVertexPropertyContext.addColumn(byteString, contextDataType);
        }
        logger.error("ffi vertex context empty ");
        return -1;
    }

    public DoubleColumn<FRAG_T> getDoubleColumn(long index) {
        if (Objects.nonNull(ffiVertexPropertyContext)) {
            return ffiVertexPropertyContext.getDoubleColumn(index).get();
        }
        logger.error("ffi vertex context empty ");
        return null;
    }

    public IntColumn<FRAG_T> getIntColumn(long index) {
        if (Objects.nonNull(ffiVertexPropertyContext)) {
            return ffiVertexPropertyContext.getIntColumn(index).get();
        }
        logger.error("ffi vertex context empty ");
        return null;
    }

    public LongColumn<FRAG_T> getLongColumn(long index) {
        if (Objects.nonNull(ffiVertexPropertyContext)) {
            return ffiVertexPropertyContext.getLongColumn(index).get();
        }
        logger.error("ffi vertex context empty ");
        return null;
    }

    public DoubleColumn<FRAG_T> getDoubleColumn(String name) {
        if (Objects.nonNull(ffiVertexPropertyContext)) {
            FFIByteString byteString = FFITypeFactory.newByteString();
            byteString.copyFrom(name);
            return ffiVertexPropertyContext.getDoubleColumn(byteString).get();
        }
        logger.error("ffi vertex context empty ");
        return null;
    }

    public LongColumn<FRAG_T> getLongColumn(String name) {
        if (Objects.nonNull(ffiVertexPropertyContext)) {
            FFIByteString byteString = FFITypeFactory.newByteString();
            byteString.copyFrom(name);
            return ffiVertexPropertyContext.getLongColumn(byteString).get();
        }
        logger.error("ffi vertex context empty ");
        return null;
    }

    public IntColumn<FRAG_T> getIntColumn(String name) {
        if (Objects.nonNull(ffiVertexPropertyContext)) {
            FFIByteString byteString = FFITypeFactory.newByteString();
            byteString.copyFrom(name);
            return ffiVertexPropertyContext.getIntColumn(byteString).get();
        }
        logger.error("ffi vertex context empty ");
        return null;
    }

}
