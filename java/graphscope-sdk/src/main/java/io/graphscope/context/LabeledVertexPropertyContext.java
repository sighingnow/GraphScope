package io.graphscope.context;

import com.alibaba.ffi.FFIByteString;
import com.alibaba.ffi.FFITypeFactory;
import com.alibaba.grape.utils.FFITypeFactoryhelper;
import io.graphscope.column.DoubleColumn;
import io.graphscope.column.IntColumn;
import io.graphscope.column.LongColumn;
import io.graphscope.context.ffi.FFILabeledVertexPropertyContext;
import io.graphscope.utils.CppClassName;
import io.graphscope.fragment.ArrowFragment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public abstract class LabeledVertexPropertyContext<OID_T> implements PropertyDefaultContextBase<OID_T> {
    private long ffiContextAddress;
    private FFILabeledVertexPropertyContext<ArrowFragment<OID_T>> ffiLabeledVertexPropertyContext;
    private FFILabeledVertexPropertyContext.Factory factory;
    private static Logger logger = LoggerFactory.getLogger(LabeledVertexPropertyContext.class.getName());

    /**
     * Must be called by jni, to create ffi context.
     *
     * @param fragment
     */
    protected void createFFIContext(ArrowFragment<OID_T> fragment) {
        String fragmentTemplateStr = FFITypeFactoryhelper.getForeignName(fragment);
        String contextName = FFITypeFactoryhelper.makeParameterize(CppClassName.LABELED_VERTEX_PROPERTY_CONTEXT, fragmentTemplateStr);
        System.out.println("context name: " + contextName);
        factory = FFITypeFactory.getFactory(FFILabeledVertexPropertyContext.class, contextName);
        ffiLabeledVertexPropertyContext = factory.create(fragment);
        ffiContextAddress = ffiLabeledVertexPropertyContext.getAddress();
        System.out.println("create vertex property Context: " + contextName + "@" + ffiContextAddress);
    }

    public long addColumn(int labelId, String str, ContextDataType contextDataType) {
        if (Objects.nonNull(ffiLabeledVertexPropertyContext)) {
            FFIByteString byteString = FFITypeFactory.newByteString();
            byteString.copyFrom(str);
            return ffiLabeledVertexPropertyContext.addColumn(labelId, byteString, contextDataType);
        }
        logger.error("ffi vertex context empty ");
        return -1;
    }

    public DoubleColumn<ArrowFragment<OID_T>> getDoubleColumn(int labelId, long index) {
        if (Objects.nonNull(ffiLabeledVertexPropertyContext)) {
            return ffiLabeledVertexPropertyContext.getDoubleColumn(labelId, index).get();
        }
        logger.error("ffi vertex context empty ");
        return null;
    }

    public IntColumn<ArrowFragment<OID_T>> getIntColumn(int labelId, long index) {
        if (Objects.nonNull(ffiLabeledVertexPropertyContext)) {
            return ffiLabeledVertexPropertyContext.getIntColumn(labelId, index).get();
        }
        logger.error("ffi vertex context empty ");
        return null;
    }

    public LongColumn<ArrowFragment<OID_T>> getLongColumn(int labelId, long index) {
        if (Objects.nonNull(ffiLabeledVertexPropertyContext)) {
            return ffiLabeledVertexPropertyContext.getLongColumn(labelId, index).get();
        }
        logger.error("ffi vertex context empty ");
        return null;
    }

    public DoubleColumn<ArrowFragment<OID_T>> getDoubleColumn(int labelId, String name) {
        if (Objects.nonNull(ffiLabeledVertexPropertyContext)) {
            FFIByteString byteString = FFITypeFactory.newByteString();
            byteString.copyFrom(name);
            return ffiLabeledVertexPropertyContext.getDoubleColumn(labelId, byteString).get();
        }
        logger.error("ffi vertex context empty ");
        return null;
    }

    public LongColumn<ArrowFragment<OID_T>> getLongColumn(int labelId, String name) {
        if (Objects.nonNull(ffiLabeledVertexPropertyContext)) {
            FFIByteString byteString = FFITypeFactory.newByteString();
            byteString.copyFrom(name);
            return ffiLabeledVertexPropertyContext.getLongColumn(labelId, byteString).get();
        }
        logger.error("ffi vertex context empty ");
        return null;
    }

    public IntColumn<ArrowFragment<OID_T>> getIntColumn(int labelId, String name) {
        if (Objects.nonNull(ffiLabeledVertexPropertyContext)) {
            FFIByteString byteString = FFITypeFactory.newByteString();
            byteString.copyFrom(name);
            return ffiLabeledVertexPropertyContext.getIntColumn(labelId, byteString).get();
        }
        logger.error("ffi vertex context empty ");
        return null;
    }
}
