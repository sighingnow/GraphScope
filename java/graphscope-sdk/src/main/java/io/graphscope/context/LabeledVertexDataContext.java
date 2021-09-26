package io.v6d.modules.graph.context;

import com.alibaba.ffi.FFITypeFactory;
import com.alibaba.grape.ds.GSVertexArray;
import com.alibaba.grape.ds.Vertex;
import com.alibaba.grape.stdcxx.StdVector;
import com.alibaba.grape.utils.FFITypeFactoryhelper;
import io.v6d.modules.graph.context.ffi.FFILabeledVertexDataContext;
import io.v6d.modules.graph.fragment.ArrowFragment;

import java.util.Objects;

import static io.v6d.modules.graph.utils.CPP_CLASS.LABELED_VERTEX_DATA_CONTEXT;

public abstract class LabeledVertexDataContext<OID_T, DATA_T> implements PropertyDefaultContextBase<OID_T> {
    private long ffiContextAddress;
    private FFILabeledVertexDataContext<ArrowFragment<OID_T>, DATA_T> ffiLabeledVertexDataContext;
    private FFILabeledVertexDataContext.Factory factory;

    /**
     * Must be called by jni, to create ffi context.
     *
     * @param fragment
     * @param dataClass
     */
    protected void createFFIContext(ArrowFragment<OID_T> fragment, Class<?> oidClass, Class<?> dataClass) {
//        System.out.println("fragment: " + FFITypeFactoryhelper.makeParameterize(ARROW_FRAGMENT, FFITypeFactoryhelper.javaType2CppType(oidClass)));
        String fragmentTemplateStr = FFITypeFactoryhelper.getForeignName(fragment.getClass());
        String contextName = FFITypeFactoryhelper.makeParameterize(LABELED_VERTEX_DATA_CONTEXT, fragmentTemplateStr, FFITypeFactoryhelper.javaType2CppType(dataClass));
        System.out.println("context name: " + contextName);
        factory = FFITypeFactory.getFactory(FFILabeledVertexDataContext.class, contextName);
        ffiLabeledVertexDataContext = factory.create(fragment, true);
        ffiContextAddress = ffiLabeledVertexDataContext.getAddress();
        System.out.println(contextName + ", " + ffiContextAddress);
    }

    public DATA_T getValue(Vertex<Long> vertex) {
        if (Objects.isNull(ffiLabeledVertexDataContext)) {
            return null;
        }
        return ffiLabeledVertexDataContext.getValue(vertex);
    }

    public StdVector<GSVertexArray<DATA_T>> data() {
        if (Objects.isNull(ffiLabeledVertexDataContext)) {
            return null;
        }
        return ffiLabeledVertexDataContext.data();
    }
}
