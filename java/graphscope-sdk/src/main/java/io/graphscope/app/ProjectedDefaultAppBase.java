package io.graphscope.app;

import com.alibaba.grape.fragment.ArrowProjectedFragment;
import com.alibaba.grape.parallel.DefaultMessageManager;
import io.graphscope.context.ProjectedDefaultContextBase;

public interface ProjectedDefaultAppBase<OID_T, VID_T, VDATA_T, EDATA_T,
        C extends ProjectedDefaultContextBase<ArrowProjectedFragment<OID_T, VID_T, VDATA_T, EDATA_T>>> {
    void PEval(ArrowProjectedFragment<OID_T, VID_T, VDATA_T, EDATA_T> fragment, ProjectedDefaultContextBase<ArrowProjectedFragment<OID_T, VID_T, VDATA_T, EDATA_T>> context,
               DefaultMessageManager messageManager);

    void IncEval(ArrowProjectedFragment<OID_T, VID_T, VDATA_T, EDATA_T> graph, ProjectedDefaultContextBase<ArrowProjectedFragment<OID_T, VID_T, VDATA_T, EDATA_T>> context,
                 DefaultMessageManager messageManager);
}
