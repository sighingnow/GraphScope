package io.v6d.modules.graph.app;

import io.v6d.modules.graph.context.PropertyDefaultContextBase;
import io.v6d.modules.graph.fragment.ArrowFragment;
import io.v6d.modules.graph.parallel.PropertyMessageManager;

@SuppressWarnings("rawtypes")
public interface PropertyDefaultAppBase<OID_T,
        C extends PropertyDefaultContextBase<OID_T>> {
    void PEval(ArrowFragment<OID_T> fragment, PropertyDefaultContextBase<OID_T> context,
               PropertyMessageManager messageManager);

    void IncEval(ArrowFragment<OID_T> graph, PropertyDefaultContextBase<OID_T> context,
                 PropertyMessageManager messageManager);
}