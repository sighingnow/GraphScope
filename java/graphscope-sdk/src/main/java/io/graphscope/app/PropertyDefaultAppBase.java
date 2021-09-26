package io.graphscope.app;

import io.graphscope.context.PropertyDefaultContextBase;
import io.graphscope.fragment.ArrowFragment;
import io.graphscope.parallel.PropertyMessageManager;

@SuppressWarnings("rawtypes")
public interface PropertyDefaultAppBase<OID_T,
        C extends PropertyDefaultContextBase<OID_T>> {
    void PEval(ArrowFragment<OID_T> fragment, PropertyDefaultContextBase<OID_T> context,
               PropertyMessageManager messageManager);

    void IncEval(ArrowFragment<OID_T> graph, PropertyDefaultContextBase<OID_T> context,
                 PropertyMessageManager messageManager);
}