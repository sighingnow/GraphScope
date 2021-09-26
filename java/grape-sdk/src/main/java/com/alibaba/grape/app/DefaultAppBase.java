package com.alibaba.grape.app;

import com.alibaba.grape.fragment.ImmutableEdgecutFragment;
import com.alibaba.grape.parallel.DefaultMessageManager;

/**
 * The inerface for all PIE apps, also providing some useful functions.
 *
 * @param <OID_T>    original id type
 * @param <VID_T>    vertex id type
 * @param <VDATA_T>> vertex data type
 * @param <EDATA_T>> edge data type
 * @param <C>        context type
 */
@SuppressWarnings("rawtypes")
public interface DefaultAppBase<OID_T, VID_T, VDATA_T, EDATA_T,
        C extends DefaultContextBase<OID_T, VID_T, VDATA_T, EDATA_T>>
        extends AppBase<OID_T, VID_T, VDATA_T, EDATA_T, C> {
    void PEval(ImmutableEdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T> graph, DefaultContextBase context,
               DefaultMessageManager messageManager);

    void IncEval(ImmutableEdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T> graph, DefaultContextBase context,
                 DefaultMessageManager messageManager);
}
