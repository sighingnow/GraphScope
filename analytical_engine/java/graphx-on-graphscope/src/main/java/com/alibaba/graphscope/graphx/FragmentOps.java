package com.alibaba.graphscope.graphx;

import com.alibaba.graphscope.fragment.ArrowProjectedFragment;
import org.apache.spark.graphx.impl.GrapeGraphImpl;

/**
 * Defines static method which enabling convertion between graphx.GrapeGraph and fragment.
 */
public class FragmentOps {

    public static <OID, VID, GS_VD, GS_ED, GX_VD, GX_ED> ArrowProjectedFragment<OID, VID, GS_VD, GS_ED> graph2Fragment(
        GrapeGraphImpl<GX_VD, GX_ED> grapeGraph) {

        return null;
    }

    public static <OID, VID, GS_VD, GS_ED, GX_VD, GX_ED> GrapeGraphImpl<GX_VD, GX_ED> fragment2Graph(
        ArrowProjectedFragment<OID, VID, GS_VD, GS_ED> fragment) {
        return null;
    }
}
