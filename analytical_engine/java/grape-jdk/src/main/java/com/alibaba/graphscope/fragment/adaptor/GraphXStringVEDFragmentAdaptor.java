package com.alibaba.graphscope.fragment.adaptor;

import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.FragmentType;
import com.alibaba.graphscope.fragment.GraphXStringVEDFragment;

public class GraphXStringVEDFragmentAdaptor<OID_T, VID_T, VD_T, ED_T> extends
    AbstractGraphXFragmentAdaptor<OID_T, VID_T, VD_T, ED_T> {

    private GraphXStringVEDFragment<OID_T, VID_T, VD_T, ED_T> graphXStringVEDFragment;

    public GraphXStringVEDFragmentAdaptor(
        GraphXStringVEDFragment<OID_T, VID_T, VD_T, ED_T> graphXStringVEDFragment) {
        super(graphXStringVEDFragment);
        this.graphXStringVEDFragment = graphXStringVEDFragment;
    }

    public GraphXStringVEDFragment<OID_T, VID_T, VD_T, ED_T> getFragment() {
        return graphXStringVEDFragment;
    }

    /**
     * Return the underlying fragment type,i.e. ArrowProjected or Simple.
     *
     * @return underlying fragment type.
     */
    @Override
    public FragmentType fragmentType() {
        return FragmentType.GraphXStringVEDFragment;
    }

    @Override
    public long getInEdgeNum() {
        return graphXStringVEDFragment.getInEdgeNum();
    }

    @Override
    public long getOutEdgeNum() {
        return graphXStringVEDFragment.getOutEdgeNum();
    }

    /**
     * Get the data on vertex.
     *
     * @param vertex querying vertex.
     * @return vertex data
     */
    @Override
    public VD_T getData(Vertex<VID_T> vertex) {
        throw new IllegalStateException("Not implemented");
    }
}