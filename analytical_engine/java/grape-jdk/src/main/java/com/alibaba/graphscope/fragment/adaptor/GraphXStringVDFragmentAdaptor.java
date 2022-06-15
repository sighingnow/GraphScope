package com.alibaba.graphscope.fragment.adaptor;

import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.FragmentType;
import com.alibaba.graphscope.fragment.GraphXStringVDFragment;

public class GraphXStringVDFragmentAdaptor<OID_T,VID_T,VD_T,ED_T> extends AbstractGraphXFragmentAdaptor<OID_T,VID_T,VD_T,ED_T> {

    private GraphXStringVDFragment<OID_T,VID_T,VD_T,ED_T> graphXStringVDFragment;
    public GraphXStringVDFragmentAdaptor(GraphXStringVDFragment<OID_T,VID_T,VD_T,ED_T> graphXStringVDFragment){
        super(graphXStringVDFragment);
        this.graphXStringVDFragment = graphXStringVDFragment;
    }

    /**
     * Return the underlying fragment type,i.e. ArrowProjected or Simple.
     *
     * @return underlying fragment type.
     */
    @Override
    public FragmentType fragmentType() {
        return FragmentType.GraphXStringVDFragment;
    }

    @Override
    public long getInEdgeNum() {
        return graphXStringVDFragment.getInEdgeNum();
    }

    @Override
    public long getOutEdgeNum() {
        return graphXStringVDFragment.getOutEdgeNum();
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
