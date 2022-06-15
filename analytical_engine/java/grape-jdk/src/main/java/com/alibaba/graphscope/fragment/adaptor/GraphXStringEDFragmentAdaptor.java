package com.alibaba.graphscope.fragment.adaptor;

import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.FragmentType;
import com.alibaba.graphscope.fragment.GraphXStringEDFragment;
import com.alibaba.graphscope.fragment.GraphXStringVDFragment;

public class GraphXStringEDFragmentAdaptor<OID_T,VID_T,VD_T,ED_T> extends AbstractGraphXFragmentAdaptor<OID_T,VID_T,VD_T,ED_T> {

    private GraphXStringEDFragment<OID_T,VID_T,VD_T,ED_T> graphXStringEDFragment;
    public GraphXStringEDFragmentAdaptor(GraphXStringEDFragment<OID_T,VID_T,VD_T,ED_T> graphXStringEDFragment){
        super(graphXStringEDFragment);
        this.graphXStringEDFragment = graphXStringEDFragment;
    }


    public GraphXStringEDFragment<OID_T,VID_T,VD_T,ED_T> getFragment(){
        return graphXStringEDFragment;
    }
    /**
     * Return the underlying fragment type,i.e. ArrowProjected or Simple.
     *
     * @return underlying fragment type.
     */
    @Override
    public FragmentType fragmentType() {
        return FragmentType.GraphXStringEDFragment;
    }

    @Override
    public long getInEdgeNum() {
        return graphXStringEDFragment.getInEdgeNum();
    }

    @Override
    public long getOutEdgeNum() {
        return graphXStringEDFragment.getOutEdgeNum();
    }

    /**
     * Get the data on vertex.
     *
     * @param vertex querying vertex.
     * @return vertex data
     */
    @Override
    public VD_T getData(Vertex<VID_T> vertex) {
        return graphXStringEDFragment.getData(vertex);
    }
}
