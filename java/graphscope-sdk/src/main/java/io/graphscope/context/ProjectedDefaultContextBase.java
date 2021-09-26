package io.v6d.modules.graph.context;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.grape.fragment.ArrowProjectedFragment;
import com.alibaba.grape.parallel.DefaultMessageManager;

//I.e. VertexDataContext in cpp
//vertexPropertyContext
public interface ProjectedDefaultContextBase<FRAG_T extends ArrowProjectedFragment> {
    void init(FRAG_T fragment, DefaultMessageManager messageManager, JSONObject jsonObject);
}
