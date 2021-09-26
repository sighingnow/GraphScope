package io.v6d.modules.graph.context;


import com.alibaba.fastjson.JSONObject;
import io.v6d.modules.graph.fragment.ArrowFragment;
import io.v6d.modules.graph.parallel.PropertyMessageManager;

/**
 * Different from DefaultContext, this context doesn't require user to define output method.
 *
 * @param <OID_T>
 */
public interface PropertyDefaultContextBase<OID_T> {
    void init(ArrowFragment<OID_T> fragment, PropertyMessageManager messageManager, JSONObject jsonObject);
}
