package io.graphscope.context;


import com.alibaba.fastjson.JSONObject;
import io.graphscope.fragment.ArrowFragment;
import io.graphscope.parallel.PropertyMessageManager;

/**
 * Different from DefaultContext, this context doesn't require user to define output method.
 *
 * @param <OID_T>
 */
public interface PropertyDefaultContextBase<OID_T> {
    void init(ArrowFragment<OID_T> fragment, PropertyMessageManager messageManager, JSONObject jsonObject);
}
