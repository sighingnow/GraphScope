package io.graphscope.example;


import com.alibaba.grape.ds.VertexRange;
import io.graphscope.app.PropertyDefaultAppBase;
import io.graphscope.context.PropertyDefaultContextBase;
import io.graphscope.fragment.ArrowFragment;
import io.graphscope.parallel.PropertyMessageManager;

/**
 * @author xiaolei.zl
 * @date 2021/06/14
 */
public class Traverse implements PropertyDefaultAppBase<Long, TraverseDefaultContextBase> {
    @Override
    public void PEval(ArrowFragment<Long> fragment,
                      PropertyDefaultContextBase<Long> context, PropertyMessageManager messageManager) {
        TraverseDefaultContextBase ctx = (TraverseDefaultContextBase) context;
        VertexRange<Long> innerVertices = fragment.innerVertices(0);
    }

    @Override
    public void IncEval(ArrowFragment<Long> fragment,
                        PropertyDefaultContextBase<Long> context, PropertyMessageManager messageManager) {
        TraverseDefaultContextBase ctx = (TraverseDefaultContextBase) context;
        if (ctx.step >= ctx.maxStep) {
            return;
        }
        messageManager.ForceContinue();
    }
}

