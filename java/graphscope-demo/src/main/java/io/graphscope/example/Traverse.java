package io.graphscope.example;


import com.alibaba.grape.ds.VertexRange;
import io.v6d.modules.graph.app.PropertyDefaultAppBase;
import io.v6d.modules.graph.context.PropertyDefaultContextBase;
import io.v6d.modules.graph.fragment.ArrowFragment;
import io.v6d.modules.graph.parallel.PropertyMessageManager;

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

