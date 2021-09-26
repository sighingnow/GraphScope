package io.graphscope.utils;

import io.graphscope.context.*;

public class ContextUtils {
    public static String getPropertyCtxObjBaseClzName(PropertyDefaultContextBase ctxObj) {
        if (ctxObj instanceof LabeledVertexDataContext) {
            return "LabeledVertexDataContext";
        } else if (ctxObj instanceof LabeledVertexPropertyContext) {
            return "LabeledVertexPropertyContext";
        }
        return null;
    }

    public static String getProjectedCtxObjBaseClzName(ProjectedDefaultContextBase ctxObj) {
        if (ctxObj instanceof VertexDataContext) {
            return "VertexDataContext";
        } else if (ctxObj instanceof VertexPropertyContext) {
            return "VertexPropertyContext";
        }
        return null;
    }
}
