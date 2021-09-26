package io.graphscope.column;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class ParamGetter {
    private JSONObject jsonObject;

    public ParamGetter() {

    }

    public void init(String str) {
        jsonObject = JSON.parseObject(str);
    }

    public String get(String str) {
        return jsonObject.getString(str);
    }

    public String getOrDefault(String str, String defaultValue) {
        String res = jsonObject.getString(str);
        if (res == null) return defaultValue;
        return res;
    }

    public Long getLong(String str) {
        return jsonObject.getLong(str);
    }

    public Double getDouble(String str) {
        return jsonObject.getDouble(str);
    }

    public Integer getInteger(String str) {
        return jsonObject.getInteger(str);
    }
}
