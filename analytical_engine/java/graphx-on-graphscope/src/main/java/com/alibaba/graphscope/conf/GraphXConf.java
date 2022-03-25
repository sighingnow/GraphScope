package com.alibaba.graphscope.conf;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.graphscope.app.GraphXAppBase;
import java.net.URLClassLoader;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphXConf {
    private static Logger logger = LoggerFactory.getLogger(GraphXConf.class.getName());

    private static String USER_CLASS = "user_class";
    private Optional<Class<? extends GraphXAppBase>> user_class = Optional.empty();
    private Optional<String> user_args = Optional.empty();

    public Optional<Class<? extends GraphXAppBase>> getUserAppClass() {
        return user_class;
    }

    public void setUserAppClass(Class<? extends GraphXAppBase> clz) {
        if (user_class.isPresent()) {
            throw new IllegalStateException("You can not set user app class twice");
        }
        user_class = Optional.of(clz);
    }

    public static GraphXConf parseFromJson(JSONObject jsonObject, ClassLoader classLoader)
        throws ClassNotFoundException {
        GraphXConf conf = new GraphXConf();
        if (jsonObject.containsKey(USER_CLASS) && !jsonObject.getString(USER_CLASS).isEmpty()) {
            logger.info("Parse user app class {} from json str", jsonObject.getString(USER_CLASS));
            conf.setUserAppClass(
                (Class<? extends GraphXAppBase>) classLoader.loadClass(
                    jsonObject.getString(USER_CLASS)));
        }
        return conf;
    }

}
