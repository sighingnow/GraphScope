package com.alibaba.graphscope.utils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphxAdaptorTest {
    private Logger logger = LoggerFactory.getLogger(GraphxAdaptorTest.class.getName());

    @Test
    public void test()
        throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        String genericString = "com.alibaba.graphscope.app.GraphXAdaptor<int64_t,int64_t,int64_t>";
        ClassLoader classLoader = getClass().getClassLoader();
        Class<?> graphxClz = classLoader.loadClass("com.alibaba.graphscope.app.GraphXAdaptor");
        logger.info("Load clz : {}", graphxClz.getName());
        Method method = graphxClz.getDeclaredMethod("create", String.class, String.class,String.class);
        if (method == null){
            throw new IllegalStateException("Fail to found method in graphxAdaptor");
        }
        String []t = genericString.split("<");
        if (t.length != 2){
            throw new IllegalStateException("Not possible legnth: " + t.length);
        }
        String []t2 = t[1].substring(0, t[1].length() - 1).split(",");
        if (t2.length != 3){
            throw new IllegalStateException("Not possible legnth: " + t2.length);
        }
        Object obj = method.invoke(null, t2[0], t2[1], t2[2]);
        logger.info("Successfully invoked method, got" + obj);
    }

    @Test
    public void test1()
        throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        String genericString = "com.alibaba.graphscope.context.GraphXAdaptorContext<int64_t,int64_t,int64_t>";
        ClassLoader classLoader = getClass().getClassLoader();
        Class<?> graphxClz = classLoader.loadClass("com.alibaba.graphscope.context.GraphXAdaptorContext");
        logger.info("Load clz : {}", graphxClz.getName());
        Method method = graphxClz.getDeclaredMethod("create", String.class, String.class,String.class);
        if (method == null){
            throw new IllegalStateException("Fail to found method in graphxAdaptor");
        }
        String []t = genericString.split("<");
        if (t.length != 2){
            throw new IllegalStateException("Not possible legnth: " + t.length);
        }
        String []t2 = t[1].substring(0, t[1].length() - 1).split(",");
        if (t2.length != 3){
            throw new IllegalStateException("Not possible legnth: " + t2.length);
        }
        Object obj = method.invoke(null, t2[0], t2[1], t2[2]);
        logger.info("Successfully invoked method, got" + obj);
    }
}
