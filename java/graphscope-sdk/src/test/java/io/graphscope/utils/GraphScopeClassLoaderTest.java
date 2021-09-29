package io.graphscope.utils;

import org.junit.Test;

import java.net.URLClassLoader;

public class GraphScopeClassLoaderTest {
    @Test
    public void test1(){
        String cp = "/a/b/c:d.jar";
        URLClassLoader urlClassLoader = GraphScopeClassLoader.newGraphScopeClassLoader(cp);
        System.out.println(urlClassLoader.getURLs());
    }
}
