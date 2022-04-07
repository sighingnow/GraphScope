package com.alibaba.graphscope.utils;


import org.junit.Test;

public class CallUtilsTest {

    public void test(){
        String val = CallUtils.getCallerCallerClassName();
        System.out.println(val);
    }
}
