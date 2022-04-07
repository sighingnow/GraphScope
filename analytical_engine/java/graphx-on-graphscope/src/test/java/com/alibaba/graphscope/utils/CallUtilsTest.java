package com.alibaba.graphscope.utils;


import org.junit.Test;

public class CallUtilsTest {
    public static class A{
        public static void a(){
            System.out.println(CallUtils.getCallerCallerClassName());
        }
    }
    public static class B{
        public static void b(){
            A.a();
        }
    }
    public static class C{
        public static void c(){
            B.b();
        }
    }

    @Test
    public void test(){
        C.c();
    }
}
