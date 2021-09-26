package com.alibaba.grape.sample;

import org.junit.Before;
import org.junit.Test;

public class CompareUnsignedTest {
    private long [] data;
    @Before
    public void prepare(){
        data = new long [1000000];
        for (int i = 0; i < data.length; ++i){
            if (i % 2 == 0) data[i] = - (20000000 + i);
            else data[i] = (20000000 + i);
        }
    }
    @Test
    public void test1(){
        double time0 = System.nanoTime();
        int i = 0, j = data.length - 1;
        boolean result = false;
        while (i < j){
            result |= Long.compareUnsigned(data[i++], data[j--]) > 0;
        }
        System.out.println("compare unsigned" + (System.nanoTime() - time0) / 10e9);
        System.out.println(result);
    }
    @Test
    public void test2(){
        double time0 = System.nanoTime();
        int i = 0, j = data.length - 1;
        boolean result = false;
        while (i < j){
            result |= data[i++] > data[j--];
        }
        System.out.println("comparison" + (System.nanoTime() - time0) / 10e9);
        System.out.println(result);
    }
}
