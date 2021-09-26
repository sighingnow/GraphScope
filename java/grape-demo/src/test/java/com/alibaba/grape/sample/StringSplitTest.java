package com.alibaba.grape.sample;

import com.google.common.base.Splitter;
import org.junit.Test;

import java.util.List;

public class StringSplitTest {
    @Test
    public void test1(){
        String str = "0   1     3";
        long res = 0;
        long span = -System.nanoTime();
        for (int i = 0; i < 10000; ++i){
            String[] fileds = str.split("\\s+");
            res += fileds.length;
        }
        span += System.nanoTime();
        System.out.println("string split " + span/1000000000 + " cnt " + res);
    }
    @Test
    public void test2(){
        String str = "0   1     3";
        long res = 0;
        long span = -System.nanoTime();
        for (int i = 0; i < 10000; ++i){
            List<String> fileds = Splitter.on(' ').omitEmptyStrings().splitToList(str);
            res += fileds.size();
        }
        span += System.nanoTime();
        System.out.println("spliter split " + span/1000000000 + " cnt " + res);
    }
}
