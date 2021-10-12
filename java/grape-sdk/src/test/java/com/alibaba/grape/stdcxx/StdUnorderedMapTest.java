package com.alibaba.grape.stdcxx;

import com.alibaba.fastffi.FFITypeFactory;
import com.alibaba.grape.utils.CppClassName;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class StdUnorderedMapTest {

    private StdUnorderedMap.Factory<Integer, Long> factory = FFITypeFactory.getFactory(StdUnorderedMap.class,
            CppClassName.STD_UNORDERED_MAP + "<unsigned,uint64_t>");
    private StdUnorderedMap<Integer, Long> map;

    @Before
    public void init() {
        map = factory.create();
    }

    @Test
    public void test1() {
        for (int i = 0; i < 10; ++i) {
            map.set(i, (long) i);
        }
        for (int i = 0; i < 10; ++i) {
            Assert.assertTrue("Different at " + i, i == map.get(i).intValue());
        }
    }
}
