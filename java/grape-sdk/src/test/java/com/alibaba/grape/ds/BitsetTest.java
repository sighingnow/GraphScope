package com.alibaba.grape.ds;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BitsetTest {

    private Bitset bitset;

    @Before
    public void before() {
        bitset = Bitset.factory.create();
        bitset.init(20);
    }

    @Test
    public void test1() {
        for (int i = 0; i < 10; i += 2) {
            bitset.set_bit((long) i);
        }
        for (int i = 0; i < 10; i += 2) {
            Assert.assertTrue(bitset.get_bit((long) i));
        }
        for (int i = 1; i < 10; i += 2) {
            Assert.assertFalse(bitset.get_bit((long) i));
        }
    }
}
