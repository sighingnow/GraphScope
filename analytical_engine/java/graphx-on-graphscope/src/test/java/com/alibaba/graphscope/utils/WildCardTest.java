package com.alibaba.graphscope.utils;

import org.junit.Test;

public class WildCardTest {

    public static class Conf<A, B, C> {

        private A a;

        public Conf() {

        }

        public void setA(A a) {
            this.a = a;
        }

        public A getA() {
            return a;
        }
    }

    public static <A, B, C> Conf<A, B, C> create(Class<? super A> clz, Class<? super B> clz2,
        Class<? super C> clz3) {
        return new Conf<A, B, C>();
    }

    @Test
    public void test1() {
        Conf<Long, Double, Integer> conf = create(Long.class, Double.class, Integer.class);
    }
}
