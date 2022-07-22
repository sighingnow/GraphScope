package com.alibaba.graphscope.utils;

@FunctionalInterface
public interface TriConsumerV2<T, U, V> {
    public void accept(T t, U u, V v) throws InterruptedException;
}
