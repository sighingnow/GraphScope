package com.alibaba.grape.functional;

import java.util.Objects;


@FunctionalInterface
public interface TriConsumer<A, B, C> {
    void accept(A a, B b, C c);

    default TriConsumer<A, B, C> andThen(
            TriConsumer<? super A, ? super B, ? super C> after) {
        Objects.requireNonNull(after);
        return (aa, bb, cc) -> {
            accept(aa, bb, cc);
            after.accept(aa, bb, cc);
        };
    }
}