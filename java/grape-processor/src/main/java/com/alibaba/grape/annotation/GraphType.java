package com.alibaba.grape.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Retention(value = RUNTIME)
@Target(ElementType.TYPE)
public @interface GraphType {
    Class<?> oidType();

    Class<?> vidType();

    Class<?> edataType();

    Class<?> vdataType();

    String cppOidType() default "";

    String cppVidType() default "";

    String cppEdataType() default "";

    String cppVdataType() default "";

    String fragType() default "";
}
