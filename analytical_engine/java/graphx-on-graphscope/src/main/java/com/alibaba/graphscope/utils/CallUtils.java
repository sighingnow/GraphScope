package com.alibaba.graphscope.utils;

import java.util.Arrays;
import java.util.stream.Collectors;
import org.checkerframework.checker.units.qual.C;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CallUtils {
    private static Logger logger = LoggerFactory.getLogger(CallUtils.class.getName());

    /**
     * Get the caller class name of the class which called us.
     * TODO: make this generic. now current working for UserClass -> GraphOps.Pregel -> PregeClass
     * @return class name
     */
    public static String getCallerCallerClassName() {
        StackTraceElement[] stElements = Thread.currentThread().getStackTrace();
        String callerClassName = null;
        int index = 0;
        while (index < stElements.length){
            if (stElements[index].getClassName().equals(CallUtils.class.getName())){
                break;
            }
            index += 1;
        }
        logger.info("Find CallUtils at {},total len {}", index, stElements.length);
        while (index + 1 < stElements.length && stElements[index].getClassName().equals(stElements[index+1].getClassName())){
            index += 1;
        }
        index += 1;
        if (index >= stElements.length){
            //we are already at last pos, so we must failed.
            throw new IllegalStateException("already reach last " +index + " " + stElements.length + ", " +Arrays.stream(stElements).map(StackTraceElement::getClassName).collect(
                Collectors.joining("\n")));
        }
        //now we are at the first position of class calling CallUtils.
        logger.info("The class called CallUtils is " + stElements[index].getClassName());
        int callers = 0;
        while (index < stElements.length){
            if (!stElements[index].getClassName().equals(stElements[index - 1].getClassName())){
                logger.info("new caller find: " + stElements[index + 1]);
                callers += 1;
            }
            if (callers == 3){
                return stElements[index].getClassName();
            }
            index += 1;
        }
        throw new IllegalStateException("failed" + Arrays.stream(stElements).map(StackTraceElement::getClassName).collect(
            Collectors.joining("\n")));
    }
}
