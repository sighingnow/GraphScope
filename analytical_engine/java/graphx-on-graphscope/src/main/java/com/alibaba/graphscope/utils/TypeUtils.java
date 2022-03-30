package com.alibaba.graphscope.utils;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Int;

public class TypeUtils {
    //These constants should be consistent with LoaderUtils
    private static final int CODE_WIDTH = 4; // 4bits
    // Follows vineyard::type_to_int
    private static final int LONG_CODE = 0x0004;
    private static final int INT_CODE = 0x0002;
    private static final int DOUBLE_CODE = 0x0007;
    private static final int FLOAT_CODE = 0x0006;
    private static Logger logger = LoggerFactory.getLogger(TypeUtils.class);
    /**
     * Generate an int containing clz array info.
     *
     * @param clzs input classes.
     * @return generated value, the order of encoded bits value is the same as input order.
     */
    public static int generateTypeInt(Class<?>... clzs) {
        if (clzs.length >= 32) {
            throw new IllegalStateException("expect less than 32 clzs");
        }
        StringBuilder sb = new StringBuilder();
        int res = 0;
        for (Class<?> clz : clzs) {
            res <<= CODE_WIDTH;
            res |= classToInt(clz);
            sb.append(clz.getSimpleName() + ", ");
        }
        logger.info("clz {} =>  binary string {}",sb.toString(), Integer.toBinaryString(res));
        return res;
    }

    public static int classToInt(Class<?> clz) {
        if (clz.equals(Long.class) || clz.equals(long.class)) {
            return LONG_CODE;
        } else if (clz.equals(Integer.class) || clz.equals(int.class)) {
            return INT_CODE;
        } else if (clz.equals(Double.class) || clz.equals(double.class)) {
            return DOUBLE_CODE;
        } else if (clz.equals(Float.class) || clz.equals(float.class)) {
            return FLOAT_CODE;
        }
        throw new IllegalStateException("Unrecognized class: " + clz);
    }
}
