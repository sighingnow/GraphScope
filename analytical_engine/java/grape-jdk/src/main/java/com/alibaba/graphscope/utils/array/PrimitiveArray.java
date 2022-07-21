package com.alibaba.graphscope.utils.array;

import com.alibaba.graphscope.utils.array.impl.DoubleArray;
import com.alibaba.graphscope.utils.array.impl.IntArray;
import com.alibaba.graphscope.utils.array.impl.LongArray;
import com.alibaba.graphscope.utils.array.impl.ObjectArray;
import java.io.Serializable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface PrimitiveArray<T> extends Serializable{
    Logger logger = LoggerFactory.getLogger(PrimitiveArray.class.getName());
    T get(int index);

    default T get(long index){
        return get((int) index);
    }

    void set(int index, T value);

    default void set(long index, T value){
        set((int) index, value);
    }

    int size();

    static <TT>PrimitiveArray<TT> create(Class<? extends TT> clz, int len){
        if (clz.equals(double.class) || clz.equals(Double.class)){
//            logger.info("creating double primitive array");
            return (PrimitiveArray<TT>) new DoubleArray(len);
        }
        else if (clz.equals(long.class) || clz.equals(Long.class)){
//            logger.info("creating long primitive array");
            return (PrimitiveArray<TT>) new LongArray(len);
        }
        else if (clz.equals(int.class) || clz.equals(Integer.class)){
//            logger.info("creating int primitive array");
            return (PrimitiveArray<TT>) new IntArray(len);
        }
        else {
            return (PrimitiveArray<TT>) new ObjectArray(clz,len);
        }
    }
}
