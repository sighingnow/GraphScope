package com.alibaba.graphscope.utils;

import com.alibaba.graphscope.fragment.BaseGraphXFragment;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import com.alibaba.graphscope.stdcxx.FFIByteVector;
import com.alibaba.graphscope.utils.array.PrimitiveArray;
import java.io.IOException;
import java.util.BitSet;
import scala.Function2;
import scala.Tuple2;
import scala.collection.Iterator;

/**
 * Message store with bitset indicating validity.
 * @param <T> type
 */
public interface MessageStore<T> extends PrimitiveArray<T> {

    void addMessages(Iterator<Tuple2<Long,T>> msgs, BaseGraphXFragment<Long,Long,?,?> fragment, BitSet nextSet);

    void flushMessages(BitSet nextSet, DefaultMessageManager messageManager,BaseGraphXFragment<Long,Long,?,?> fragment)
        throws IOException;

    void digest(FFIByteVector vector,BaseGraphXFragment<Long,Long,?,?> fragment, BitSet curSet);

    static <T> MessageStore<T> create(int fnum, int len, Class<? extends T> clz, Function2<T,T,T> function2)
        throws IOException {
        if (clz.equals(Long.class) || clz.equals(long.class)){
            return (MessageStore<T>) new LongMessageStore(len,fnum,
                (Function2<Long, Long, Long>) function2);
        }
        else if (clz.equals(Double.class) || clz.equals(double.class)){
            return (MessageStore<T>) new DoubleMessageStore(len, fnum,
                (Function2<Double, Double, Double>) function2);
        }
        else if (clz.equals(Integer.class) || clz.equals(int.class)){
            return (MessageStore<T>) new IntMessageStore(len, fnum,
                (Function2<Integer, Integer, Integer>) function2);
        }
        else return new ObjectMessageStore<>(len,fnum, clz,function2);
    }
}
