package com.alibaba.graphscope.utils;

import com.alibaba.graphscope.graphx.SerializationUtils;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.function.Function;
import org.junit.Assert;
import org.junit.Test;
import scala.Function1;
import scala.Function3;
import scala.Tuple3;

public class SerializationTest {
     public <A> byte[] write(A obj) throws IOException {
        ByteArrayOutputStream bo = new ByteArrayOutputStream();
        new ObjectOutputStream(bo).writeObject(obj);
        return bo.toByteArray();
    }

    public Object read(byte[] arr) throws IOException, ClassNotFoundException {
         return new ObjectInputStream(new ByteArrayInputStream(arr)).readObject();
    }

    @Test
    public void test() throws ClassNotFoundException {
        SerializationUtils<Long,Long,Long> serializationUtils = new SerializationUtils<>();
        Function3<Long,Long,Long,Long> vprog = serializationUtils.vprog();
        SerializationUtils.write(vprog, "tmp-vprog");
        Function3<Long,Long,Long,Long> vprogRecovered = (Function3<Long, Long, Long, Long>) SerializationUtils.read("tmp-vprog");
        Assert.assertTrue(vprogRecovered.apply(1L,2L,3L).equals(1L));
    }

}
