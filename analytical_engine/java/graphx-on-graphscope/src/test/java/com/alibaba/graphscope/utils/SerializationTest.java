package com.alibaba.graphscope.utils;

import com.alibaba.graphscope.graphx.SerializationUtils;
import com.alibaba.graphscope.graphx.SerializationUtils2.Wrapper$;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.apache.spark.util.ClosureCleaner;
import org.junit.Assert;
import org.junit.Test;
import scala.Function3;

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
        SerializationUtils<Long, Long, Long> serializationUtils = new SerializationUtils<>();
        Function3<Long, Long, Long, Long> vprog = serializationUtils.vprog();
        SerializationUtils.write(vprog, "tmp-vprog");
        Function3<Long, Long, Long, Long> vprogRecovered = (Function3<Long, Long, Long, Long>) SerializationUtils.read(
            "tmp-vprog");
        Assert.assertTrue(vprogRecovered.apply(1L, 2L, 3L).equals(1L));
    }

    @Test
    public void test2() throws ClassNotFoundException {
//        SerializationUtils<Long, Long, Long> serializationUtils = new SerializationUtils<>();
//        Function3<Long, Double, Double, Double> vprog = PageRankTest.vertexProgram();
//        SerializationUtils2 serializationUtils2 = new SerializationUtils2();
        Function3<?, ?, ?, ?> vprog = Wrapper$.MODULE$.vprog();
        ClosureCleaner.clean(vprog, true, true);
        SerializationUtils.write(vprog, "tmp-vprog2");
        Function3<Long, Double, Double, Double> vprogRecovered = (Function3<Long, Double, Double, Double>) SerializationUtils.read(
            "tmp-vprog2");
//        System.out.println(vprogRecovered.apply(1L, 2.0, 3.0));
        Assert.assertTrue(vprogRecovered.apply(1L, 2.0, 3.0).equals(3.2));
    }

    @Test
    public void test3() {
//        test("Deserialize object containing a primitive Class as attribute") {
//            val serializer = new JavaSerializer(new SparkConf())
//            val instance = serializer.newInstance()
//            val obj = instance.deserialize[ContainsPrimitiveClass](instance.serialize(
//                new ContainsPrimitiveClass()))
//            // enforce class cast
//            obj.getClass
//        }
//        JavaSerializer serializer = new JavaSerializer(new SparkConf());
//        JavaSerializerInstance instance = serializer.newInstance();

    }
}
