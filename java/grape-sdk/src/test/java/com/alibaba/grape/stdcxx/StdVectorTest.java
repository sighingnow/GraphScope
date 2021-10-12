package com.alibaba.grape.stdcxx;

import com.alibaba.fastffi.FFIByteString;
import com.alibaba.fastffi.FFITypeFactory;
import org.junit.Assert;
import org.junit.Test;

public class StdVectorTest {

    private StdVector.Factory<Integer> intVectorFactory =
            FFITypeFactory.getFactory(StdVector.class, "std::vector<jint>");

    private StdVector.Factory<FFIByteString> stringVectorFactory =
            FFITypeFactory.getFactory(StdVector.class, "std::vector<std::string>");
    private StdString.Factory stringFactory = StdString.factory;

    @Test
    public void test1() {
        StdVector<Integer> integerStdVector = intVectorFactory.create();
        for (int i = 0; i < 10; ++i) {
            integerStdVector.push_back(i);
        }
        Assert.assertTrue(integerStdVector.size() == 10);
        for (int i = 0; i < 10; ++i) {
            Assert.assertEquals(i, integerStdVector.get(i).intValue());
        }
        integerStdVector.clear();
    }

    @Test
    public void test2() {
        StdVector<FFIByteString> stringVector = stringVectorFactory.create();
        for (int i = 0; i < 10; ++i) {
            String tmp = "tmp" + i;
            FFIByteString str = FFITypeFactory.newByteString();
            str.copyFrom(tmp);
            stringVector.push_back(str);
        }
        Assert.assertTrue(stringVector.size() == 10);
        for (int i = 0; i < 10; ++i) {
            Assert.assertEquals("tmp" + i, stringVector.get(i).toString());
        }
        stringVector.clear();
    }
}
