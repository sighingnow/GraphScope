package com.alibaba.grape.jobConf;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class jobConfTest {
    private JobConf jobConf;
    @Before
    public void setEnv(){
        jobConf = new JobConf();
    }

    @Test
    public void test1(){
        jobConf.setEfilePath("efilePath");
        jobConf.setVfilePath("vfilePath");
        try {
            Assert.assertTrue(jobConf.submit());
        }
        catch (IOException e){
            e.printStackTrace();
        }

        JobConf local = new JobConf();
        local.readConfig();
        Assert.assertEquals("efilePath", local.getEfilePath());
        Assert.assertEquals("vfilePath", local.getVfilePath());
    }
}
