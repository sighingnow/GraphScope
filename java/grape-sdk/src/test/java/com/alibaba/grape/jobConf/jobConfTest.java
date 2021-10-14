/*
 * Copyright 2021 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.grape.jobConf;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class jobConfTest {
    private JobConf jobConf;

    @Before
    public void setEnv() {
        jobConf = new JobConf();
    }

    @Test
    public void test1() {
        jobConf.setEfilePath("efilePath");
        jobConf.setVfilePath("vfilePath");
        try {
            Assert.assertTrue(jobConf.submit());
        } catch (IOException e) {
            e.printStackTrace();
        }

        JobConf local = new JobConf();
        local.readConfig();
        Assert.assertEquals("efilePath", local.getEfilePath());
        Assert.assertEquals("vfilePath", local.getVfilePath());
    }
}
