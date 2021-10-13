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

package io.graphscope.fragment;

import com.alibaba.fastffi.FFITypeFactory;
import com.alibaba.grape.stdcxx.StdUnorderedMap;
import io.graphscope.utils.VineyardHelper;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static io.graphscope.utils.JNILibraryName.VINEYARD_JNI_LIBRARY;

public class ArrowFragmentGroupTest {
    static {
        try {
            System.loadLibrary(VINEYARD_JNI_LIBRARY);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static final String arrowFragmentGroupFFIName = "vineyard::ArrowFragmentGroup";
    public static List<String> params;
    //    private static final String socket = "/tmp/vineyard.sock.fragGroup";
    public long fragObjectId;
    private ArrowFragmentGroup fragmentGroup;
    private static Process vineyard_server;

//    @BeforeClass
//    public static void startVineyardServer() {
//
//        String[] commands = {"/bin/bash", "-c", "vineyardd -socket=" + socket};
//        ProcessBuilder builder = new ProcessBuilder();
//        try {
//            builder.command(commands);
//            File log = new File("vineyardd-log");
//            File error = new File("vineyardd-error");
//            builder.redirectOutput(ProcessBuilder.Redirect.appendTo(error));
//            builder.redirectError(ProcessBuilder.Redirect.appendTo(log));
//            vineyard_server = builder.start();
//            Thread.sleep(4000);
//            System.out.println("vineyardd server started, listening to" + socket);
//        } catch (IOException | InterruptedException e) {
//            e.printStackTrace();
//            Assert.fail("exeception");
//        } finally {
//            System.out.println("finally");
//        }
//    }


    public void cppLoadFragment() {
        loadConf();
        String concat = String.join(" ", params);
        System.out.println(concat);
        String[] commands = {"/bin/bash", "-c", concat};
        ProcessBuilder builder = new ProcessBuilder();
        try {
            builder.command(commands);
            File log = new File("vineyard-exec-fragment-group-test-log");
            File error = new File("vineyard-exec-fragment-group-test-error");
            builder.redirectOutput(ProcessBuilder.Redirect.appendTo(error));
            builder.redirectError(ProcessBuilder.Redirect.appendTo(log));
            Process process = builder.start();
            int exitVal = process.waitFor();
            if (exitVal == 0) {
                System.out.println("Success!");
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("vineyard-exec-log")))) {
                    String tmp;
                    while ((tmp = reader.readLine()) != null) {
                        if (tmp.indexOf("fragment_group_id") != -1) {
                            fragObjectId = Long.parseUnsignedLong(tmp.substring(tmp.lastIndexOf(":") + 1).trim(), 10);
                        }
                    }
                }
                System.out.println("frag objectid " + fragObjectId);
            } else {
                Assert.fail("exit with" + exitVal);
                return;
            }
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("IO exeception");
        } catch (InterruptedException e) {
            e.printStackTrace();
            Assert.fail("interrupted");
        } finally {
            System.out.println("finally");
        }
    }

    @Test
    public void testFFIFragment() {
        cppLoadFragment();
        //create fragment
        try {
//            Class<ArrowFragment> fragmentClass = (Class<ArrowFragment>) FFITypeFactory.getType(arrowFragmentFFIName);
            Class<ArrowFragmentGroup> fragmentGroupClass = (Class<ArrowFragmentGroup>) FFITypeFactory.getType(arrowFragmentGroupFFIName);
            Constructor[] constructors = fragmentGroupClass.getConstructors();
            if (constructors.length == 0) {
                Assert.fail("no constructors found for " + arrowFragmentGroupFFIName);
            }
            for (Constructor constructor : constructors) {
                System.out.println(constructor);
                if (constructor.getParameterCount() == 1 && constructor.getParameterTypes()[0].getName().equals("long")) {
                    System.out.println("frag object id " + fragObjectId);
                    long fragAddr = VineyardHelper.getFragGroupAddrFromObjectId(fragObjectId);
                    System.out.println(" fragAddr " + fragAddr);
                    fragmentGroup = fragmentGroupClass.cast(constructor.newInstance(fragAddr));
                    System.out.println(fragmentGroup);
                }
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            Assert.fail("didn't find class " + arrowFragmentGroupFFIName);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("failed for other exceptions");
        }

        //test some simple getter
        System.out.println("fragment group: address " + fragmentGroup.getAddress());
        System.out.println("fragment group: edge label num" + fragmentGroup.edgeLabelNum());
        System.out.println("fragment group: vertex label num" + fragmentGroup.vertexLabelNum());

        StdUnorderedMap<Integer, Long> fragmentLocations = fragmentGroup.fragmentLocations();
        StdUnorderedMap<Integer, Long> fragments = fragmentGroup.fragments();
        System.out.println("fragments address: " + fragments.getAddress());
        Assert.assertTrue("fragments empty", !fragments.empty());
        System.out.println("fragment group: fragments " + fragments.size());
        System.out.println("fragment group: fragment locations: " + fragmentLocations.size());
        long fragmentId = fragments.get(0);
        System.out.println("fragment id: " + fragmentId);

        //More test to fragment?
    }

//    @AfterClass
//    public static void shutdown() throws InterruptedException {
//        vineyard_server.destroyForcibly();
//        vineyard_server.waitFor();
//        while (vineyard_server.isAlive()) {
//            Thread.sleep(500);
//            System.out.println("waiting for to be killed");
//        }
//        System.out.println("shutdown vineyard server");
//    }

    public static void loadConf() {
        params = new ArrayList<>();
        try {
            Properties properties = new Properties();
            InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("vineyard.properties");
            properties.load(in);
            params.add(properties.getProperty("vineyard_cmd"));
            params.add(properties.getProperty("socket_name"));
            params.add(properties.getProperty("elabel_num"));
            params.addAll(Arrays.asList(properties.getProperty("efiles").split(":")));
            params.add(properties.getProperty("vlabel_num"));
            params.addAll(Arrays.asList(properties.getProperty("vfiles").split(":")));
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
            return;
        }
    }
}
