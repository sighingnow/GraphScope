package com.alibaba.graphscope.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.ProcessBuilder.Redirect;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class SubprocessTest {

    @Test
    public void test1() throws IOException, InterruptedException {
        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.command("python3");
        Process process = processBuilder.start();
        OutputStream os = process.getOutputStream();
        InputStream is = process.getInputStream();
//        processBuilder.redirectInput(new File("subprocess-test-output"));
//        processBuilder.redirectOutput(new Redirect())
        os.write("a = 1\n".getBytes(StandardCharsets.UTF_8));
        os.write("b = 2\n".getBytes(StandardCharsets.UTF_8));
        os.write("a + b\n".getBytes(StandardCharsets.UTF_8));
        os.flush();
//        process.getInputStream().
//        BufferedReader stdInput = new BufferedReader(new InputStreamReader(is));
//        String str;
//        while ((str = stdInput.readLine()) != null) {
//            System.out.println(str);
//        }
//        TimeUnit.SECONDS.sleep(3);
        process.destroyForcibly();
        process.waitFor();
    }
}
