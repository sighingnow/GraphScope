package com.alibaba.graphscope.utils;

import com.alibaba.graphscope.graphx.PythonInterpreter;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import org.junit.Test;

public class SubprocessTest {

    @Test
    public void test2() throws IOException, InterruptedException {
        PythonInterpreter interpreter = new PythonInterpreter();
        interpreter.init();
        interpreter.runCommand("1");
        String res = interpreter.getResult();
        System.out.println(res);
        interpreter.runCommand("1 + 2");
        res = interpreter.getResult();
        System.out.println(res);
        interpreter.close();
        System.out.println("Finish test2");
    }

    @Test
    public void test1()
        throws IOException, InterruptedException, NoSuchFieldException, IllegalAccessException {
        ProcessBuilder builder = new ProcessBuilder("/usr/bin/env", "python3", "-i");
        Process process = builder.start();

        new Thread(() -> {
            String line;
            final BufferedReader reader = new BufferedReader(
                new InputStreamReader(process.getInputStream()));
            // Ignore line, or do something with it
            while (true) {
                try {
                    if ((line = reader.readLine()) == null) {
                        break;
                    } else {
                        System.out.println(line);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }).start();
        new Thread(() -> {
            final PrintWriter writer = new PrintWriter(
                new OutputStreamWriter(process.getOutputStream()));
            writer.println("1");
            writer.println("2 * 2");
            writer.println("exit()");
            writer.flush();
        }).start();

        int res = process.waitFor();
        if (res != 0) {
            System.out.println("Terminate unexpectedly");
        }
    }
}
