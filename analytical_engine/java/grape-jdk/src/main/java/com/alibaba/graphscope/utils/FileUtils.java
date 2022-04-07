package com.alibaba.graphscope.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class FileUtils {
    public static long getNumLinesOfFile(String path) {
        ProcessBuilder builder = new ProcessBuilder("wc", "-l", path);
        builder.inheritIO().redirectOutput(ProcessBuilder.Redirect.PIPE);
        Process process = null;
        try {
            process = builder.start();
            try (BufferedReader reader =
                new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String res = reader.readLine().split("\\s+")[0];
                return Long.parseLong(res);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }
}
