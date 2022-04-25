package com.alibaba.graphscope.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileUtils {
    private static Logger logger = LoggerFactory.getLogger(FileUtils.class.getName());
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

    public static void sendFiles(String[] files, String destination){
        for (String file : files){
            logger.info("sending file [{}] to [{}]", file, destination);
            ProcessBuilder builder = new ProcessBuilder("scp", file, destination+":" + file);
            builder.inheritIO().redirectOutput(ProcessBuilder.Redirect.PIPE);
            Process process = null;
            try {
                process = builder.start();
                process.waitFor();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
