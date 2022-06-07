package com.alibaba.graphscope.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A python interpreter wrapped with java subprocess. We use this to executor graphscope python
 * code.
 */
public class PythonInterpreter {

    private Logger logger = LoggerFactory.getLogger(PythonInterpreter.class);
    private Process process;
    private BlockingQueue<String> outputQueue, inputQueue;
    private InterpreterOutputStream os;
    private InterpreterInputStream is;

    public PythonInterpreter(){

    }
    public void init() throws IOException {
        ProcessBuilder builder = new ProcessBuilder("/usr/bin/env", "python3", "-i");
        process = builder.start();
        outputQueue = new LinkedBlockingQueue<>();
        inputQueue = new LinkedBlockingQueue<>();
        logger.info("Start process {}", process);
        os = new InterpreterOutputStream(process.getInputStream(), outputQueue);
        is = new InterpreterInputStream(process.getOutputStream(), inputQueue);
        os.start();
        is.start();
    }

    public void runCommand(String str) {
        inputQueue.offer(str);
        logger.info("offering cmd str {}", str);
    }

    public String getResult() throws InterruptedException {
        if (is.isAlive()) {
            logger.info("input stream thread alive, use take");
            return outputQueue.take();
        } else {
            logger.info("input stream thread dead, use poll");
            return outputQueue.poll();
        }
    }

    public void close() throws InterruptedException {
        is.end();
        logger.info("closing input stream thread");
        is.interrupt();
        os.join();
        logger.info("");
    }

    public static class InterpreterInputStream extends Thread {

        private Logger logger = LoggerFactory.getLogger(InterpreterInputStream.class.getName());
        private PrintWriter writer;
        private BlockingQueue<String> queue;

        public InterpreterInputStream(OutputStream os, BlockingQueue<String> inputQueue) {
            writer = new PrintWriter(new OutputStreamWriter(os));
            queue = inputQueue;
        }

        @Override
        public void run() {
            String cmd;
            while (!interrupted()) {
                try {
                    cmd = queue.take();
                    writer.println(cmd);
                    logger.info("Submitting command {}", cmd);
                    writer.flush();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        public void end() {
            boolean res = queue.offer("exit()");
            if (!res) {
                throw new IllegalStateException("exit from subprocess failed");
            }
        }
    }

    public static class InterpreterOutputStream extends Thread {

        private Logger logger = LoggerFactory.getLogger(InterpreterOutputStream.class.getName());
        private BufferedReader reader;
        private BlockingQueue<String> queue;

        public InterpreterOutputStream(InputStream is, BlockingQueue<String> queue) {
            reader = new BufferedReader(new InputStreamReader(is));
            this.queue = queue;
        }

        @Override
        public void run() {
            String line;
            int cnt = 0;
            while (true) {
                try {
                    if ((line = reader.readLine()) == null) {
                        break;
                    } else {
                        queue.add(line);
                        cnt += 1;
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            logger.info("totally {} lines were read from interpreter", cnt);
        }
    }
}
