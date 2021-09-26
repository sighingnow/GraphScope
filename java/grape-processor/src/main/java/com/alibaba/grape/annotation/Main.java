package com.alibaba.grape.annotation;

public class Main {

    public static final boolean ignoreError = true;
    public static final boolean verbose = false;

    /**
     * @param args
     */
    public static void main(String[] args) {
        if (args.length != 3) {
            System.out.println("Expected 3 params.");
            return;
        }
        System.out.println("Files are generated in " + GrapeAppScanner.scanAppAndGenerate(args[0], args[1], args[2], true));
    }
}
