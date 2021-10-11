package com.alibaba.grape.utils;

import static com.alibaba.grape.utils.JNILibraryName.GRAPE_JNI_LIBRARY;

public class LoadLibrary {
    static {
        //load grape jni library
        try {
//            NativeLoader.loadLibrary(GRAPE_JNI_LIBRARY);
//            System.out.println(GRAPE_JNI_LIBRARY);
            System.loadLibrary(GRAPE_JNI_LIBRARY);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Used by c++ to load compiled user app
     * Load the full path library
     *
     * @param userLibrary
     */
    public static void invoke(String userLibrary) {
        System.out.println("loading " + userLibrary);
        System.load(userLibrary);
    }
}