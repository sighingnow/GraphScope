package io.graphscope.utils;

public class LoadLibrary {
//    static {
//        //load grape jni library
//        try {
////            NativeLoader.loadLibrary(VINEYARD_JNI_LIBRARY);
//            System.loadLibrary(VINEYARD_JNI_LIBRARY);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }

    /**
     * Used by c++ to load compiled user app
     *
     * @param userLibrary
     */
    public static void invoke(String userLibrary) throws UnsatisfiedLinkError {
        System.out.println("loading library " + userLibrary);
        System.load(userLibrary);
    }
}
