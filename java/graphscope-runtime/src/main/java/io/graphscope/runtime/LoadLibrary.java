package io.graphscope.runtime;

public class LoadLibrary {
    /**
     * Loading the library with absolute path.
     * @param userLibrary
     */
    public static void invoke(String userLibrary) {
        System.out.println("loading " + userLibrary);
        System.load(userLibrary);
    }
}
