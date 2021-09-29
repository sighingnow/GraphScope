package io.graphscope.utils;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;

public class GraphScopeClassLoader {
    public static URLClassLoader newGraphScopeClassLoader(String classPath) {
        return new URLClassLoader(classPath2URLArray(classPath),
                Thread.currentThread().getContextClassLoader());
    }

    /**
     * Invoke the non-param constructor
     *
     * @param classLoader
     * @param className
     * @return
     * @throws ClassNotFoundException
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    public static Object loadAndCreateObject(URLClassLoader classLoader,
                                             String className)
            throws ClassNotFoundException, InstantiationException,
            IllegalAccessException {
        Class<?> clz = classLoader.loadClass(className);
        return clz.newInstance();
    }

    private static URL[] classPath2URLArray(String classPath) {
        if (Objects.isNull(classPath) || classPath.length() == 0) {
            System.err.println("Empty class Path!");
            return new URL[]{};
        }
        String[] splited = classPath.split(":");
        URL[] res = new URL[splited.length];
        Arrays.stream(splited)
                .map(File::new)
                .map(file -> {
                    try {
                        return file.toURL();
                    } catch (MalformedURLException e) {
                        e.printStackTrace();
                    }
                    return null;
                })
                .collect(Collectors.toList());
        System.out.println(String.join(
                ":",
                Arrays.stream(res).map(URL::toString).collect(Collectors.toList())));
        return res;
    }
}
