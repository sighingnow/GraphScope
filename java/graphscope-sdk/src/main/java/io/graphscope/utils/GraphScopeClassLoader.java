package io.graphscope.utils;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.List;
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

    public static Class<?> loadClass(URLClassLoader classLoader, String className) throws ClassNotFoundException {
        Class<?> clz = classLoader.loadClass(className);
        System.out.print("[GS class loader]: loading class " + className + ", " + clz.getName());
        return clz;
    }

    private static URL[] classPath2URLArray(String classPath) {
        if (Objects.isNull(classPath) || classPath.length() == 0) {
            System.err.println("Empty class Path!");
            return new URL[]{};
        }
        String[] splited = classPath.split(":");
        List<URL> res= Arrays.stream(splited)
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
        System.out.println("Extracted URL" + String.join(
                ":",
                res.stream().map(URL::toString).collect(Collectors.toList())));
        URL [] ret = new URL[splited.length];
        for (int i = 0; i < splited.length; ++i){
            ret[i] = res.get(i);
        }
        return ret;
    }
}
