package io.graphscope.utils;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
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

    public static Object loadClassAndCreate(URLClassLoader classLoader, Class<?> clz, long address) throws ClassNotFoundException {
        System.out.println("[GS class loader]: re loading class " + clz.getName() + " with loader: " + classLoader + ", " + clz.getClassLoader());
        Class<?> clazz = loadClass(classLoader, clz.getName());

        Constructor[] constructors = clazz.getDeclaredConstructors();
        Object res = null;
        for (Constructor constructor : constructors){
            if (constructor.getParameterCount() == 1 && constructor.getParameterTypes()[0].getName().equals("long")){
                System.out.println("[GS class loader]: desired constructor exists.");
                try {
                    res = constructor.newInstance(address);
                    System.out.println("[GS class loader]: Construct "+ res);
                }
                catch (Exception e){
                    e.printStackTrace();
                }
            }
        }
        if (Objects.isNull(res)){
            System.out.println("[ERROR] No desired constructor found for " + clz.getName());
        }
        return res;
    }

    public static Class<?> loadClass(URLClassLoader classLoader, String className) throws ClassNotFoundException {
        Class<?> clz = classLoader.loadClass(className);
        System.out.println("[GS class loader]: loading class " + className + ", " + clz.getName());
//        System.out.println("[GS class loader]: url loader: " + classLoader + ", getClassLoader: " + clz.getClassLoader().toString());
//        {
//            Constructor[] constructors = clz.getDeclaredConstructors();
//            for (Constructor constructor : constructors){
//                if (constructor.getParameterCount() == 1 && constructor.getParameterTypes()[0].getName().equals("long")){
//                    System.out.println("[GS class loader]: ffi get class, desired constructor exists.");
//                }
//            }
//        }
//        Class<?> urlLoadedClass = classLoader.loadClass(clz.getName());
//        {
//            Constructor[] constructors = urlLoadedClass.getDeclaredConstructors();
//            for (Constructor constructor : constructors){
//                if (constructor.getParameterCount() == 1 && constructor.getParameterTypes()[0].getName().equals("long")){
//                    System.out.println("[GS class loader]: url loaded class, desired constructor exists.");
//                }
//            }
//        }
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
