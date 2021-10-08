package io.graphscope.runtime;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class GraphScopeClassLoader {
    private static String FFI_TYPE_FACTORY_CLASS = "com.alibaba.ffi.FFITypeFactory";
    public static Class<?> ffiTypeFactoryClass = null;
    public static URLClassLoader newGraphScopeClassLoader(String classPath) {
        return new URLClassLoader(classPath2URLArray(classPath),
                Thread.currentThread().getContextClassLoader());
    }

    /**
     * Invoke the non-param constructor
     *
     * @param classLoader
     * @param className a/b/c/ or a.b.c
     * @return
     * @throws ClassNotFoundException
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    public static Object loadAndCreate(URLClassLoader classLoader,
                                             String className)
            throws ClassNotFoundException, InstantiationException,
            IllegalAccessException {
        Class<?> clz = classLoader.loadClass(formatting(className));
        return clz.newInstance();
    }

    /**
     * Create FFIPointer with the helper of FFITypeFactory class. FFITupeFactoryClass should be able to load in classLoader.
     * @param classLoader
     * @param foreignName
     * @param address
     * @return
     * @throws ClassNotFoundException
     * @throws NoSuchMethodException
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public static Object createFFIPointer(URLClassLoader classLoader, String foreignName, long address) throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException, InstantiationException {
        synchronized (FFI_TYPE_FACTORY_CLASS){
            if (Objects.isNull(ffiTypeFactoryClass)){
                ffiTypeFactoryClass = classLoader.loadClass(FFI_TYPE_FACTORY_CLASS);
            }
        }
        log("Creating FFIPointer, typename [" + foreignName + "], address [" + address + "]" + ", ffi type factor [" + ffiTypeFactoryClass);
        //First load class by FFITypeFactor
        Method getTypeMethod = ffiTypeFactoryClass.getDeclaredMethod("getType", String.class);
        Class<?> javaClass = (Class<?>) getTypeMethod.invoke(null, foreignName);
        if (Objects.nonNull(javaClass)){
            Constructor[] constructors = javaClass.getDeclaredConstructors();
            for (Constructor constructor : constructors){
                if (constructor.getParameterCount() == 1 && constructor.getParameterTypes()[0].getName().equals("long")){
                    log("Desired constructor exists for " + javaClass.getName());
                    Object obj = constructor.newInstance(address);
                    log("Successfully Construct "+ obj);
                    return obj;
                }
            }
            log("No Suitable constructors found.");
        }
        return null;
    }

    /**
     * We now accept two kind of className, a/b/c or a.b.c are both ok.
     * Special case: for 'Communicator'
     * @param classLoader
     * @param className
     * @return
     * @throws ClassNotFoundException
     */
    public static Class<?> loadClass(URLClassLoader classLoader, String className) throws ClassNotFoundException {
        if (className.equals("Communicator")){
            Class<?> clz = classLoader.loadClass(formatting(new String("com.alibaba.grape.communication.Communicator")));
            log("Loaded communicator class");
            return clz;
        }
        Class<?> clz = classLoader.loadClass(formatting(className));
        log("Loaded class " + className);
        return clz;
    }
    private static String formatting(String className){
        if (className.indexOf("/") == -1) return className;
        return className.replace("/", ".");
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
    private static void log(String info){
        System.out.print("[GS Class Loader]: ");
        System.out.println(info);
    }
}
