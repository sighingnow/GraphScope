/*
 * Copyright 2021 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
import java.util.Vector;
import java.util.stream.Collectors;

/**
 * GraphScope Class Loader contains several static functions which will be used by GraphScope grape_engine at runtime,
 * to initiating java runtime environment.
 * When java app(packed in jar) is submitted to grape engine, a jvm will be initiated via JNI interface pointer if jvm
 * has not been created is this process.
 * Rather than directly load java classes via JNI interface pointer, we utilize a URLClassLoader for each jar to do
 * this. The benefit of this solution is the ioslated jvm environment provided by class loaders, which can avoid
 * possible class conflicts.
 */
public class GraphScopeClassLoader {
    private static String FFI_TYPE_FACTORY_CLASS = "com.alibaba.fastffi.FFITypeFactory";

    private static class ClassScope {
        private static java.lang.reflect.Field LIBRARIES = null;
        static {
            try {
                LIBRARIES = ClassLoader.class.getDeclaredField("loadedLibraryNames");
            } catch (NoSuchFieldException e) {
                e.printStackTrace();
            }
            LIBRARIES.setAccessible(true);
        }

        /**
         * Get the libraries already loaded in one classLoader. Note that one lib can not be loaded twice via the same
         * class loader.
         *
         * @param loader
         * @return
         * @throws IllegalAccessException
         */
        public static String[] getLoadedLibraries(final ClassLoader loader) throws IllegalAccessException {
            final Vector<String> libraries = (Vector<String>) LIBRARIES.get(loader);
            return libraries.toArray(new String[] {});
        }
    }

    /**
     * Create a new URLClassLoaders with given classPaths. The classPath shall be in form {@code /path/to/A:B.jar}.
     * @param classPath
     * @return
     * @throws IllegalAccessException
     */
    public static URLClassLoader newGraphScopeClassLoader(String classPath) throws IllegalAccessException {
        String[] libraries = ClassScope.getLoadedLibraries(ClassLoader.getSystemClassLoader());
        log("Loaded lib: " + String.join(" ", libraries));
        return new URLClassLoader(classPath2URLArray(classPath), Thread.currentThread().getContextClassLoader());
    }

    /**
     * Return a default URLClassLoader with no classPath.
     *
     * @return the class loader.
     * 
     * @throws IllegalAccessException
     */
    public static URLClassLoader newGraphScopeClassLoader() throws IllegalAccessException {
        String[] libraries = ClassScope.getLoadedLibraries(ClassLoader.getSystemClassLoader());
        log("Loaded lib: " + String.join(" ", libraries));
        // CAUTION: add '.' to avoid empty url.
        return new URLClassLoader(classPath2URLArray("."), Thread.currentThread().getContextClassLoader());
    }

    /**
     * Load the specified class with one URLClassLoader, return a new instance for this class. The class name
     * could be fully-specified or dash-separated.
     *
     * @param classLoader
     * @param className
     *            a/b/c/ or a.b.c
     * 
     * @return a instance for loaded class.
     * 
     * @throws ClassNotFoundException
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    public static Object loadAndCreate(URLClassLoader classLoader, String className)
            throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        Class<?> clz = classLoader.loadClass(formatting(className));
        return clz.newInstance();
    }

    /**
     * This function wrap one C++ object into a java object, namely a FFIPointer.
     * 
     * @param classLoader The class loader with used to load java classes.
     * @param foreignName The foreign name for C++ object,shall be fully specified.
     * @param address The address for C++ object.
     * 
     * @return a FFIPointer wrapper.
     * 
     * @throws ClassNotFoundException
     * @throws NoSuchMethodException
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public static Object createFFIPointer(URLClassLoader classLoader, String foreignName, long address)
            throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException,
            InstantiationException {
        //FFITypeFactor class need to be ensure loaded in current classLoader, don't make it static.
        Class<?> ffiTypeFactoryClass = classLoader.loadClass(FFI_TYPE_FACTORY_CLASS);
        log("Creating FFIPointer, typename [" + foreignName + "], address [" + address + "]" + ", ffi type factor ["
                + ffiTypeFactoryClass);
        // a new classLoader contains new class path, we load the ffi.properties here.
        Method loadClassLoaderMethod = ffiTypeFactoryClass.getDeclaredMethod("loadClassLoader", ClassLoader.class);
        loadClassLoaderMethod.invoke(null, classLoader);

        // To make FFITypeFactor use our classLoader to find desired type matching, we load FFIType with our
        // classLoader.
        Class<?> ffiTypeClass = classLoader.loadClass("com.alibaba.fastffi.FFIType");
        System.out.println("ffitype cl :" + ffiTypeClass.getClassLoader() + ", url cl: " + classLoader);

        // First load class by FFITypeFactor
        Method getTypeMethod = ffiTypeFactoryClass.getDeclaredMethod("getType", Class.class, String.class);
        Class<?> ffiJavaClass = (Class<?>) getTypeMethod.invoke(null, ffiTypeClass, foreignName);
        // The class loaded by FFITypeFactor's classLoader can not be directly used by us. We load again with our class
        // loader.
        Class<?> javaClass = classLoader.loadClass(ffiJavaClass.getName());
        if (Objects.nonNull(javaClass)) {
            Constructor[] constructors = javaClass.getDeclaredConstructors();
            for (Constructor constructor : constructors) {
                if (constructor.getParameterCount() == 1
                        && constructor.getParameterTypes()[0].getName().equals("long")) {
                    log("Desired constructor exists for " + javaClass.getName());
                    Object obj = constructor.newInstance(address);
                    log("Successfully Construct " + obj);
                    return obj;
                }
            }
            log("No Suitable constructors found.");
        }
        log("Loaded null class.");
        return null;
    }

    /**
     * We now accept two kind of className, a/b/c or a.b.c are both ok.
     * TODO(lei): remove the special case for 'Communicator'
     * 
     * @param classLoader
     * @param className
     * 
     * @return loaded class.
     * 
     * @throws ClassNotFoundException
     */
    public static Class<?> loadClass(URLClassLoader classLoader, String className) throws ClassNotFoundException {
        if (className.equals("Communicator")) {
            Class<?> clz = classLoader
                    .loadClass(formatting(new String("com.alibaba.grape.communication.Communicator")));
            log("Loaded communicator class");
            return clz;
        }
        Class<?> clz = classLoader.loadClass(formatting(className));
        log("Loaded class " + className);
        return clz;
    }

    private static String formatting(String className) {
        if (className.indexOf("/") == -1)
            return className;
        return className.replace("/", ".");
    }

    private static URL[] classPath2URLArray(String classPath) {
        if (Objects.isNull(classPath) || classPath.length() == 0) {
            System.err.println("Empty class Path!");
            return new URL[] {};
        }
        String[] splited = classPath.split(":");
        List<URL> res = Arrays.stream(splited).map(File::new).map(file -> {
            try {
                return file.toURL();
            } catch (MalformedURLException e) {
                e.printStackTrace();
            }
            return null;
        }).collect(Collectors.toList());
        System.out.println(
                "Extracted URL" + String.join(":", res.stream().map(URL::toString).collect(Collectors.toList())));
        URL[] ret = new URL[splited.length];
        for (int i = 0; i < splited.length; ++i) {
            ret[i] = res.get(i);
        }
        return ret;
    }

    private static void log(String info) {
        System.out.print("[GS Class Loader]: ");
        System.out.println(info);
    }
}
