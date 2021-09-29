package io.graphscope.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

/**
 * Java 1.8 does not allow us to modify classPath at runtime, so we need another ugly way to add jar to classPath at runtime.
 */
public class ClassPathHelper {
    public static Logger logger = LoggerFactory.getLogger(ClassPathHelper.class.getName());

    /**
     * Let JNI catch the exception
     * @param fileName
     * @throws MalformedURLException
     * @throws NoSuchMethodException
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     */
    public static void addFileToClassPath(String fileName) throws MalformedURLException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        File file = new File(fileName);
        if (file.exists()){
            logger.info("Adding [" + fileName + "] to class path.");
            URL url = file.toURI().toURL();

            URLClassLoader classLoader = (URLClassLoader) ClassLoader.getSystemClassLoader();
            Method method = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
            method.setAccessible(true);
            method.invoke(classLoader, url);
        }
        logger.error("No existing file: " + fileName);
    }
    
}
