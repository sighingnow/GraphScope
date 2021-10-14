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

package io.graphscope.utils;

import io.graphscope.app.ProjectedDefaultAppBase;
import io.graphscope.app.PropertyDefaultAppBase;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

public class AppBaseParser {
    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Error: Expected only one class, fully named.");
            return;
        }
        loadClassAndParse(args[0]);
    }

    private static void loadClassAndParse(String className) {
        try {
            Class<?> clz = Class.forName(className);
            boolean flag = PropertyDefaultAppBase.class.isAssignableFrom(clz);
            if (flag == true) {
                System.out.println("PropertyDefaultApp");
                Class<? extends PropertyDefaultAppBase> clzCasted = (Class<? extends PropertyDefaultAppBase>) clz;
                Type type = clzCasted.getGenericInterfaces()[0];
                if (type instanceof ParameterizedType) {
                    ParameterizedType parameterizedType = (ParameterizedType) type;
                    Type[] typeParams = parameterizedType.getActualTypeArguments();
                    if (typeParams.length != 2) {
                        System.out.println("Error: Number of params error, expected 2, actuval " + typeParams.length);
                        return;
                    }
                    System.out.println("TypeParams: " + typeParams[0].getTypeName());
                    return;
                }
                System.out.println("Error: Not a parameterized type " + type.getTypeName());
                return;
            }
            // try Projected
            flag = ProjectedDefaultAppBase.class.isAssignableFrom(clz);
            if (flag == true) {
                System.out.println("ProjectedDefaultApp");
                Class<? extends ProjectedDefaultAppBase> clzCasted = (Class<? extends ProjectedDefaultAppBase>) clz;
                Type type = clzCasted.getGenericInterfaces()[0];
                if (type instanceof ParameterizedType) {
                    ParameterizedType parameterizedType = (ParameterizedType) type;
                    Type[] typeParams = parameterizedType.getActualTypeArguments();
                    String[] typeParamNames = new String[4];
                    if (typeParams.length != 5) {
                        System.out.println("Error: Number of params error, expected 5, actuval " + typeParams.length);
                        return;
                    }
                    for (int i = 0; i < 4; ++i) {
                        typeParamNames[i] = typeParams[i].getTypeName();
                    }
                    System.out.println("TypeParams: " + String.join(",", typeParamNames));
                    return;
                }
                System.out.println("Error: Not a parameterized type " + type.getTypeName());
                return;
            }
            System.out.println("Unrecognizable class Name");
        } catch (Exception e) {
            System.out.println("Exception occurred");
            e.printStackTrace();
        }
    }

    private static Method getMethod(Class<?> clz) {
        Method[] methods = clz.getDeclaredMethods();
        for (Method method : methods) {
            if (method.getName().equals("PEval")) {
                return method;
            }
        }
        return null;
    }

    private static Class<?> getFragmentClassFromMethod(Method method) {
        Class<?>[] params = method.getParameterTypes();
        if (params.length != 3) {
            System.err.println("Expected 3 parameters for this method: " + method.getName());
            return null;
        }
        return params[0];
    }
}
