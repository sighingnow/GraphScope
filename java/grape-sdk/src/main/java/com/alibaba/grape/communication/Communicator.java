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

package com.alibaba.grape.communication;

import com.alibaba.fastffi.FFITypeFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Objects;

import static com.alibaba.grape.utils.CppClassName.GRAPE_COMMUNICATOR;

/**
 * Let c++ detect whether the app class is instance of Communicator.
 * if yes, we call init communicator.
 */
public abstract class Communicator {
    private FFICommunicator communicatorImpl;
    private static Logger logger = LoggerFactory.getLogger(Communicator.class.getName());

    /**
     * This function is set private, mean to only be called by jni, and let the exceptions accepted by cpp,
     * so they can be obviously displayed.
     *
     * @param appAddr
     * @throws ClassNotFoundException
     * @throws InvocationTargetException
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    private void initCommunicator(long appAddr) throws ClassNotFoundException, InvocationTargetException, InstantiationException, IllegalAccessException {
        Class<FFICommunicator> communicatorClass = (Class<FFICommunicator>) FFITypeFactory.getType(GRAPE_COMMUNICATOR);
        Constructor[] constructors = communicatorClass.getConstructors();

        for (Constructor constructor : constructors) {
            if (constructor.getParameterCount() == 1 && constructor.getParameterTypes()[0].getName().equals("long")) {
                communicatorImpl = communicatorClass.cast(constructor.newInstance(appAddr));
                System.out.println(communicatorImpl);
            }
        }
    }

    public <MSG_T> void sum(MSG_T msgIn, MSG_T msgOut) {
        if (Objects.isNull(communicatorImpl)) {
            logger.error("Communicator null ");
            return;
        }
        communicatorImpl.sum(msgIn, msgOut);
    }

    public <MSG_T> void min(MSG_T msgIn, MSG_T msgOut) {
        if (Objects.isNull(communicatorImpl)) {
            logger.error("Communicator null ");
            return;
        }
        communicatorImpl.min(msgIn, msgOut);
    }

    public <MSG_T> void max(MSG_T msgIn, MSG_T msgOut) {
        if (Objects.isNull(communicatorImpl)) {
            logger.error("Communicator null ");
            return;
        }
        communicatorImpl.max(msgIn, msgOut);
    }
}
