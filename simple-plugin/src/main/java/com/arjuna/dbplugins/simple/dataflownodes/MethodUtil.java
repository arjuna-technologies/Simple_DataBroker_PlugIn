/*
 * Copyright (c) 2014, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.simple.dataflownodes;

import java.lang.reflect.Method;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MethodUtil
{
    private static final Logger logger = Logger.getLogger(MethodUtil.class.getName());

    public static Method getMethod(Class<?> nodeClass, String nodeMethodName)
    {
        try
        {
            return nodeClass.getMethod(nodeMethodName, new Class[]{String.class});
        }
        catch (Throwable throwable)
        {
            logger.log(Level.WARNING, "Unable to find method \"" + nodeMethodName + "\"", throwable);

            return null;
        }
    }
}
