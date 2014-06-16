/*
 * Copyright (c) 2014, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.simple.connectors;

import java.lang.reflect.Method;
import java.util.logging.Level;
import java.util.logging.Logger;
import com.arjuna.databroker.data.DataConsumer;
import com.arjuna.databroker.data.DataFlowNode;
import com.arjuna.databroker.data.DataProvider;

public class SimpleDataConsumer<T> implements DataConsumer<T>
{
    private static final Logger logger = Logger.getLogger(SimpleDataConsumer.class.getName());

    public SimpleDataConsumer(DataFlowNode dataFlowNode, Method method)
    {
        logger.log(Level.FINE, "SimpleDataConsumer: " + dataFlowNode + ", " + method);
        
        _dataFlowNode = dataFlowNode;
        _method       = method;
    }

    @Override
    public DataFlowNode getDataFlowNode()
    {
        return _dataFlowNode;
    }

    @Override
    public void consume(DataProvider<T> dataProvider, T data)
    {
        try
        {
            _method.invoke(_dataFlowNode, data);
        }
        catch (Throwable throwable)
        {
            logger.log(Level.WARNING, "Problem invoking consumer", throwable);
        }
    }

    private DataFlowNode _dataFlowNode;
    private Method       _method;
}
