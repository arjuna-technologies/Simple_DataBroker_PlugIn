/*
 * Copyright (c) 2014-2015, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.simple.dataflownodes;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;
import com.arjuna.databroker.data.DataConsumer;
import com.arjuna.databroker.data.DataFlow;
import com.arjuna.databroker.data.DataSink;
import com.arjuna.databroker.data.jee.DataFlowNodeState;
import com.arjuna.databroker.data.jee.annotation.DataConsumerInjection;
import com.arjuna.databroker.data.jee.annotation.DataFlowNodeStateInjection;
import com.arjuna.databroker.data.jee.annotation.PostCreated;

public class SimpleDataSink implements DataSink
{
    private static final Logger logger = Logger.getLogger(SimpleDataSink.class.getName());

    public SimpleDataSink(String name, Map<String, String> properties)
    {
        logger.log(Level.FINE, "SimpleDataSink: " + name + ", " + properties);

        _name       = name;
        _properties = properties;

        _sentHistory = new LinkedList<String>();
    }

    @Override
    public String getName()
    {
        return _name;
    }

    @Override
    public void setName(String name)
    {
        _name = name;
    }

    @Override
    public Map<String, String> getProperties()
    {
        return Collections.unmodifiableMap(_properties);
    }

    @Override
    public void setProperties(Map<String, String> properties)
    {
        _properties = properties;
    }
    
    @Override
    public DataFlow getDataFlow()
    {
        return _dataFlow;
    }

    @Override
    public void setDataFlow(DataFlow dataFlow)
    {
        _dataFlow = dataFlow;
    }

    public void send(String data)
    {
        logger.log(Level.FINE, "SimpleDataSink.send: data = " + data);

        increaseCount();

        _sentHistory.add(data);
    }

    public List<String> getSentHistory()
    {
        return _sentHistory;
    }

    @PostCreated
    public void setup()
    {
        if (_dataFlowNodeState != null)
            _dataFlowNodeState.setState(new Integer(0));
        else
            logger.log(Level.WARNING, "SimpleDataSink.setup: no data flow node state available");
    }

    public int getCount()
    {
        if (_dataFlowNodeState != null)
        {
            Serializable state = _dataFlowNodeState.getState();
            if ((state != null) && (state instanceof Integer))
                return Integer.valueOf(((Integer) state)).intValue();
            else if (state == null)
                logger.log(Level.WARNING, "SimpleDataSink.getCount: no data flow state");
            else
                logger.log(Level.WARNING, "SimpleDataSink.getCount: unexpected data flow node state class: " + state.getClass());
        }
        else
            logger.log(Level.WARNING, "SimpleDataSink.getCount: no data flow node state available");
        
        return -1;
    }
    
    private void increaseCount()
    {
        if (_dataFlowNodeState != null)
        {
            Serializable state = _dataFlowNodeState.getState();
            if ((state != null) && (state instanceof Integer))
                _dataFlowNodeState.setState(Integer.valueOf(((Integer) state)).intValue() + 1);
            else if (state == null)
                logger.log(Level.WARNING, "SimpleDataSink.increaseCount: no data flow state");
            else
                logger.log(Level.WARNING, "SimpleDataSink.increaseCount: unexpected data flow node state class: " + state.getClass());
        }
        else
            logger.log(Level.WARNING, "SimpleDataSink.increaseCount: no data flow node state available");
    }

    @Override
    public Collection<Class<?>> getDataConsumerDataClasses()
    {
        Set<Class<?>> dataConsumerDataClasses = new HashSet<Class<?>>();

        dataConsumerDataClasses.add(String.class);
        
        return dataConsumerDataClasses;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> DataConsumer<T> getDataConsumer(Class<T> dataClass)
    {
        if (dataClass == String.class)
            return (DataConsumer<T>) _dataConsumer;
        else
            return null;
    }

    private List<String> _sentHistory;
    
    private String               _name;
    private Map<String, String>  _properties;
    private DataFlow             _dataFlow;
    @DataFlowNodeStateInjection
    DataFlowNodeState            _dataFlowNodeState;
    @DataConsumerInjection(methodName="send")
    private DataConsumer<String> _dataConsumer;
}
