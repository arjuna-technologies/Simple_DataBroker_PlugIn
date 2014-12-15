/*
 * Copyright (c) 2014, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.simple.dataflownodes;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import com.arjuna.databroker.data.DataConsumer;
import com.arjuna.databroker.data.DataFlow;
import com.arjuna.databroker.data.DataFlowNodeState;
import com.arjuna.databroker.data.DataProvider;
import com.arjuna.databroker.data.DataStore;
import com.arjuna.databroker.data.jee.annotation.DataConsumerInjection;
import com.arjuna.databroker.data.jee.annotation.DataFlowNodeStateInjection;
import com.arjuna.databroker.data.jee.annotation.DataProviderInjection;
import com.arjuna.databroker.data.jee.annotation.PostCreated;

public class SimpleDataStore implements DataStore
{
    private static final Logger logger = Logger.getLogger(SimpleDataStore.class.getName());

    public SimpleDataStore(String name, Map<String, String> properties)
    {
        logger.log(Level.FINE, "SimpleDataStore: " + name + ", " + properties);

        _name          = name;
        _properties    = properties;
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

    public void store(String data)
    {
        logger.log(Level.FINE, "SimpleDataStore.store: data = " + data);
    }

    public void dummyQueryReport(String data)
    {
        logger.log(Level.FINE, "SimpleDataStore.dummyQueryReport: " + data);

        increaseCount();

        _dataProvider.produce(data);
    }

    @PostCreated
    public void setup()
    {
        if (_dataFlowNodeState != null)
            _dataFlowNodeState.setState(new Integer(0));
        else
            logger.log(Level.WARNING, "SimpleDataStore.setup: no data flow node state available");
    }

    public int getCount()
    {
        if (_dataFlowNodeState != null)
        {
            Serializable state = _dataFlowNodeState.getState();
            if ((state != null) && (state instanceof Integer))
                return Integer.valueOf(((Integer) state)).intValue();
            else if (state == null)
                logger.log(Level.WARNING, "SimpleDataStore.getCount: no data flow state");
            else
                logger.log(Level.WARNING, "SimpleDataStore.getCount: unexpected data flow node state class: " + state.getClass());
        }
        else
            logger.log(Level.WARNING, "SimpleDataStore.getCount: no data flow node state available");
        
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
                logger.log(Level.WARNING, "SimpleDataStore.increaseCount: no data flow state");
            else
                logger.log(Level.WARNING, "SimpleDataStore.increaseCount: unexpected data flow node state class: " + state.getClass());
        }
        else
            logger.log(Level.WARNING, "SimpleDataStore.increaseCount: no data flow node state available");
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

    @Override
    public Collection<Class<?>> getDataProviderDataClasses()
    {
        Set<Class<?>> dataProviderDataClasses = new HashSet<Class<?>>();

        dataProviderDataClasses.add(String.class);
        
        return dataProviderDataClasses;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> DataProvider<T> getDataProvider(Class<T> dataClass)
    {
        if (dataClass == String.class)
            return (DataProvider<T>) _dataProvider;
        else
            return null;
    }

    private String               _name;
    private Map<String, String>  _properties;
    private DataFlow             _dataFlow;
    @DataFlowNodeStateInjection
    DataFlowNodeState            _dataFlowNodeState;
    @DataConsumerInjection(methodName="store")
    private DataConsumer<String> _dataConsumer;
    @DataProviderInjection
    private DataProvider<String> _dataProvider;
}
