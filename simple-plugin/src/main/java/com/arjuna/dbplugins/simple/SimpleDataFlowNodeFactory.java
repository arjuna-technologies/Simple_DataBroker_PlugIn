/*
 * Copyright (c) 2014-2015, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.simple;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.arjuna.databroker.data.DataFlowNode;
import com.arjuna.databroker.data.DataFlowNodeFactory;
import com.arjuna.databroker.data.DataProcessor;
import com.arjuna.databroker.data.DataService;
import com.arjuna.databroker.data.DataSink;
import com.arjuna.databroker.data.DataSource;
import com.arjuna.databroker.data.DataStore;
import com.arjuna.databroker.data.InvalidClassException;
import com.arjuna.databroker.data.InvalidMetaPropertyException;
import com.arjuna.databroker.data.InvalidNameException;
import com.arjuna.databroker.data.InvalidPropertyException;
import com.arjuna.databroker.data.MissingMetaPropertyException;
import com.arjuna.databroker.data.MissingPropertyException;
import com.arjuna.dbplugins.simple.dataflownodes.SimpleDataProcessor;
import com.arjuna.dbplugins.simple.dataflownodes.SimpleDataService;
import com.arjuna.dbplugins.simple.dataflownodes.SimpleDataSink;
import com.arjuna.dbplugins.simple.dataflownodes.SimpleDataSource;
import com.arjuna.dbplugins.simple.dataflownodes.SimpleDataStore;

public class SimpleDataFlowNodeFactory implements DataFlowNodeFactory
{
    public SimpleDataFlowNodeFactory(String name, Map<String, String> properties)
    {
        _name       = name;
        _properties = properties;
    }

    @Override
    public String getName()
    {
        return _name;
    }

    @Override
    public Map<String, String> getProperties()
    {
        return _properties;
    }

    @Override
    public List<Class<? extends DataFlowNode>> getClasses()
    {
        List<Class<? extends DataFlowNode>> classes = new LinkedList<Class<? extends DataFlowNode>>();
        
        classes.add(DataSource.class);
        classes.add(DataProcessor.class);
        classes.add(DataSink.class);
        classes.add(DataService.class);
        classes.add(DataStore.class);
        
        return classes;
    }

    @Override
    public <T extends DataFlowNode> List<String> getMetaPropertyNames(Class<T> dataFlowNodeClass)
    {
        return Collections.emptyList();
    }

    @Override
    public <T extends DataFlowNode> List<String> getPropertyNames(Class<T> dataFlowNodeClass, Map<String, String> metaProperties)
        throws InvalidClassException, InvalidMetaPropertyException, MissingMetaPropertyException
    {
        return Collections.emptyList();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends DataFlowNode> T createDataFlowNode(String name, Class<T> dataFlowNodeClass, Map<String, String> metaProperties, Map<String, String> properties)
        throws InvalidNameException, InvalidPropertyException, MissingPropertyException
    {
        if (dataFlowNodeClass.isAssignableFrom(SimpleDataSource.class))
            return (T) new SimpleDataSource(name, properties);
        else if (dataFlowNodeClass.isAssignableFrom(SimpleDataProcessor.class))
            return (T) new SimpleDataProcessor(name, properties);
        else if (dataFlowNodeClass.isAssignableFrom(SimpleDataSink.class))
            return (T) new SimpleDataSink(name, properties);
        else if (dataFlowNodeClass.isAssignableFrom(SimpleDataService.class))
            return (T) new SimpleDataService(name, properties);
        else if (dataFlowNodeClass.isAssignableFrom(SimpleDataStore.class))
            return (T) new SimpleDataStore(name, properties);
        else
            return null;
    }

    private String              _name;
    private Map<String, String> _properties;
}
