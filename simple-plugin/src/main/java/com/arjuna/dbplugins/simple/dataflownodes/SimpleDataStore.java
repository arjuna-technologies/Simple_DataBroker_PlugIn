/*
 * Copyright (c) 2014, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.simple.dataflownodes;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import com.arjuna.databroker.data.DataConsumer;
import com.arjuna.databroker.data.DataProvider;
import com.arjuna.databroker.data.DataStore;
import com.arjuna.dbplugins.simple.connectors.SimpleDataConsumer;
import com.arjuna.dbplugins.simple.connectors.SimpleDataProvider;

public class SimpleDataStore implements DataStore
{
    private static final Logger logger = Logger.getLogger(SimpleDataStore.class.getName());

    public static final String REMOVEKEY_PROPERTYNAME = "Remove Key";

    public SimpleDataStore(String name, Map<String, String> properties)
    {
        logger.info("SimpleDataStore: " + name + ", " + properties);

        _name          = name;
        _properties    = properties;

        _dataConsumer = new SimpleDataConsumer<String>(this, MethodUtil.getMethod(SimpleDataStore.class, "store"));
        _dataProvider = new SimpleDataProvider<String>(this);
    }

    public String getName()
    {
        return _name;
    }

    public Map<String, String> getProperties()
    {
        return Collections.unmodifiableMap(_properties);
    }

    public void store(String data)
    {
        logger.info("SimpleDataStore.store: data = " + data);
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
    private DataConsumer<String> _dataConsumer;
    private DataProvider<String> _dataProvider;
}
