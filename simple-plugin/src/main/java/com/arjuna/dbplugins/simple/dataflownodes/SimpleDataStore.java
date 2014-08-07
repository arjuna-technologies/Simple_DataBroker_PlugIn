/*
 * Copyright (c) 2014, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.simple.dataflownodes;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.risbic.intraconnect.basic.BasicDataConsumer;
import org.risbic.intraconnect.basic.BasicDataProvider;
import com.arjuna.databroker.data.DataConsumer;
import com.arjuna.databroker.data.DataProvider;
import com.arjuna.databroker.data.DataStore;

public class SimpleDataStore implements DataStore
{
    private static final Logger logger = Logger.getLogger(SimpleDataStore.class.getName());

    public static final String REMOVEKEY_PROPERTYNAME = "Remove Key";

    public SimpleDataStore(String name, Map<String, String> properties)
    {
        logger.log(Level.INFO, "SimpleDataStore: " + name + ", " + properties);

        _name          = name;
        _properties    = properties;

        _dataConsumer = new BasicDataConsumer<String>(this, "store", String.class);
        _dataProvider = new BasicDataProvider<String>(this);
    }

    @Override
    public String getName()
    {
        return _name;
    }

    @Override
    public Map<String, String> getProperties()
    {
        return Collections.unmodifiableMap(_properties);
    }

    public void store(String data)
    {
        logger.log(Level.INFO, "SimpleDataStore.store: data = " + data);
    }

    public void dummyQueryReport(String data)
    {
        logger.log(Level.INFO, "SimpleDataSource.dummyQueryReport: " + data);

        _dataProvider.produce(data);
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
