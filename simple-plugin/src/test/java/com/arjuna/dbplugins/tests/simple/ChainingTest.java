/*
 * Copyright (c) 2014, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.tests.simple;

import java.util.Collection;
import java.util.Collections;
import org.junit.Test;
import com.arjuna.dbplugins.simple.dataflownodes.SimpleDataProcessor;
import com.arjuna.dbplugins.simple.dataflownodes.SimpleDataService;
import com.arjuna.dbplugins.simple.dataflownodes.SimpleDataSink;
import com.arjuna.dbplugins.simple.dataflownodes.SimpleDataSource;
import com.arjuna.dbplugins.simple.dataflownodes.SimpleDataStore;

public class ChainingTest
{
    @Test
    public void simplestChain()
    {
        SimpleDataSource    simpleDataSource    = new SimpleDataSource("Simple Data Source", Collections.<String, String>emptyMap());
        SimpleDataProcessor simpleDataProcessor = new SimpleDataProcessor("Simple Data Processor", Collections.<String, String>emptyMap());
        SimpleDataSink      simpleDataSink      = new SimpleDataSink("Simple Data Sink", Collections.<String, String>emptyMap());

        simpleDataSource.getDataProvider(String.class).addDataConsumer(simpleDataProcessor.getDataConsumer(String.class));
        simpleDataProcessor.getDataProvider(String.class).addDataConsumer(simpleDataSink.getDataConsumer(String.class));

        simpleDataSource.receive("Data Bundle 1");
        simpleDataSource.receive("Data Bundle 2");
        simpleDataSource.receive("Data Bundle 3");
        simpleDataSource.receive("Data Bundle 4");
    }
}
