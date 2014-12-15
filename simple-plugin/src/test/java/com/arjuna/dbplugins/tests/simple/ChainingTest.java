/*
 * Copyright (c) 2014, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.tests.simple;

import java.util.Collections;

import org.junit.Test;

import static org.junit.Assert.*;

import com.arjuna.databroker.data.connector.ObservableDataProvider;
import com.arjuna.databroker.data.connector.ObserverDataConsumer;
import com.arjuna.databroker.data.jee.DataFlowNodeLifeCycleControl;
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

        DataFlowNodeLifeCycleControl.processCreatedDataFlowNode(simpleDataSource, null);
        DataFlowNodeLifeCycleControl.processCreatedDataFlowNode(simpleDataProcessor, null);
        DataFlowNodeLifeCycleControl.processCreatedDataFlowNode(simpleDataSink, null);

        ((ObservableDataProvider<String>) simpleDataSource.getDataProvider(String.class)).addDataConsumer((ObserverDataConsumer<String>) simpleDataProcessor.getDataConsumer(String.class));
        ((ObservableDataProvider<String>) simpleDataProcessor.getDataProvider(String.class)).addDataConsumer((ObserverDataConsumer<String>) simpleDataSink.getDataConsumer(String.class));

        assertEquals("Simple DataSource count", 0, simpleDataSource.getCount());
        assertEquals("Simple DataProcessor count", 0, simpleDataProcessor.getCount());
        assertEquals("Simple DataSink count", 0, simpleDataSink.getCount());

        simpleDataSource.dummyGetData("Data Bundle 1");
        simpleDataSource.dummyGetData("Data Bundle 2");
        simpleDataSource.dummyGetData("Data Bundle 3");
        simpleDataSource.dummyGetData("Data Bundle 4");

        DataFlowNodeLifeCycleControl.removeDataFlowNode(simpleDataSource);
        DataFlowNodeLifeCycleControl.removeDataFlowNode(simpleDataProcessor);
        DataFlowNodeLifeCycleControl.removeDataFlowNode(simpleDataSink);

        assertArrayEquals("Unexpected history at DataSink", new String[]{"[Data Bundle 1]", "[Data Bundle 2]", "[Data Bundle 3]", "[Data Bundle 4]"}, simpleDataSink.getSentHistory().toArray());
        
        assertEquals("Simple DataSource count", 4, simpleDataSource.getCount());
        assertEquals("Simple DataProcessor count", 4, simpleDataProcessor.getCount());
        assertEquals("Simple DataSink count", 4, simpleDataSink.getCount());
    }

    @Test
    public void fullConnectedChain()
    {
        SimpleDataSource    simpleDataSource    = new SimpleDataSource("Simple Data Source", Collections.<String, String>emptyMap());
        SimpleDataProcessor simpleDataProcessor = new SimpleDataProcessor("Simple Data Processor", Collections.<String, String>emptyMap());
        SimpleDataSink      simpleDataSink1     = new SimpleDataSink("Simple Data Sink 1", Collections.<String, String>emptyMap());
        SimpleDataSink      simpleDataSink2     = new SimpleDataSink("Simple Data Sink 2", Collections.<String, String>emptyMap());
        SimpleDataSink      simpleDataSink3     = new SimpleDataSink("Simple Data Sink 3", Collections.<String, String>emptyMap());
        SimpleDataService   simpleDataService   = new SimpleDataService("Simple Data Service", Collections.<String, String>emptyMap());
        SimpleDataStore     simpleDataStore     = new SimpleDataStore("Simple Data Store", Collections.<String, String>emptyMap());

        DataFlowNodeLifeCycleControl.processCreatedDataFlowNode(simpleDataSource, null);
        DataFlowNodeLifeCycleControl.processCreatedDataFlowNode(simpleDataProcessor, null);
        DataFlowNodeLifeCycleControl.processCreatedDataFlowNode(simpleDataSink1, null);
        DataFlowNodeLifeCycleControl.processCreatedDataFlowNode(simpleDataSink2, null);
        DataFlowNodeLifeCycleControl.processCreatedDataFlowNode(simpleDataSink3, null);
        DataFlowNodeLifeCycleControl.processCreatedDataFlowNode(simpleDataService, null);
        DataFlowNodeLifeCycleControl.processCreatedDataFlowNode(simpleDataStore, null);

        ((ObservableDataProvider<String>) simpleDataSource.getDataProvider(String.class)).addDataConsumer((ObserverDataConsumer<String>) simpleDataService.getDataConsumer(String.class));
        ((ObservableDataProvider<String>) simpleDataService.getDataProvider(String.class)).addDataConsumer((ObserverDataConsumer<String>) simpleDataSink1.getDataConsumer(String.class));

        ((ObservableDataProvider<String>) simpleDataSource.getDataProvider(String.class)).addDataConsumer((ObserverDataConsumer<String>) simpleDataProcessor.getDataConsumer(String.class));
        ((ObservableDataProvider<String>) simpleDataProcessor.getDataProvider(String.class)).addDataConsumer((ObserverDataConsumer<String>) simpleDataSink2.getDataConsumer(String.class));

        ((ObservableDataProvider<String>) simpleDataSource.getDataProvider(String.class)).addDataConsumer((ObserverDataConsumer<String>) simpleDataService.getDataConsumer(String.class));
        ((ObservableDataProvider<String>) simpleDataStore.getDataProvider(String.class)).addDataConsumer((ObserverDataConsumer<String>) simpleDataSink3.getDataConsumer(String.class));

        assertEquals("Simple DataSource count", 0, simpleDataSource.getCount());
        assertEquals("Simple DataProcessor count", 0, simpleDataProcessor.getCount());
        assertEquals("Simple DataSink 1 count", 0, simpleDataSink1.getCount());
        assertEquals("Simple DataSink 2 count", 0, simpleDataSink2.getCount());
        assertEquals("Simple DataSink 3 count", 0, simpleDataSink3.getCount());
        assertEquals("Simple DataService count", 0, simpleDataService.getCount());
        assertEquals("Simple DataStore count", 0, simpleDataStore.getCount());

        simpleDataService.dummyImport("Import Bundle 1");
        simpleDataSource.dummyGetData("Data Bundle 1");
        simpleDataStore.dummyQueryReport("Report Bundle 1");
        simpleDataService.dummyImport("Import Bundle 2");
        simpleDataSource.dummyGetData("Data Bundle 2");
        simpleDataStore.dummyQueryReport("Report Bundle 2");

        DataFlowNodeLifeCycleControl.removeDataFlowNode(simpleDataSource);
        DataFlowNodeLifeCycleControl.removeDataFlowNode(simpleDataProcessor);
        DataFlowNodeLifeCycleControl.removeDataFlowNode(simpleDataSink1);
        DataFlowNodeLifeCycleControl.removeDataFlowNode(simpleDataSink2);
        DataFlowNodeLifeCycleControl.removeDataFlowNode(simpleDataSink3);
        DataFlowNodeLifeCycleControl.removeDataFlowNode(simpleDataService);
        DataFlowNodeLifeCycleControl.removeDataFlowNode(simpleDataStore);

        assertArrayEquals("Unexpected history at DataSink 1", new String[]{"Import Bundle 1", "Import Bundle 2"}, simpleDataSink1.getSentHistory().toArray());
        assertArrayEquals("Unexpected history at DataSink 2", new String[]{"[Data Bundle 1]", "[Data Bundle 2]"}, simpleDataSink2.getSentHistory().toArray());
        assertArrayEquals("Unexpected history at DataSink 3", new String[]{"Report Bundle 1", "Report Bundle 2"}, simpleDataSink3.getSentHistory().toArray());

        assertEquals("Simple DataSource count", 2, simpleDataSource.getCount());
        assertEquals("Simple DataProcessor count", 2, simpleDataProcessor.getCount());
        assertEquals("Simple DataSink 1 count", 2, simpleDataSink1.getCount());
        assertEquals("Simple DataSink 2 count", 2, simpleDataSink2.getCount());
        assertEquals("Simple DataSink 3 count", 2, simpleDataSink3.getCount());
        assertEquals("Simple DataService count", 2, simpleDataService.getCount());
        assertEquals("Simple DataStore count", 2, simpleDataStore.getCount());
    }
}
