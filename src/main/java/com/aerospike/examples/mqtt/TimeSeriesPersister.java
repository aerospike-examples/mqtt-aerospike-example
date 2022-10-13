package com.aerospike.examples.mqtt;

import com.aerospike.client.AerospikeClient;
import io.github.aerospike_examples.timeseries.DataPoint;
import io.github.aerospike_examples.timeseries.TimeSeriesClient;

import org.eclipse.paho.client.mqttv3.*;

import java.util.UUID;

public class TimeSeriesPersister implements IMqttMessageListener{
    private final TimeSeriesClient timeSeriesClient;

    public TimeSeriesPersister(AerospikeClient asClient, String asNamespace, String mqttBroker) throws MqttException {
        timeSeriesClient = new TimeSeriesClient(asClient,asNamespace);
        String subscriberId = UUID.randomUUID().toString();
        System.out.printf("Subscribing to %s using subscriber id %s\n",mqttBroker,subscriberId);
    }

    public void messageArrived(String topic, MqttMessage mqttMessage){
        String msg = new String(mqttMessage.getPayload(), Constants.DEFAULT_CHARSET);
        TimeSeriesDataPoint timeSeriesDataPoint = TimeSeriesDataPoint.decodeFromMQTT(msg);
        timeSeriesClient.put(timeSeriesDataPoint.getTimeSeriesName(),
                new DataPoint(timeSeriesDataPoint.getTimestamp(),timeSeriesDataPoint.getValue()));
    }

}
