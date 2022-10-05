package com.aerospike.examples.mqtt;

import com.aerospike.client.AerospikeClient;
import io.github.aerospike_examples.timeseries.DataPoint;
import io.github.aerospike_examples.timeseries.TimeSeriesClient;

import org.eclipse.paho.client.mqttv3.*;

import java.util.UUID;

public class TimeSeriesPersister
        /*implements IMqttActionListener*/{
    private final TimeSeriesClient timeSeriesClient;
    private IMqttClient timeSeriesDataSubscriber;

    public TimeSeriesPersister(AerospikeClient asClient, String asNamespace, String mqttBroker, String topic ) throws MqttException {
        timeSeriesClient = new TimeSeriesClient(asClient,asNamespace);
        String subscriberId = UUID.randomUUID().toString();
        System.out.printf("Subscribing to %s using subscriber id %s\n",mqttBroker,subscriberId);

        timeSeriesDataSubscriber = new MqttClient(mqttBroker, subscriberId);

    }

    void messageArrived(String topic, MqttMessage mqttMessage){
        String msg = new String(mqttMessage.getPayload(), Constants.DEFAULT_CHARSET);
        TimeSeriesDataPoint timeSeriesDataPoint = TimeSeriesDataPoint.decodeFromMQTT(msg);
        timeSeriesClient.put(timeSeriesDataPoint.getTimeSeriesName(),
                new DataPoint(timeSeriesDataPoint.getTimestamp(),timeSeriesDataPoint.getValue()));
    }

}
