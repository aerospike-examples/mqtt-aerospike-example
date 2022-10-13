package com.aerospike.examples.mqtt;

import com.aerospike.client.AerospikeClient;
import io.github.aerospike_examples.timeseries.DataPoint;
import io.github.aerospike_examples.timeseries.TimeSeriesClient;

import org.eclipse.paho.client.mqttv3.*;

public class MQTTDataPersister implements IMqttMessageListener{
    private final TimeSeriesClient timeSeriesClient;

    public MQTTDataPersister(TimeSeriesClient timeSeriesClient) throws MqttException {
        this.timeSeriesClient = timeSeriesClient;
    }

    public void messageArrived(String topic, MqttMessage mqttMessage){
        String msg = new String(mqttMessage.getPayload(), Constants.DEFAULT_CHARSET);
        TimeSeriesDataPoint timeSeriesDataPoint = TimeSeriesDataPoint.decodeFromMQTT(msg);
        timeSeriesClient.put(timeSeriesDataPoint.getTimeSeriesName(),
                new DataPoint(timeSeriesDataPoint.getTimestamp(),timeSeriesDataPoint.getValue()));
    }

}
