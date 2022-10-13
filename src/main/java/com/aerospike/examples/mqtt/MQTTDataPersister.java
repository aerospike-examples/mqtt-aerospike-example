package com.aerospike.examples.mqtt;

import io.github.aerospike_examples.timeseries.DataPoint;
import io.github.aerospike_examples.timeseries.TimeSeriesClient;

import org.eclipse.paho.client.mqttv3.*;

public class MQTTDataPersister implements IMqttMessageListener{
    private final TimeSeriesClient timeSeriesClient;

    public MQTTDataPersister(TimeSeriesClient timeSeriesClient) {
        this.timeSeriesClient = timeSeriesClient;
    }

    public void messageArrived(String topic, MqttMessage mqttMessage){
        String mqttMessageAsString = new String(mqttMessage.getPayload(), Constants.MQTT_DEFAULT_CHARSET);
        String timeSeriesName = MQTTUtilities.timeSeriesNameFromMQTTMessage(mqttMessageAsString);
        DataPoint dataPoint = MQTTUtilities.dataPointFromMQTTMessage(mqttMessageAsString);
        timeSeriesClient.put(timeSeriesName,dataPoint);
    }

}
