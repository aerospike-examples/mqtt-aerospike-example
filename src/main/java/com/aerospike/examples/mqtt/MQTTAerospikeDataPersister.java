package com.aerospike.examples.mqtt;

import io.github.aerospike_examples.timeseries.DataPoint;
import io.github.aerospike_examples.timeseries.TimeSeriesClient;

import org.eclipse.paho.client.mqttv3.*;

/**
 * Implements the IMqttListener interface
 * When a message arrives, store it as a time series point
 */
public class MQTTAerospikeDataPersister implements IMqttMessageListener{
    private final TimeSeriesClient timeSeriesClient;

    /**
     * Constructor - takes a time series client
     * @param timeSeriesClient - time series client
     */
    public MQTTAerospikeDataPersister(TimeSeriesClient timeSeriesClient) {
        this.timeSeriesClient = timeSeriesClient;
    }

    /**
     * Parse the message, extract time series name and DataPoint
     * and save to the database
     * @param topic name of the topic on the message was published to
     * @param mqttMessage the actual message.
     */
    public void messageArrived(String topic, MqttMessage mqttMessage){
        String mqttMessageAsString = new String(mqttMessage.getPayload(), Constants.MQTT_DEFAULT_CHARSET);
        String timeSeriesName = MQTTUtilities.timeSeriesNameFromMQTTMessage(mqttMessageAsString);
        DataPoint dataPoint = MQTTUtilities.dataPointFromMQTTMessage(mqttMessageAsString);
        timeSeriesClient.put(timeSeriesName,dataPoint);
        MQTTUtilities.outputMessage(String.format("Data point for %s received from %s. Wrote (%s,%.6f) to Aerospike for %s",
                timeSeriesName,topic,
                MQTTUtilities.dataPointDateToString(dataPoint),
                dataPoint.getValue(),timeSeriesName));
    }

}
