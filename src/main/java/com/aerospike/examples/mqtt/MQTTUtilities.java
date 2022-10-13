package com.aerospike.examples.mqtt;

import io.github.aerospike_examples.timeseries.DataPoint;

public class MQTTUtilities {
    public static final String MQTT_MESSAGE_FORMAT =
            "%s" + Constants.MQTT_MESSAGE_DELIMITER + "%d" + Constants.MQTT_MESSAGE_DELIMITER + "%."
                    + Constants.DECIMAL_PLACES_PRECISION_FOR_MQTT + "f";

    public static String encodeForMQTT(String timeSeriesName, DataPoint dataPoint){
        return String.format(MQTT_MESSAGE_FORMAT,timeSeriesName,dataPoint.getTimestamp(),dataPoint.getValue());
    }

    public static String timeSeriesNameFromMQTTMessage(String mqttMessage) throws NumberFormatException{
        return splitMQTTMessageIntoParts(mqttMessage)[0];
    }

    public static DataPoint dataPointFromMQTTMessage(String mqttMessage){
        String[] mqttMessageParts = splitMQTTMessageIntoParts(mqttMessage);
        return new DataPoint(Long.parseLong(mqttMessageParts[1]),Double.parseDouble(mqttMessageParts[2]));

    }

    private static String[] splitMQTTMessageIntoParts(String mqttMessage){
        return mqttMessage.split(Constants.MQTT_MESSAGE_DELIMITER);
    }

}
