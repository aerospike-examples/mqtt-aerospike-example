package com.aerospike.examples.mqtt;

import io.github.aerospike_examples.timeseries.DataPoint;
import io.github.aerospike_examples.timeseries.TimeSeriesClient;
import io.github.aerospike_examples.timeseries.TimeSeriesInfo;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Utility functions
 */
public class MQTTUtilities {
    /**
     * The format of the message we will write to the MQTT broker
     * which is sensorName:timestamp:value
     */
    public static final String MQTT_MESSAGE_FORMAT =
            "%s" + Constants.MQTT_MESSAGE_DELIMITER + "%d" + Constants.MQTT_MESSAGE_DELIMITER + "%."
                    + Constants.DECIMAL_PLACES_PRECISION_FOR_MQTT + "f";

    /**
     * Encode the time series point into the correct format to post to the MQTT broker
     * @param timeSeriesName - time series name
     * @param dataPoint - data point
     * @return message as string
     */
    public static String encodeForMQTT(String timeSeriesName, DataPoint dataPoint){
        return String.format(MQTT_MESSAGE_FORMAT,timeSeriesName,dataPoint.getTimestamp(),dataPoint.getValue());
    }

    /**
     * Extract the time series name from the received MQTT message
     * @param mqttMessage - mqttMessage as string
     * @return time series name
     */
    public static String timeSeriesNameFromMQTTMessage(String mqttMessage){
        return splitMQTTMessageIntoParts(mqttMessage)[0];
    }

    /**
     * Extract the data point from the received MQTT message
     * @param mqttMessage  - mqtt message as string
     * @return DataPoint
     */
    public static DataPoint dataPointFromMQTTMessage(String mqttMessage){
        String[] mqttMessageParts = splitMQTTMessageIntoParts(mqttMessage);
        return new DataPoint(Long.parseLong(mqttMessageParts[1]),Double.parseDouble(mqttMessageParts[2]));

    }

    /**
     * Internal helper method, breaking the MQTT message into its constituent parts
     * @param mqttMessage - MQTT message as string
     * @return messages parts as String[]
     */
    private static String[] splitMQTTMessageIntoParts(String mqttMessage){
        return mqttMessage.split(Constants.MQTT_MESSAGE_DELIMITER);
    }

    /**
     * Allow message output
     * @param message
     */
    static void outputMessage(String message){
        System.out.println(message);
    }

    /**
     * Message output with additional carriage return
     * @param message
     */
    static void outputMessageWithPara(String message){
        outputMessage(message+"\n");
    }

    static String dataPointDateToString(DataPoint dataPoint){
        String timeSeriesDateFormat = "yyyy-MM-dd HH:mm:ss.SSS";
        SimpleDateFormat dateFormatter = new SimpleDateFormat(timeSeriesDateFormat);
        return dateFormatter.format(new Date(dataPoint.getTimestamp()));
    }
    /**
     * Utility method to print out a time series
     *
     * @param timeSeriesName Name of time series to print data for
     */
    static void printTimeSeries(TimeSeriesClient timeSeriesClient, String timeSeriesName) {
        TimeSeriesInfo timeSeriesInfo = TimeSeriesInfo.getTimeSeriesDetails(timeSeriesClient, timeSeriesName);
        outputMessageWithPara(timeSeriesInfo.toString());
        DataPoint[] dataPoints = timeSeriesClient.getPoints(timeSeriesName, new Date(timeSeriesInfo.getStartDateTimestamp()),
                new Date(timeSeriesInfo.getEndDateTimestamp()));
        System.out.println("Timestamp,Value");
        for (DataPoint dataPoint : dataPoints) {
            outputMessage(String.format("%s,%.6f", dataPointDateToString(dataPoint), dataPoint.getValue()));
        }
    }

}
