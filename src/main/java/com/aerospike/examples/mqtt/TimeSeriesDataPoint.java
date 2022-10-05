package com.aerospike.examples.mqtt;

import java.util.Date;

public class TimeSeriesDataPoint {
    private final String timeSeriesName;
    private final long timestamp;
    private final double value;

    private static final String MESSAGE_DELIMITER = String.valueOf(Constants.MQTT_MESSAGE_DELIMITER);

    public TimeSeriesDataPoint(String timeSeriesName, Date timestamp, double value){
        this(timeSeriesName,timestamp.getTime(),value);
    }

    public TimeSeriesDataPoint(String timeSeriesName, long timestamp, double value){
        this.timeSeriesName = timeSeriesName;
        this.timestamp = timestamp;
        this.value = value;
    }

    public String getTimeSeriesName() {
        return timeSeriesName;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public double getValue() {
        return value;
    }

    public String encodeForMQTT(){
        String formatString = "%s" + MESSAGE_DELIMITER + "%d" + MESSAGE_DELIMITER + "%."
                + Integer.toString(Constants.DECIMAL_PLACES_PRECISION_FOR_MQTT) + "f";
        return String.format(formatString,timeSeriesName,timestamp,value);
    }

    public static TimeSeriesDataPoint decodeFromMQTT(String message) throws NumberFormatException{
        String[] messageParts = message.split(MESSAGE_DELIMITER);
        return new TimeSeriesDataPoint(messageParts[0],Long.valueOf(messageParts[1]),Double.valueOf(messageParts[2]));
    }
}
