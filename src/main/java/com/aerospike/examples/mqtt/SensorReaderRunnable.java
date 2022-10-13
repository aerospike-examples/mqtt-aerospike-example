package com.aerospike.examples.mqtt;

import io.github.aerospike_examples.timeseries.DataPoint;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttTopic;

public class SensorReaderRunnable implements Runnable{
    private static final String MESSAGE_DELIMITER = String.valueOf(Constants.MQTT_MESSAGE_DELIMITER);

    private String sensorName;
    private TimeSeriesSimulator timeSeriesSimulator;
    long millisecondsBetweenReading;
    long readingCount;
    MqttTopic publicationTopic;

    public SensorReaderRunnable(String sensorName, TimeSeriesSimulator timeSeriesSimulator,
                                long millisecondsBetweenReading, long readingCount, MqttTopic publicationTopic){
        this.sensorName = sensorName;
        this.timeSeriesSimulator = timeSeriesSimulator;
        this.millisecondsBetweenReading = millisecondsBetweenReading;
        this.readingCount = readingCount;
        this.publicationTopic = publicationTopic;
    }

    public void run(){
        for(int i=0;i<readingCount;i++){
            DataPoint dataPoint = timeSeriesSimulator.currentDataPoint;
            System.out.println(dataPoint);
            byte[] payload = encodeForMQTT(dataPoint).getBytes();
            try {
                MqttMessage msg = new MqttMessage(payload);
                msg.setQos(0);
                msg.setRetained(true);

                publicationTopic.publish(msg);
            } catch (MqttException e) {
                throw new RuntimeException(e);
            }
            timedWait();
            timeSeriesSimulator.getNextDataPoint();
        }
        System.out.println("Done running");

    }

    private void timedWait(){
        long startTime = System.currentTimeMillis();
        while((System.currentTimeMillis() - startTime) > millisecondsBetweenReading){
            // Do Nothing
        }
    }

    private String encodeForMQTT(DataPoint dataPoint){
        String formatString = "%s" + MESSAGE_DELIMITER + "%d" + MESSAGE_DELIMITER + "%."
                + Integer.toString(Constants.DECIMAL_PLACES_PRECISION_FOR_MQTT) + "f";
        return String.format(formatString,sensorName,dataPoint.getTimestamp(),dataPoint.getValue());
    }

}
