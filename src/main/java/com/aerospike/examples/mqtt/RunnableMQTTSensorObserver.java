package com.aerospike.examples.mqtt;

import io.github.aerospike_examples.timeseries.DataPoint;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttTopic;

import java.util.Date;

import java.text.SimpleDateFormat;

/**
 * A runnable object which allows us to monitor a time series simulator
 * and publish the results to an MQTT topic for a fixed number of iterations
 * which take place at a fixed frequency
 */
public class RunnableMQTTSensorObserver implements Runnable{
    private final ITimeSeriesSimulator timeSeriesSimulator;
    private final long millisecondsBetweenObservations;
    private final long observationCount;
    private final MqttTopic publicationTopic;

    /**
     * Constructor
     * @param timeSeriesSimulator - simulator
     * @param millisecondsBetweenObservations - time between observations
     * @param observationCount - total number of observations to make
     * @param publicationTopic - topic to publish to
     */
    public RunnableMQTTSensorObserver(ITimeSeriesSimulator timeSeriesSimulator,
                                      long millisecondsBetweenObservations, long observationCount, MqttTopic publicationTopic){
        this.timeSeriesSimulator = timeSeriesSimulator;
        this.millisecondsBetweenObservations = millisecondsBetweenObservations;
        this.observationCount = observationCount;
        this.publicationTopic = publicationTopic;
    }

    public void run(){
        for(int i = 0; i< observationCount; i++){
            DataPoint dataPoint = timeSeriesSimulator.getCurrentDataPoint();

            byte[] payload = MQTTUtilities.encodeForMQTT(timeSeriesSimulator.getSimulatorName(),dataPoint).getBytes();
            try {
                MqttMessage msg = new MqttMessage(payload);
                msg.setQos(0);
                msg.setRetained(true);
                publicationTopic.publish(msg);
                MQTTUtilities.outputMessage(String.format("Sampling %s at time %s. Found value %.6f. Written to MQTT topic %s",
                        timeSeriesSimulator.getSimulatorName(),
                        MQTTUtilities.dataPointDateToString(dataPoint),
                        dataPoint.getValue(),publicationTopic.getName()));

            } catch (MqttException e) {
                throw new RuntimeException(e);
            }
            timedWait();
            timeSeriesSimulator.getNextDataPoint();
        }
    }

    private void timedWait(){
        long startTime = System.currentTimeMillis();
        while((System.currentTimeMillis() - startTime) > millisecondsBetweenObservations){
            try{
                Thread.sleep(1);
            }
            catch(InterruptedException e){
                // Do nothing
            }
        }
    }

}
