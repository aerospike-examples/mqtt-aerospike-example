package com.aerospike.examples.mqtt;

import io.github.aerospike_examples.timeseries.DataPoint;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttTopic;

public class RunnableMQTTSensorObserver implements Runnable{
    private final TimeSeriesSimulator timeSeriesSimulator;
    final long millisecondsBetweenObservations;
    final long readingCount;
    final MqttTopic publicationTopic;

    public RunnableMQTTSensorObserver(TimeSeriesSimulator timeSeriesSimulator,
                                      long millisecondsBetweenObservations, long observationCount, MqttTopic publicationTopic){
        this.timeSeriesSimulator = timeSeriesSimulator;
        this.millisecondsBetweenObservations = millisecondsBetweenObservations;
        this.readingCount = observationCount;
        this.publicationTopic = publicationTopic;
    }

    public void run(){
        for(int i=0;i<readingCount;i++){
            DataPoint dataPoint = timeSeriesSimulator.getCurrentDataPoint();
            byte[] payload = MQTTUtilities.encodeForMQTT(timeSeriesSimulator.getSimulatorName(),dataPoint).getBytes();
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
