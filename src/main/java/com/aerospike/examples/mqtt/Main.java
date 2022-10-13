package com.aerospike.examples.mqtt;

import com.aerospike.client.AerospikeClient;
import io.github.aerospike_examples.timeseries.util.Utilities;
import org.eclipse.paho.client.mqttv3.*;

import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.UUID;

public class Main {
    public static void main(String[] args) throws MqttException, InterruptedException {
        String subscriberId = UUID.randomUUID().toString();

        String mqttBroker = "tcp://test.mosquitto.org:1883";
        String sensorTopic = "/engineTemperature";


        MqttConnectOptions options = new MqttConnectOptions();
        options.setAutomaticReconnect(true);
        options.setCleanSession(true);
        options.setConnectionTimeout(1000);

        IMqttClient subscriber = new MqttClient(mqttBroker, subscriberId);

        subscriber.connect(options);

//        subscriber.subscribe(sensorTopic,(topic,msg) -> {
//            byte[] payload = msg.getPayload();
//            System.out.printf("Message received : %s\n",new String(payload, StandardCharsets.UTF_8));
//        });

        AerospikeClient asClient = new AerospikeClient("127.0.0.1",3000);
        String asNamespace = "test";

        TimeSeriesPersister timeSeriesPersister = new TimeSeriesPersister(asClient, asNamespace, mqttBroker);
        subscriber.subscribe(sensorTopic,timeSeriesPersister);

        // Seed for randomiser so we can acheive deterministic results in testing
        long RANDOM_SEED = 6760187239798559903L;

        double initialValue = 12;

        long timeIncrementSeconds = 60;
        long observationIntervalMillSeconds = timeIncrementSeconds * Constants.MILLISECONDS_IN_SECOND;
        double observationIntervalVariabilityPct = 5;
        double dailyDriftPct = 2;
        double dailyVariancePct = 7;

        // Today's date, starting at midnight
        Date startDateTime = new Date(Utilities.getTruncatedTimestamp(System.currentTimeMillis()));

        TimeSeriesSimulator timeSeriesSimulator = new TimeSeriesSimulator(startDateTime,initialValue,observationIntervalMillSeconds,
                observationIntervalVariabilityPct,dailyDriftPct,dailyVariancePct,RANDOM_SEED);

        String publisherId = UUID.randomUUID().toString();

        IMqttClient publisher = new MqttClient(mqttBroker, publisherId);


        publisher.connect(options);

        MqttTopic mqttTopic = publisher.getTopic(sensorTopic);

        SensorReaderRunnable sensorReader = new SensorReaderRunnable("Sensor-001",timeSeriesSimulator,100,10,mqttTopic);
        Thread t = new Thread(sensorReader);
        t.start();
        t.join();
        System.out.println("Done waiting");
        publisher.disconnect();

    }
}
