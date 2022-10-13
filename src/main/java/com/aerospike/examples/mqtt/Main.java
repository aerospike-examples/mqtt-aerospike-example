package com.aerospike.examples.mqtt;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.policy.InfoPolicy;
import io.github.aerospike_examples.timeseries.TimeSeriesClient;
import io.github.aerospike_examples.timeseries.util.Utilities;
import org.eclipse.paho.client.mqttv3.*;

import java.util.Date;
import java.util.Random;
import java.util.UUID;

public class Main {
    // MQTT Broker
    public static final String MQTT_BROKER_URL = "tcp://test.mosquitto.org:1883";
    public static final String MQTT_TOPIC_NAME = String.format("/MQTT-Demo-Topic-%06d",new Random().nextInt(1000000));
    public static final String MQTT_SUBSCRIBER_ID = UUID.randomUUID().toString();

    // Simulation parameters
    public static final long INTERVAL_BETWEEN_OBSERVATIONS_MS = 60 * 60 * Constants.MILLISECONDS_IN_SECOND;
    public static final int OBSERVATION_INTERVAL_VARIATION_PCT = 5;
    public static final int DAILY_DRIFT_PCT = 2;
    public static final int DAILY_VOLATILITY_PCT = 10;
    public static final int INITIAL_VALUE = 10000;
    public static final Date START_DATE_TIME = new Date(Utilities.getTruncatedTimestamp(System.currentTimeMillis()));

    // Aerospike parameters
    public static final String AEROSPIKE_SEED_HOST = "127.0.0.1";
    public static final int AEROSPIKE_SERVICE_PORT = 3000;
    public static final String AEROSPIKE_NAMESPACE = "test";


    public static void main(String[] args) throws MqttException, InterruptedException {
        // Preparation
        AerospikeClient asClient = new AerospikeClient(AEROSPIKE_SEED_HOST,AEROSPIKE_SERVICE_PORT);
        TimeSeriesClient asTimeSeriesClient = new TimeSeriesClient(asClient,AEROSPIKE_NAMESPACE);
        asClient.truncate(new InfoPolicy(),AEROSPIKE_NAMESPACE,asTimeSeriesClient.getTimeSeriesSet(),null);

        // Set up an MQTT Subscriber connection
        IMqttClient mqttSubscriber = new MqttClient(MQTT_BROKER_URL, MQTT_SUBSCRIBER_ID);
        mqttSubscriber.connect(standardMqttConnectOptions());

        // And subscribe to a topic on that connection
        MQTTDataPersister mqttDataPersister = new MQTTDataPersister(asTimeSeriesClient);
        mqttSubscriber.subscribe(MQTT_TOPIC_NAME, mqttDataPersister);

        TimeSeriesSimulator timeSeriesSimulator = new TimeSeriesSimulator(START_DATE_TIME,INITIAL_VALUE,INTERVAL_BETWEEN_OBSERVATIONS_MS,
                OBSERVATION_INTERVAL_VARIATION_PCT,DAILY_DRIFT_PCT,DAILY_VOLATILITY_PCT);

        String publisherId = UUID.randomUUID().toString();

        IMqttClient publisher = new MqttClient(MQTT_BROKER_URL, publisherId);

        publisher.connect(standardMqttConnectOptions());

        MqttTopic mqttTopic = publisher.getTopic(MQTT_TOPIC_NAME);

        SensorReaderRunnable sensorReader = new SensorReaderRunnable("Sensor-001",timeSeriesSimulator,100,10,mqttTopic);
        Thread t = new Thread(sensorReader);
        t.start();
        t.join();
        System.out.println("Done waiting");
        publisher.disconnect();
        Thread.sleep(1000);
        mqttSubscriber.disconnect();
        System.out.println(asTimeSeriesClient.getPoints("Sensor-001",START_DATE_TIME,new Date()).length);
    }

    private static MqttConnectOptions standardMqttConnectOptions(){
        MqttConnectOptions options = new MqttConnectOptions();
        options.setAutomaticReconnect(true);
        options.setCleanSession(true);
        options.setConnectionTimeout(1000);
        return options;
    }
}
