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
    public static final String MQTT_PUBLISHER_ID = UUID.randomUUID().toString();

    // Simulation parameters
    public static final String SENSOR_NAME = "Engine-001-RPM-Sensor";
    public static final long INTERVAL_BETWEEN_OBSERVATIONS_MS = 60 * 60 * Constants.MILLISECONDS_IN_SECOND;
    public static final int OBSERVATION_INTERVAL_VARIATION_PCT = 5;
    public static final int DAILY_DRIFT_PCT = 2;
    public static final int DAILY_VOLATILITY_PCT = 10;
    public static final int INITIAL_VALUE = 10000;
    public static final Date START_DATE_TIME = new Date(Utilities.getTruncatedTimestamp(System.currentTimeMillis()));

    public static final int TIME_BETWEEN_SIMULATOR_UPDATES_MS = 100;
    public static final int SIMULATOR_ITERATIONS = 10;

    // Aerospike parameters
    public static final String AEROSPIKE_SEED_HOST = "127.0.0.1";
    public static final int AEROSPIKE_SERVICE_PORT = 3000;
    public static final String AEROSPIKE_NAMESPACE = "test";

    public static void main(String[] args) throws MqttException, InterruptedException {
        // Preparation - set up Aerospike client
        AerospikeClient asClient = new AerospikeClient(AEROSPIKE_SEED_HOST,AEROSPIKE_SERVICE_PORT);
        // Clear down the time series space
        asClient.truncate(new InfoPolicy(),AEROSPIKE_NAMESPACE,
                io.github.aerospike_examples.timeseries.util.Constants.DEFAULT_TIME_SERIES_SET,null);

        // Set up an MQTT Subscriber connection
        IMqttClient mqttSubscriber = new MqttClient(MQTT_BROKER_URL, MQTT_SUBSCRIBER_ID);
        mqttSubscriber.connect(standardMqttConnectOptions());

        // Set up an Aerospike Time Series Client
        TimeSeriesClient asTimeSeriesClient = new TimeSeriesClient(asClient,AEROSPIKE_NAMESPACE);
        // Which an MQTT message listener can use to persist the MQTT data
        IMqttMessageListener mqttDataListener = new MQTTDataPersister(asTimeSeriesClient);
        // Subscribe the listener to the topic
        mqttSubscriber.subscribe(MQTT_TOPIC_NAME, mqttDataListener);

        // Now set up a publisher
        IMqttClient mqttPublisher = new MqttClient(MQTT_BROKER_URL, MQTT_PUBLISHER_ID);
        mqttPublisher.connect(standardMqttConnectOptions());

        // We will be publishing to a topic - get an object to represent that
        MqttTopic mqttTopic = mqttPublisher.getTopic(MQTT_TOPIC_NAME);

        // Set up a simulation object
        TimeSeriesSimulator timeSeriesSimulator = new TimeSeriesSimulator(SENSOR_NAME,START_DATE_TIME,INITIAL_VALUE,
                INTERVAL_BETWEEN_OBSERVATIONS_MS, OBSERVATION_INTERVAL_VARIATION_PCT,DAILY_DRIFT_PCT,DAILY_VOLATILITY_PCT);

        // Pass it to an object that can observe it, which is runnable
        Runnable sensorObserver = new RunnableMQTTSensorObserver(timeSeriesSimulator, TIME_BETWEEN_SIMULATOR_UPDATES_MS,
                SIMULATOR_ITERATIONS,mqttTopic);

        // Set up a thread which runs the observer
        Thread sensorObserverThread = new Thread(sensorObserver);

        // Start it
        sensorObserverThread.start();

        // Wait for it to finish
        sensorObserverThread.join();

        // Tidy up
        mqttPublisher.disconnect();
        Thread.sleep(10 * TIME_BETWEEN_SIMULATOR_UPDATES_MS);
        mqttSubscriber.disconnect();
        System.out.println(asTimeSeriesClient.getPoints(SENSOR_NAME,START_DATE_TIME,new Date()).length);
    }

    private static MqttConnectOptions standardMqttConnectOptions(){
        MqttConnectOptions options = new MqttConnectOptions();
        options.setAutomaticReconnect(true);
        options.setCleanSession(true);
        options.setConnectionTimeout(1000);
        return options;
    }
}
