package com.aerospike.examples.mqtt;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.policy.InfoPolicy;
import io.github.aerospike_examples.timeseries.TimeSeriesClient;
import io.github.aerospike_examples.timeseries.TimeSeriesInfo;
import io.github.aerospike_examples.timeseries.util.Utilities;
import org.eclipse.paho.client.mqttv3.*;

import java.util.Date;
import java.util.Random;
import java.util.UUID;

/**
 * Class for demonstrating way in which time series data received via MQTT
 * can be persisted in the Aerospike database
 *
 * We simulate a sensor whose values are read periodically
 * These values are sent to an MQTT topic as data points in a time series
 *
 * A subscriber watches the topic and stores the time series data in Aerospike
 */
public class MQTTPersistenceDemo {
    /**
     * URL for the MQTT broker we will be using
     */
    public static final String MQTT_BROKER_URL = "tcp://test.mosquitto.org:1883";

    /**
     * Name of the MQTT topic we will publish and subscribe to
     */
    public static final String MQTT_TOPIC_NAME = String.format("/MQTT-Demo-Topic-%06d",new Random().nextInt(1000000));

    /**
     * ID to use when connecting to MQTT server as a subscriber
     */
    public static final String MQTT_SUBSCRIBER_ID = UUID.randomUUID().toString();

    /**
     * ID to use when connecting to MQTT server as a publisher
     */
    public static final String MQTT_PUBLISHER_ID = UUID.randomUUID().toString();

    // Simulation parameters
    /**
     * Name of the sensor whose values are being simulated
     */
    public static final String SENSOR_NAME = "Engine-001-RPM-Sensor";

    /**
     * When sensor observations are simulated, what should be the approximate period between observations
     */
    public static final long INTERVAL_BETWEEN_OBSERVATIONS_MS = 60 * 60 * Constants.MILLISECONDS_IN_SECOND;

    /**
     * The above period can be subjected to random variation with a certain specified limit
     * This parameter specifies that limit
     */
    public static final int OBSERVATION_INTERVAL_VARIATION_PCT = 5;

    /**
     * The average daily drift, in percentage terms, that should be seen for the sensor value simulation
     */
    public static final int DAILY_DRIFT_PCT = 2;

    /**
     * The average daily volatility, in percentage terms, that should be seen for the sensor value simulation
     */
    public static final int DAILY_VOLATILITY_PCT = 10;

    /**
     * The initial value to be used when commencing the simulation
     */
    public static final int INITIAL_VALUE = 10000;

    /**
     * The start date/time to be used when executing the simulation
     */
    public static final Date START_DATE_TIME = new Date(Utilities.getTruncatedTimestamp(System.currentTimeMillis()));

    /**
     * The parameter INTERVAL_BETWEEN_OBSERVATIONS_MS determines the average time between data points produced by the simulation
     * TIME_BETWEEN_SIMULATOR_UPDATES_MS is the actual time between the samples
     * So INTERVAL_BETWEEN_OBSERVATIONS_MS might equal 1 hour, but we retrieve this data every 100ms
     */
    public static final int TIME_BETWEEN_SIMULATOR_UPDATES_MS = 100;

    /**
     * Number of iterations to execute during the simulation
     */
    public static final int SIMULATOR_ITERATIONS = 10;

    // Aerospike parameters
    /**
     * Aerospike host address
     */
    public static final String AEROSPIKE_SEED_HOST = "127.0.0.1";

    /**
     * Aerospike port
     */
    public static final int AEROSPIKE_SERVICE_PORT = 3000;

    /**
     * Aerospike namespace
     */
    public static final String AEROSPIKE_NAMESPACE = "test";

    /**
     * Entry point for the MQTT Demo class
     * @param args - required for main signature
     * @throws MqttException - can be thrown by MQTT accesses
     * @throws InterruptedException - can be thrown while waiting on threads
     */
    public static void main(String[] args) throws MqttException, InterruptedException, MQTTDemoException {
        // Preparation - set up Aerospike client
        AerospikeClient asClient = new AerospikeClient(AEROSPIKE_SEED_HOST,AEROSPIKE_SERVICE_PORT);
        // Clear down the time series space
        asClient.truncate(new InfoPolicy(),AEROSPIKE_NAMESPACE,
                io.github.aerospike_examples.timeseries.util.Constants.DEFAULT_TIME_SERIES_SET,null);

        // Now set up a publisher
        MQTTUtilities.outputMessageWithPara(String.format("Creating a publisher connection to an MQTT broker at %s",MQTT_BROKER_URL));
        IMqttClient mqttPublisher = new MqttClient(MQTT_BROKER_URL, MQTT_PUBLISHER_ID);
        mqttPublisher.connect(standardMqttConnectOptions());

        // Set up an MQTT Subscriber connection
        MQTTUtilities.outputMessageWithPara(String.format("Creating a subscriber connection to an MQTT broker at %s",MQTT_BROKER_URL));
        IMqttClient mqttSubscriber = new MqttClient(MQTT_BROKER_URL, MQTT_SUBSCRIBER_ID);
        mqttSubscriber.connect(standardMqttConnectOptions());

        // Set up an Aerospike Time Series Client
        MQTTUtilities.outputMessageWithPara("Creating an Aerospike Time Series Client object");
        TimeSeriesClient asTimeSeriesClient = new TimeSeriesClient(asClient,AEROSPIKE_NAMESPACE);

        if(asTimeSeriesClient.dataPointCount(SENSOR_NAME) == 0) {
            MQTTUtilities.outputMessageWithPara(
                    String.format("Number of time series points in Aerospike for series %s is zero - any points later found will have been added via MQTT",
                            SENSOR_NAME, asTimeSeriesClient.dataPointCount(SENSOR_NAME)));
        }
        else{
            mqttPublisher.disconnect();
            mqttSubscriber.disconnect();
            throw new MQTTDemoException(
                    String.format(
                            "Number of time series points found for series %s is non-zero - needs to be zero for demo to start. Investigate",
                            SENSOR_NAME)
            );
        }

        // Which an MQTT message listener can use to persist the MQTT data
        MQTTUtilities.outputMessageWithPara(
                "Creating an MQTT Message Listener. This listener will write received data to Aerospike via the time series client");
        IMqttMessageListener mqttDataListener = new MQTTDataPersister(asTimeSeriesClient);
        // Subscribe the listener to the topic
        MQTTUtilities.outputMessage(String.format("Subscribe the listener to the MQTT topic %s",MQTT_TOPIC_NAME));
        MQTTUtilities.outputMessageWithPara("Note topic name contains a random integer to avoid demo concurrency issues ");
        mqttSubscriber.subscribe(MQTT_TOPIC_NAME, mqttDataListener);

        // We will be publishing to a topic - get an object to represent that
        MqttTopic mqttTopic = mqttPublisher.getTopic(MQTT_TOPIC_NAME);

        // Set up a simulation object
        MQTTUtilities.outputMessageWithPara("Create a time series simulator object");
        ITimeSeriesSimulator timeSeriesSimulator = new TimeSeriesSimulator(SENSOR_NAME,START_DATE_TIME,INITIAL_VALUE,
                INTERVAL_BETWEEN_OBSERVATIONS_MS, OBSERVATION_INTERVAL_VARIATION_PCT,DAILY_DRIFT_PCT,DAILY_VOLATILITY_PCT);

        // Pass it to an object that can observe it, which is runnable
        MQTTUtilities.outputMessage(
                "Pass the simulator to an observer, which will periodically sample the simulator and then write observation messages to an MQTT broker");
        MQTTUtilities.outputMessageWithPara(String.format("The observer will sample the simulator %d times before terminating",SIMULATOR_ITERATIONS));
        Runnable sensorObserver = new RunnableMQTTSensorObserver(timeSeriesSimulator, TIME_BETWEEN_SIMULATOR_UPDATES_MS,
                SIMULATOR_ITERATIONS,mqttTopic);

        // Set up a thread which runs the observer
        Thread sensorObserverThread = new Thread(sensorObserver);

        // Start it
        MQTTUtilities.outputMessageWithPara("Running the sensor observer");
        sensorObserverThread.start();

        // Wait for it to finish
        MQTTUtilities.outputMessageWithPara("Wait for the sensor observer to finish ...");
        sensorObserverThread.join();
        MQTTUtilities.outputMessageWithPara("\nSensor observer finished");

        // Tidy up
        mqttPublisher.disconnect();
        Thread.sleep(10 * TIME_BETWEEN_SIMULATOR_UPDATES_MS);
        mqttSubscriber.disconnect();

        MQTTUtilities.outputMessage(String.format("\nRetrieving data for time series %s from Aerospike database",SENSOR_NAME));
        MQTTUtilities.printTimeSeries(asTimeSeriesClient,SENSOR_NAME);

        if(asTimeSeriesClient.dataPointCount(SENSOR_NAME) == SIMULATOR_ITERATIONS) {
            MQTTUtilities.outputMessage(String.format("\nFound %d data points for time series %s in Aerospike database - count as expected",
                    asTimeSeriesClient.dataPointCount(SENSOR_NAME), SENSOR_NAME));
        }
        else{
            throw new MQTTDemoException(String.format("Found %d data points for time series %s in Aerospike database - but expected %d - ERROR!",
                    asTimeSeriesClient.dataPointCount(SENSOR_NAME), SENSOR_NAME,SIMULATOR_ITERATIONS));
        }
    }

    private static MqttConnectOptions standardMqttConnectOptions(){
        MqttConnectOptions options = new MqttConnectOptions();
        options.setAutomaticReconnect(true);
        options.setCleanSession(true);
        options.setConnectionTimeout(1000);
        return options;
    }

    private static class MQTTDemoException extends Exception{
        private MQTTDemoException(String message){super(message);}
    }
}
