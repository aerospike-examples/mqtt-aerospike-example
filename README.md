# Aerospike MQTT Example for IoT

[Aerospike](https://aerospike.com/) is a high performance distributed database, particularly well suited for real time transactional processing. It is aimed at institutions and use-cases that need high throughput ( 100k tps+), with low latency (95% completion in <1ms), while managing large amounts of data (Tb+) with 100% uptime, scalability and low cost.

[MQTT](https://mqtt.org/) (MQ Telemetry Transport) is a widely used messaging protocol for the Internet of Things (IoT). It is ideal for communicating with small remote devices with limited power and network bandwidth. MQTT is used in a wide variety of industries, such as automotive, manufacturing, telecommunications, oil and gas.

Example code in the [aerospike-examples/mqtt-aerospike-example](https://github.com/aerospike-examples/mqtt-aerospike-example) GitHub repository shows end to end data flow between a small device and Aerospike, with the data being stored in Aerospike as queryable time series. Although the example is small in scope, the decoupled MQTT architecture and high performance Aerospike database will allow the approach to be scaled to accommodate thousands of devices, storing data over a period of years if necessary.

More specifically, the example simulates the generation of data from an IoT sensor and tracks how that can be sent to a specific topic on an MQTT Broker. The data simulator could [quite easily be replaced with an actual sensor](http://www.steves-internet-guide.com/using-arduino-pubsub-mqtt-client/), communicating with an MQTT Broker.

On the receiving side we look at how to subscribe to the above topic and how the data can be serialised to the Aerospike database using our Community [Time Series Client](https://github.com/aerospike-community/aerospike-time-series-client).

The net result of this is the ability to source data in a scalable fashion from IoT devices, storing it as queryable time series data within Aerospike.

## Generating the data

The data simulation in the example is reasonably sophisticated. Successive calls to the simulator result in _(timestamp,value)_ pairs (Data Points). The average time between _timestamps_ is specified at the outset as is a _percentage variability_ in the timestamps, to make the simulation realistic. The _ratio_ between successive _values_ is normally distributed - the mean and variance of this distribution is also specified before the simulation is started. So we have four parameters governing our simulation. In addition to that, an initial timestamp and value must be specified and the simulation given a name. The simulator constructor reflects this

```java
public TimeSeriesSimulator(
  String simulatorName, Date startTime, double initialValue, long observationIntervalMilliSeconds,
  double observationIntervalVariabilityPct, double dailyDriftPct, double dailyVolatilityPct) 
```

We obtain successive data points by calling

```java
public DataPoint getNextDataPoint()
```

The output below shows the kind of content we would expect to see, if simulating a sensor polling approximately hourly.

```
Sampling Engine-001-RPM-Sensor at time 2022-10-14 01:00:00.000. Found value 10000.000000. 
Sampling Engine-001-RPM-Sensor at time 2022-10-14 02:00:25.920. Found value 10470.777590. 
Sampling Engine-001-RPM-Sensor at time 2022-10-14 02:57:30.240. Found value 11123.240496. 
Sampling Engine-001-RPM-Sensor at time 2022-10-14 03:57:35.280. Found value 11066.086321. 
Sampling Engine-001-RPM-Sensor at time 2022-10-14 04:55:18.840. Found value 10599.837433. 
Sampling Engine-001-RPM-Sensor at time 2022-10-14 05:57:19.800. Found value 10268.800822. 
Sampling Engine-001-RPM-Sensor at time 2022-10-14 06:56:12.120. Found value 10256.870171. 
Sampling Engine-001-RPM-Sensor at time 2022-10-14 07:55:04.800. Found value 10329.697112. 
Sampling Engine-001-RPM-Sensor at time 2022-10-14 08:57:12.600. Found value 10307.305881. 
Sampling Engine-001-RPM-Sensor at time 2022-10-14 09:57:15.840. Found value 10436.093769. 

```

## Sending the data to an MQTT Broker

The MQTT paradigm assumes we have many disparate small devices. In order to collect information these devices will publish to a *topic* on an MQTT Broker. You can think of a broker as a centralised depot for the receipt and distribution of messages, which provides for scalability. *Topics* allow the messages to be separated into distinct collections. _Subscribers_ can independently subscribe to a topic and receive updates to the topic via push notification.

The code below shows the signature of a _Sensor Observer_ object. We provide a simulator to watch, a topic to publish to, and integers governing the frequency and number of observations.

```java
 public RunnableMQTTSensorObserver(ITimeSeriesSimulator timeSeriesSimulator, 
 long millisecondsBetweenObservations, long observationCount, MqttTopic publicationTopic)
```

The MQTT publication topic is obtained by connecting to a networked resource, ```MQTT_BROKER_URL``` using a publisher id ```MQTT_PUBLISHER_ID```. In this example, we use the public MQTT server ```tcp://test.mosquitto.org:1883```.

```java
IMqttClient mqttPublisher = new MqttClient(MQTT_BROKER_URL, MQTT_PUBLISHER_ID);
mqttPublisher.connect(standardMqttConnectOptions());
MqttTopic mqttTopic = mqttPublisher.getTopic(MQTT_TOPIC_NAME);
```

Here we are using the [Eclipse Paho](https://www.eclipse.org/paho/) implementation of the MQTT API.

When the observer is run the following code is executed ```observationCount``` times, each time resulting in the data point being sent to the publication topic.

```java
DataPoint dataPoint = timeSeriesSimulator.getCurrentDataPoint();
byte[] payload = MQTTUtilities.encodeForMQTT(timeSeriesSimulator.getSimulatorName(),dataPoint).getBytes();
MqttMessage msg = new MqttMessage(payload);
publicationTopic.publish(msg);
```

In the first line, we obtain a data point from the simulator.

In the second line, we encode the data point so it can be sent as a message. The encoding function has the following signature

```java
public static String encodeForMQTT(String timeSeriesName, DataPoint dataPoint)
```

It makes use of a very simple serialisation -  ```timeSeriesName:dataPoint.getTimestamp():dataPoint.getValue()``` - colon separated values. Please see the function ```MQTTUtilities.encodeForMQTT``` to see exactly how this is done.

In the third line we construct an ```MQTTMessage``` and finally, in the fourth line, publish it to the publication topic.

## Subscribing to an MQTT Broker

Similar to the above section, we connect to the MQTT Broker

```java
IMqttClient mqttSubscriber = new MqttClient(MQTT_BROKER_URL, MQTT_SUBSCRIBER_ID);
mqttSubscriber.connect(standardMqttConnectOptions());
```

We also create a listener object. 

```java
IMqttMessageListener mqttDataListener = new MQTTAerospikeDataPersister(asTimeSeriesClient);
```

This implements the ```IMqttMessageListener``` interface consisting of a single call.

```java
public void messageArrived(String topic, MqttMessage mqttMessage)
```

You will see that our implementation of ```IMqttMessageListener```,  ```MQTTAerospikeDataPersister``` requires an Aerospike Time Series Client when constructed.We subscribe to the topic using the listener object.

```java
mqttSubscriber.subscribe(MQTT_TOPIC_NAME, mqttDataListener);
```

## Inside the messageArrived function

Whenever a message is received the ```messageArrived``` function of the listener will be invoked. Below is our implementation code for that function.

```java
String mqttMessageAsString = new String(mqttMessage.getPayload(), Constants.MQTT_DEFAULT_CHARSET);
String timeSeriesName = MQTTUtilities.timeSeriesNameFromMQTTMessage(mqttMessageAsString);
DataPoint dataPoint = MQTTUtilities.dataPointFromMQTTMessage(mqttMessageAsString);
timeSeriesClient.put(timeSeriesName,dataPoint);
```

First we obtain the message as a string. In lines 2 and 3 we extract the time series name and data point (i.e. the timestamp and value). Finally we add the value to the Aerospike database using the ```put``` call of the ```timeSeriesClient```.

## Running the demonstration

This requires an Aerospike database accessible via the *localhost* address, listening on port 3000, although these values can be altered in the code via the ```MQTTPersistenceDemo.AEROSPIKE_SEED_HOST``` and ```MQTTPersistenceDemo.AEROSPIKE_SERVICE_PORT``` parameters. The easiest way of obtaining Aerospike is to install Docker Desktop and run an Aerospike Community container e.g.

```bash
docker run -d --name aerospike aerospike/aerospike-server
```

You can run ```MQTTPersistenceDemo.main()``` in your favourite IDE or build at the command line from the project root.

```bash
mvn clean compile assembly:single
```

Running the demonstration : 

```bash
java -jar target/aerospike-mqtt-example-1.0-SNAPSHOT-jar-with-dependencies.jar 
```

Your output should be similar to [this sample output](https://github.com/aerospike-examples/mqtt-aerospike-example/tree/resources/sample-output.txt)

## Reading the data from Aerospike using the Time Series API

```MQTTPersistenceDemo.main``` validates the end to end pipeline by requesting the data for our time series - *Engine-001-RPM-Sensor*. 

```java
MQTTUtilities.printTimeSeries(asTimeSeriesClient,SENSOR_NAME);
```

The body of the above function is as follows 

```Java
// Get the basic time series details
TimeSeriesInfo timeSeriesInfo = TimeSeriesInfo.getTimeSeriesDetails(timeSeriesClient, timeSeriesName);
// and output them
outputMessageWithPara(timeSeriesInfo.toString());
// use the time series client to get all the available points for our series with name timeSeriesName
// We use the timeSeriesInfo object to get the start and end date times for the series 
// so we can request all the points available
DataPoint[] dataPoints = timeSeriesClient.getPoints(timeSeriesName, 
  new Date(timeSeriesInfo.getStartDateTimestamp()),new Date(timeSeriesInfo.getEndDateTimestamp()));
// Header for the output
System.out.println("Timestamp,Value");
// For each point print out t formatted version of the point
for (DataPoint dataPoint : dataPoints) {
	outputMessage(String.format("%s,%.6f", dataPointDateToString(dataPoint), dataPoint.getValue()));
}
```

Typical output :

```text
Retrieving data for time series Engine-001-RPM-Sensor from Aerospike database

Name : Engine-001-RPM-Sensor Start Date : 2022-10-15 01:00:00 End Date 2022-10-15 09:52:44 Data point count : 10

Timestamp,Value
2022-10-15 01:00:00.000,10000.000000
2022-10-15 01:58:54.480,10197.212074
2022-10-15 02:57:50.040,10579.313417
2022-10-15 03:59:18.240,10025.330483
2022-10-15 04:56:36.600,10013.730374
2022-10-15 05:56:40.920,10188.447442
2022-10-15 06:58:32.880,10145.885126
2022-10-15 07:55:53.400,10350.374583
2022-10-15 08:54:05.400,10533.135383
2022-10-15 09:52:44.040,10326.813161
```

If you scroll back to the beginning of the article, you will see this is exactly the data initally emitted by our mock sensor.

## Conclusion

This example shows how the [Aerospike](https://aerospike.com/) database can be easily and scalably used to store industrial time series data made available by the [MQTT](https://mqtt.org/) ecosystem. Aerospike plus its Community [Time Series Client](https://github.com/aerospike-community/aerospike-time-series-client) streamlines the storage and retrieval of the data, supporting the ability to both write and read millions of data points per second if required.

## Further Directions

This demonstration could easily be scaled to show data being harvested from multiple sensors in parallel and saved to Aerospike.  It would also be interesting to replace the simulation with an actual device - something [Arduino based]((http://www.steves-internet-guide.com/using-arduino-pubsub-mqtt-client/)) for example.