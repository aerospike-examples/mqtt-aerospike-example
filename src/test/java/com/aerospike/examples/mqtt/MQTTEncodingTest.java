package com.aerospike.examples.mqtt;

import io.github.aerospike_examples.timeseries.DataPoint;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

import java.util.Random;
import java.util.UUID;

public class MQTTEncodingTest {
    /**
     * Check that we get the original parameters back after encoding for MQTT and then decoding
     */
    @Test
    public void checkEncodeDecodeUsingLongTimestamp(){
        // Create some message parameters
        long timestamp = System.currentTimeMillis();
        String timeSeriesName = UUID.randomUUID().toString();
        double value = (new Random()).nextDouble();

        // Encode as a message
        DataPoint dataPoint = new DataPoint(timestamp,value);
        String encodedMsgForMQTT = MQTTUtilities.encodeForMQTT(timeSeriesName,dataPoint);

        // Then decode that message
        String timeSeriesNameFromMQTTMessage = MQTTUtilities.timeSeriesNameFromMQTTMessage(encodedMsgForMQTT);
        DataPoint dataPointFromMQTTMessage = MQTTUtilities.dataPointFromMQTTMessage(encodedMsgForMQTT);

        // Do we get back the original parameters
        // Note that some rounding is introduced
        // as reflected by the parameter Constants.DECIMAL_PLACES_PRECISION_FOR_MQTT
        assertEquals(timeSeriesNameFromMQTTMessage,timeSeriesName);
        assertEquals(dataPointFromMQTTMessage.getTimestamp(),timestamp);
        assertEquals(dataPointFromMQTTMessage.getValue(),value,Math.pow(10,-1 * Constants.DECIMAL_PLACES_PRECISION_FOR_MQTT));
    }
}
