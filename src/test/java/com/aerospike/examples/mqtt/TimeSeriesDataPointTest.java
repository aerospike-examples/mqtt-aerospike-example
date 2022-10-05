package com.aerospike.examples.mqtt;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

import java.util.Random;
import java.util.UUID;

public class TimeSeriesDataPointTest {
    @Test
    /**
     * Check that we get the original parameters back after encoding for MQTT and then decoding
     */
    public void checkEncodeDecodeUsingLongTimestamp(){
        // Create some message parameters
        long timestamp = System.currentTimeMillis();
        String timeSeriesName = UUID.randomUUID().toString();
        double value = (new Random()).nextDouble();

        // Encode as a message
        TimeSeriesDataPoint timeSeriesDataPoint = new TimeSeriesDataPoint(timeSeriesName,timestamp,value);
        String encodedMsgforMQTT = timeSeriesDataPoint.encodeForMQTT();

        // Then decode that message
        TimeSeriesDataPoint decodedMessage = TimeSeriesDataPoint.decodeFromMQTT(encodedMsgforMQTT);

        // Do we get back the original parameters
        // Note that some rounding is introduced
        // as reflected by the parameter Constants.DECIMAL_PLACES_PRECISION_FOR_MQTT
        assertEquals(decodedMessage.getTimeSeriesName(),timeSeriesName);
        assertEquals(decodedMessage.getTimestamp(),timestamp);
        assertEquals(decodedMessage.getValue(),value,Math.pow(10,-1 * Constants.DECIMAL_PLACES_PRECISION_FOR_MQTT));
    }
}
