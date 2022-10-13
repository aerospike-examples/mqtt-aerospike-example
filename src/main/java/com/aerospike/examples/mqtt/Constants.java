package com.aerospike.examples.mqtt;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class Constants {
    public static final Charset MQTT_DEFAULT_CHARSET = StandardCharsets.UTF_8;
    public static final String MQTT_MESSAGE_DELIMITER = ":";
    public static final int DECIMAL_PLACES_PRECISION_FOR_MQTT = 10;

    /**
     * Converting seconds to milliseconds and back again is so prevalent, best to make it a constant for clarity
     */
    public final static long MILLISECONDS_IN_SECOND = 1000;

    // Useful constant
    public static final int SECONDS_IN_A_DAY = 24 * 60 * 60;
}
