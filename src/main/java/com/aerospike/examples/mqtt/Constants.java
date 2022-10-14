package com.aerospike.examples.mqtt;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * Class of useful constants
 */
public class Constants {
    /**
     * Default charset for MQTT
     */
    public static final Charset MQTT_DEFAULT_CHARSET = StandardCharsets.UTF_8;

    /**
     * Delimiter between fields in MQTT message
     */
    public static final String MQTT_MESSAGE_DELIMITER = ":";

    /**
     * Max decimal places in MQTT message
     */
    public static final int DECIMAL_PLACES_PRECISION_FOR_MQTT = 10;

    /**
     * Converting seconds to milliseconds and back again is so prevalent, best to make it a constant for clarity
     */
    static final long MILLISECONDS_IN_SECOND = 1000;

    /**
     * Seconds in a day - useful constant to store
     */
    static final int SECONDS_IN_A_DAY = 24 * 60 * 60;
}
