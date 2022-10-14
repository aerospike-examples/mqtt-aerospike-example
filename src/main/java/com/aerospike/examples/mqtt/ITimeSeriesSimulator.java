package com.aerospike.examples.mqtt;

import io.github.aerospike_examples.timeseries.DataPoint;

/**
 * Interface a time series simulator has to implement
 */
public interface ITimeSeriesSimulator {
    /**
     * Get simulator name
     * @return simulator name
     */
    String getSimulatorName();

    /**
     * Get current data point
     * @return data point (timestamp,value)
     */
    DataPoint getCurrentDataPoint();

    /**
     * Get next data point (timestamp,value)
     * @return data point (timestamp, value)
     */
    DataPoint getNextDataPoint();
}
