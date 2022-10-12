package com.aerospike.examples.mqtt;

import io.github.aerospike_examples.timeseries.DataPoint;
import org.junit.Assert;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Date;
import io.github.aerospike_examples.timeseries.util.Utilities;


public class TimeSeriesSimulatorTest {
    @Test
    public void simulatorTest(){
        // Today's date, starting at midnight
        Date startDateTime = new Date(Utilities.getTruncatedTimestamp(System.currentTimeMillis()));
        double initialValue = 10;
        long observationIntervalMillSeconds = 3600 * (int)Constants.MILLISECONDS_IN_SECOND;
        double observationIntervalVariabilityPct = 10;
        double dailyDrift = 0;
        double dailyVolatilityPct = 10;

        TimeSeriesSimulator timeSeriesSimulator = new TimeSeriesSimulator(startDateTime,initialValue,observationIntervalMillSeconds,
                observationIntervalVariabilityPct,dailyDrift,dailyVolatilityPct);
        long noOfIterations = TimeSeriesSimulator.SECONDS_IN_A_DAY / (observationIntervalMillSeconds / Constants.MILLISECONDS_IN_SECOND);
        for(int i=0;i<noOfIterations;i++ ){
            String timeSeriesDateFormat = "yyyy-MM-dd HH:mm:ss.SSS";
            SimpleDateFormat dateFormatter = new SimpleDateFormat(timeSeriesDateFormat);

            System.out.printf("%s : %.10f\n",dateFormatter.format(
                    new Date(timeSeriesSimulator.currentDataPoint.getTimestamp())),
                    timeSeriesSimulator.currentDataPoint.getValue());
            timeSeriesSimulator.getNextDataPoint();
        }
    }

    /**
     * Check our simulator returns values with the expected drift
     * and this is completely recoverable when there is no time series or timestamp variance
     */
    @Test
    public void checkMeanObservedNoVolatilityWithNoTimestampVariance() {
        double initialValue = 12;

        int iterationCount = 1000;
        long timeIncrementSeconds = 60;
        long observationIntervalMillSeconds = timeIncrementSeconds * Constants.MILLISECONDS_IN_SECOND;
        double observationIntervalVariabilityPct = 0;
        double dailyDriftPct = 10;
        double dailyVariancePct = 0;

        double tolerancePct = 0.01;

        // Today's date, starting at midnight
        Date startDateTime = new Date(Utilities.getTruncatedTimestamp(System.currentTimeMillis()));

        TimeSeriesSimulator timeSeriesSimulator = new TimeSeriesSimulator(startDateTime,initialValue,observationIntervalMillSeconds,
                observationIntervalVariabilityPct,dailyDriftPct,dailyVariancePct,TestConstants.RANDOM_SEED);

        DataPoint[] dataPoints = new DataPoint[iterationCount +1];
        dataPoints[0] = new DataPoint(startDateTime, initialValue);
        for (int i = 1; i <= iterationCount; i++) {
            dataPoints[i] = timeSeriesSimulator.getNextDataPoint();
        }
        Assert.assertTrue(Utilities.valueInTolerance(dailyDriftPct, calculateDailyDriftPct(dataPoints), tolerancePct));
    }

    /**
     * Check our simulator returns values with the expected drift
     * and this is completely recoverable when there is no time series variance, but there is timestamp variance
     */
    @Test
    public void checkMeanObservedNoVolatilityWithTimestampVariance() {
        double initialValue = 12;

        int iterationCount = 10000000;
        long timeIncrementSeconds = 60;
        long observationIntervalMillSeconds = timeIncrementSeconds * Constants.MILLISECONDS_IN_SECOND;
        double observationIntervalVariabilityPct = 5;
        double dailyDriftPct = 10;
        double dailyVariancePct = 0;

        double tolerancePct = 0.01;

        // Today's date, starting at midnight
        Date startDateTime = new Date(Utilities.getTruncatedTimestamp(System.currentTimeMillis()));

        TimeSeriesSimulator timeSeriesSimulator = new TimeSeriesSimulator(startDateTime,initialValue,observationIntervalMillSeconds,
                observationIntervalVariabilityPct,dailyDriftPct,dailyVariancePct,TestConstants.RANDOM_SEED);

        DataPoint[] dataPoints = new DataPoint[iterationCount +1];
        dataPoints[0] = new DataPoint(startDateTime, initialValue);
        for (int i = 1; i <= iterationCount; i++) {
            dataPoints[i] = timeSeriesSimulator.getNextDataPoint();
        }
        Assert.assertTrue(Utilities.valueInTolerance(dailyDriftPct, calculateDailyDriftPct(dataPoints), tolerancePct));
    }

    /**
     * Check our simulator returns values with the expected volatility
     * when there is no time series or timestamp variance
     */
    @Test
    public void checkVolObservedNoDriftWithNoTimestampVariance() {
        double initialValue = 12;

        int iterationCount = 10000;
        long timeIncrementSeconds = 60;
        long observationIntervalMillSeconds = timeIncrementSeconds * Constants.MILLISECONDS_IN_SECOND;
        double observationIntervalVariabilityPct = 0;
        double dailyDriftPct = 0;
        double dailyVariancePct = 10;

        double tolerancePct = 0.1;

        // Today's date, starting at midnight
        Date startDateTime = new Date(Utilities.getTruncatedTimestamp(System.currentTimeMillis()));

        TimeSeriesSimulator timeSeriesSimulator = new TimeSeriesSimulator(startDateTime,initialValue,observationIntervalMillSeconds,
                observationIntervalVariabilityPct,dailyDriftPct,dailyVariancePct,TestConstants.RANDOM_SEED);

        DataPoint[] dataPoints = new DataPoint[iterationCount +1];
        dataPoints[0] = new DataPoint(startDateTime, initialValue);
        for (int i = 1; i <= iterationCount; i++) {
            dataPoints[i] = timeSeriesSimulator.getNextDataPoint();
        }
        Assert.assertTrue(Utilities.valueInTolerance(dailyVariancePct, calculateDailyVolPct(dataPoints), tolerancePct));
    }

    /**
     * Check our simulator returns values with the expected volatility
     * when there is no time series but there is timestamp variance
     */
    @Test
    public void checkVolObservedNoDriftWithTimestampVariance() {
        double initialValue = 12;

        int iterationCount = 10000;
        long timeIncrementSeconds = 60;
        long observationIntervalMillSeconds = timeIncrementSeconds * Constants.MILLISECONDS_IN_SECOND;
        double observationIntervalVariabilityPct = 5;
        double dailyDriftPct = 0;
        double dailyVariancePct = 10;

        double tolerancePct = 0.1;

        // Today's date, starting at midnight
        Date startDateTime = new Date(Utilities.getTruncatedTimestamp(System.currentTimeMillis()));

        TimeSeriesSimulator timeSeriesSimulator = new TimeSeriesSimulator(startDateTime,initialValue,observationIntervalMillSeconds,
                observationIntervalVariabilityPct,dailyDriftPct,dailyVariancePct,TestConstants.RANDOM_SEED);

        DataPoint[] dataPoints = new DataPoint[iterationCount +1];
        dataPoints[0] = new DataPoint(startDateTime, initialValue);
        for (int i = 1; i <= iterationCount; i++) {
            dataPoints[i] = timeSeriesSimulator.getNextDataPoint();
        }
        Assert.assertTrue(Utilities.valueInTolerance(dailyVariancePct, calculateDailyVolPct(dataPoints), tolerancePct));
    }

    /**
     * Check our simulator returns values with the expected volatility
     * when there is no time series but there is timestamp variance
     *
     * Note that when the variance is higher than the drift we need a high number of iterations to obtain convergence
     */
    @Test
    public void checkVolAndDriftObservedWithNoTimestampVariance() {
        double initialValue = 12;

        int iterationCount = 10000000;
        long timeIncrementSeconds = 60;
        long observationIntervalMillSeconds = timeIncrementSeconds * Constants.MILLISECONDS_IN_SECOND;
        double observationIntervalVariabilityPct = 0;
        double dailyDriftPct = 2;
        double dailyVariancePct = 7;

        double tolerancePct = 10;

        // Today's date, starting at midnight
        Date startDateTime = new Date(Utilities.getTruncatedTimestamp(System.currentTimeMillis()));

        TimeSeriesSimulator timeSeriesSimulator = new TimeSeriesSimulator(startDateTime,initialValue,observationIntervalMillSeconds,
                observationIntervalVariabilityPct,dailyDriftPct,dailyVariancePct,TestConstants.RANDOM_SEED);

        DataPoint[] dataPoints = new DataPoint[iterationCount +1];
        dataPoints[0] = new DataPoint(startDateTime, initialValue);
        for (int i = 1; i <= iterationCount; i++) {
            dataPoints[i] = timeSeriesSimulator.getNextDataPoint();
        }
        Assert.assertTrue(Utilities.valueInTolerance(dailyDriftPct, calculateDailyDriftPct(dataPoints), tolerancePct));
        Assert.assertTrue(Utilities.valueInTolerance(dailyVariancePct, calculateDailyVolPct(dataPoints), tolerancePct));
    }

    /**
     * Check our simulator returns values with the expected volatility
     * when there is no time series but there is timestamp variance
     *
     * Note that when the variance is higher than the drift we need a high number of iterations to obtain convergence
     */
    @Test
    public void checkVolAndDriftObservedWithTimestampVariance() {
        double initialValue = 12;

        int iterationCount = 10000000;
        long timeIncrementSeconds = 60;
        long observationIntervalMillSeconds = timeIncrementSeconds * Constants.MILLISECONDS_IN_SECOND;
        double observationIntervalVariabilityPct = 5;
        double dailyDriftPct = 2;
        double dailyVariancePct = 7;

        double tolerancePct = 10;

        // Today's date, starting at midnight
        Date startDateTime = new Date(Utilities.getTruncatedTimestamp(System.currentTimeMillis()));

        TimeSeriesSimulator timeSeriesSimulator = new TimeSeriesSimulator(startDateTime,initialValue,observationIntervalMillSeconds,
                observationIntervalVariabilityPct,dailyDriftPct,dailyVariancePct,TestConstants.RANDOM_SEED);

        DataPoint[] dataPoints = new DataPoint[iterationCount +1];
        dataPoints[0] = new DataPoint(startDateTime, initialValue);
        for (int i = 1; i <= iterationCount; i++) {
            dataPoints[i] = timeSeriesSimulator.getNextDataPoint();
        }
        Assert.assertTrue(Utilities.valueInTolerance(dailyDriftPct, calculateDailyDriftPct(dataPoints), tolerancePct));
        Assert.assertTrue(Utilities.valueInTolerance(dailyVariancePct, calculateDailyVolPct(dataPoints), tolerancePct));
    }

    /**
     * Check our simulator returns values with the expected timestamp variance
     */
    @Test
    public void checkTimestampVariance() {
        double initialValue = 12;

        int iterationCount = 10000;
        long timeIncrementSeconds = 60;
        long observationIntervalMillSeconds = timeIncrementSeconds * Constants.MILLISECONDS_IN_SECOND;
        double observationIntervalVariabilityPct = 10;
        double dailyDriftPct = 2;
        double dailyVariancePct = 7;

        double tolerancePct = 1;

        double expectedSqrtVariancePct = observationIntervalVariabilityPct / Math.pow(3,0.5);
        // Today's date, starting at midnight
        Date startDateTime = new Date(Utilities.getTruncatedTimestamp(System.currentTimeMillis()));

        TimeSeriesSimulator timeSeriesSimulator = new TimeSeriesSimulator(startDateTime,initialValue,observationIntervalMillSeconds,
                observationIntervalVariabilityPct,dailyDriftPct,dailyVariancePct,TestConstants.RANDOM_SEED);

        DataPoint[] dataPoints = new DataPoint[iterationCount +1];
        dataPoints[0] = new DataPoint(startDateTime, initialValue);
        for (int i = 1; i <= iterationCount; i++) {
            dataPoints[i] = timeSeriesSimulator.getNextDataPoint();
        }
        double sumSqs = 0;
        for(int i=1;i<iterationCount;i++) sumSqs+=
                Math.pow(dataPoints[i].getTimestamp() - dataPoints[i-1].getTimestamp()
                        - timeIncrementSeconds * Constants.MILLISECONDS_IN_SECOND,2);
        double sqrtVariance = Math.sqrt(sumSqs/(iterationCount - 1));
        double normalisedSqrtVarianceOfTimestampsPct = 100 * sqrtVariance / timeIncrementSeconds / Constants.MILLISECONDS_IN_SECOND;
        Assert.assertTrue(Utilities.valueInTolerance(expectedSqrtVariancePct, normalisedSqrtVarianceOfTimestampsPct, tolerancePct));
    }

    /**
     * Private method to calculate the mean drift from a set of data points
     *
     * @param dataPoints - data points
     * @return calculated mean daily drift as a percentage
     */
    private static double calculateDailyDriftPct(DataPoint[] dataPoints) {
        DataPoint[] dataPointDiffs = new DataPoint[dataPoints.length - 1];
        for (int i = 0; i < dataPointDiffs.length; i++) {
            double valueDiff = dataPoints[i+1].getValue() / dataPoints[i].getValue() - 1;
            long timeDiff = dataPoints[i+1].getTimestamp() - dataPoints[i].getTimestamp();
            dataPointDiffs[i] = new DataPoint(timeDiff,valueDiff);
        }
        double sumDiffs = 0;
        long sumTimes = 0;
        for (int i = 0; i < dataPointDiffs.length; i++) {
            sumDiffs += dataPointDiffs[i].getValue();
            sumTimes += dataPointDiffs[i].getTimestamp();
        }
        return 100 * sumDiffs * TimeSeriesSimulator.SECONDS_IN_A_DAY * Constants.MILLISECONDS_IN_SECOND / sumTimes;
    }

    /**
     * Private method to calculate the daily volatility from a set of data points
     *
     * @param dataPoints - data points
     * @return calculated mean daily volatility as a percentage
     */
    private static double calculateDailyVolPct(DataPoint[] dataPoints) {
        DataPoint[] dataPointDiffs = new DataPoint[dataPoints.length - 1];
        for (int i = 0; i < dataPointDiffs.length; i++) {
            double valueDiff = dataPoints[i+1].getValue() / dataPoints[i].getValue() - 1;
            long timeDiff = dataPoints[i+1].getTimestamp() - dataPoints[i].getTimestamp();
            dataPointDiffs[i] = new DataPoint(timeDiff,valueDiff);
        }

        double sumSqs = 0;
        long sumTimes = 0;
        double estimatedDailyDriftPct = calculateDailyDriftPct(dataPoints);
        double perMilliSecondEstimatedDrift = estimatedDailyDriftPct / (100 * TimeSeriesSimulator.SECONDS_IN_A_DAY * Constants.MILLISECONDS_IN_SECOND);
        for (int i = 0; i < dataPointDiffs.length; i++) {
            sumSqs += Math.pow(dataPointDiffs[i].getValue() - perMilliSecondEstimatedDrift * dataPointDiffs[i].getTimestamp(),2);
            sumTimes += dataPointDiffs[i].getTimestamp();
        }
        return 100 * Math.sqrt(sumSqs * Constants.MILLISECONDS_IN_SECOND * TimeSeriesSimulator.SECONDS_IN_A_DAY/ sumTimes);
    }
}
