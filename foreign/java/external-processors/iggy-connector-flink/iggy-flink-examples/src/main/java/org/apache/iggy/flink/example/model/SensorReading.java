package org.apache.iggy.flink.example.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

/**
 * Sensor reading event for stream processing examples.
 */
public class SensorReading implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String sensorId;
    private final double temperature;
    private final double humidity;
    private final Instant timestamp;

    @JsonCreator
    public SensorReading(
            @JsonProperty("sensorId") String sensorId,
            @JsonProperty("temperature") double temperature,
            @JsonProperty("humidity") double humidity,
            @JsonProperty("timestamp") Instant timestamp) {
        this.sensorId = sensorId;
        this.temperature = temperature;
        this.humidity = humidity;
        this.timestamp = timestamp != null ? timestamp : Instant.now();
    }

    public String getSensorId() {
        return sensorId;
    }

    public double getTemperature() {
        return temperature;
    }

    public double getHumidity() {
        return humidity;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SensorReading that = (SensorReading) o;
        return Double.compare(that.temperature, temperature) == 0
                && Double.compare(that.humidity, humidity) == 0
                && Objects.equals(sensorId, that.sensorId)
                && Objects.equals(timestamp, that.timestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sensorId, temperature, humidity, timestamp);
    }

    @Override
    public String toString() {
        return "SensorReading{"
                + "sensorId='" + sensorId + '\''
                + ", temperature=" + temperature
                + ", humidity=" + humidity
                + ", timestamp=" + timestamp
                + '}';
    }
}
