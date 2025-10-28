package org.apache.iggy.flink.example.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

/**
 * Alert event generated from sensor readings.
 */
public class Alert implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String sensorId;
    private final String alertType;
    private final double value;
    private final String message;
    private final Instant timestamp;

    @JsonCreator
    public Alert(
            @JsonProperty("sensorId") String sensorId,
            @JsonProperty("alertType") String alertType,
            @JsonProperty("value") double value,
            @JsonProperty("message") String message,
            @JsonProperty("timestamp") Instant timestamp) {
        this.sensorId = sensorId;
        this.alertType = alertType;
        this.value = value;
        this.message = message;
        this.timestamp = timestamp != null ? timestamp : Instant.now();
    }

    public String getSensorId() {
        return sensorId;
    }

    public String getAlertType() {
        return alertType;
    }

    public double getValue() {
        return value;
    }

    public String getMessage() {
        return message;
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
        Alert alert = (Alert) o;
        return Double.compare(alert.value, value) == 0
                && Objects.equals(sensorId, alert.sensorId)
                && Objects.equals(alertType, alert.alertType)
                && Objects.equals(message, alert.message)
                && Objects.equals(timestamp, alert.timestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sensorId, alertType, value, message, timestamp);
    }

    @Override
    public String toString() {
        return "Alert{"
                + "sensorId='" + sensorId + '\''
                + ", alertType='" + alertType + '\''
                + ", value=" + value
                + ", message='" + message + '\''
                + ", timestamp=" + timestamp
                + '}';
    }
}
