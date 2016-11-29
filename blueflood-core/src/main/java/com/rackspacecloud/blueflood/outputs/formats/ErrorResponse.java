package com.rackspacecloud.blueflood.outputs.formats;

import java.util.List;

public class ErrorResponse {

    private List<ErrorData> errors;

    public ErrorResponse() {
    }

    public ErrorResponse(List<ErrorData> errors) {
        this.errors = errors;
    }

    public List<ErrorData> getErrors() {
        return errors;
    }

    public void setErrors(List<ErrorData> errors) {
        this.errors = errors;
    }

    @Override
    public String toString() {
        return "ErrorResponse{" +
                "errors=" + errors +
                '}';
    }

    public static class ErrorData {

        private String tenantId;
        private Long timestamp;
        private String metricName;
        private String source;
        private String message;

        public ErrorData() {
        }

        /***
         * Data class for storing error message for a metric from an ingest validation error
         * @param tenantId the tenantId of for the metric
         * @param metricName the name of the metric
         * @param source the source of the error within the metric, i.e. the field with the violiation
         * @param message the error message
         * @param timestamp the collectionTime of a metric, timestamp of an aggregated metric, or when value of an event
         */
        public ErrorData(String tenantId, String metricName, String source, String message, Long timestamp) {
            this.tenantId = tenantId == null ? "" : tenantId;
            this.metricName = metricName == null ? "" : metricName;
            this.source = source;
            this.message = message;
            this.timestamp = timestamp;
        }

        public String getTenantId() {
            return tenantId;
        }

        public Long getTimestamp() {
            return timestamp;
        }

        public String getMetricName() {
            return metricName;
        }

        public String getSource() {
            return source;
        }

        public String getMessage() {
            return message;
        }

        public void setTenantId(String tenantId) {
            this.tenantId = tenantId;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        public void setMetricName(String metricName) {
            this.metricName = metricName;
        }

        public void setSource(String source) {
            this.source = source;
        }

        public void setMessage(String message) {
            this.message = message;
        }

        @Override
        public String toString() {
            return "ErrorData{" +
                    "tenantId='" + tenantId + '\'' +
                    ", timestamp='" + timestamp + '\'' +
                    ", metricName='" + metricName + '\'' +
                    ", source='" + source + '\'' +
                    ", message='" + message + '\'' +
                    '}';
        }
    }

}
