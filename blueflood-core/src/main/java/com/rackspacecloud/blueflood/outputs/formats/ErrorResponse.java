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
        private String metricName;
        private String source;
        private String message;

        public ErrorData() {
        }

        public ErrorData(String tenantId, String metricName, String source, String message) {
            this.tenantId = tenantId == null ? "" : tenantId;
            this.metricName = metricName == null ? "" : metricName;
            this.source = source;
            this.message = message;
        }

        public String getTenantId() {
            return tenantId;
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
                    ", metricName='" + metricName + '\'' +
                    ", source='" + source + '\'' +
                    ", message='" + message + '\'' +
                    '}';
        }
    }

}
