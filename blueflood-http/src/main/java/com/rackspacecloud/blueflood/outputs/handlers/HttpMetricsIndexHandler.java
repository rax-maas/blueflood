package com.rackspacecloud.blueflood.outputs.handlers;

import com.rackspacecloud.blueflood.http.*;
import com.rackspacecloud.blueflood.io.Constants;
import com.rackspacecloud.blueflood.io.DiscoveryIO;
import com.rackspacecloud.blueflood.io.SearchResult;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.utils.ModuleLoader;
import com.rackspacecloud.blueflood.tracker.Tracker;
import io.netty.buffer.Unpooled;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;


public class HttpMetricsIndexHandler implements HttpRequestHandler {
    private static final Logger log = LoggerFactory.getLogger(HttpMetricsIndexHandler.class);
    private DiscoveryIO discoveryHandle;

    @Override
    public void handle(ChannelHandlerContext ctx, FullHttpRequest request) {

        Tracker.getInstance().track(request);

        final String tenantId = request.headers().get("tenantId");

        HttpRequestWithDecodedQueryParams requestWithParams = (HttpRequestWithDecodedQueryParams) request;

        // get the query param
        List<String> query = requestWithParams.getQueryParams().get("query");
        if (query == null || query.size() != 1) {
            DefaultHandler.sendErrorResponse(ctx, request, "Invalid Query String",
                    HttpResponseStatus.BAD_REQUEST);
            return;
        }

        // get the include_enum_values param to determine if results should contain enum values if applicable
        List<String> includeEnumValues = requestWithParams.getQueryParams().get("include_enum_values");

        if ((includeEnumValues != null) &&
            (includeEnumValues.size() != 0) &&
            (includeEnumValues.get(0).compareToIgnoreCase("true") == 0)) {
            // include_enum_values is present and set to true, use the ENUMS_DISCOVERY_MODULES as the discoveryHandle
            discoveryHandle = (DiscoveryIO) ModuleLoader.getInstance(DiscoveryIO.class, CoreConfig.ENUMS_DISCOVERY_MODULES);
        } else {
            // default discoveryHandle to DISCOVERY_MODULES
            discoveryHandle = (DiscoveryIO) ModuleLoader.getInstance(DiscoveryIO.class, CoreConfig.DISCOVERY_MODULES);
        }

        if (discoveryHandle == null) {
            sendResponse(ctx, request, null, HttpResponseStatus.NOT_FOUND);
            return;
        }

        try {
            List<SearchResult> searchResults = discoveryHandle.search(tenantId, query.get(0));
            sendResponse(ctx, request, getSerializedJSON(searchResults), HttpResponseStatus.OK);
        } catch (Exception e) {
            log.error(String.format("Exception occurred while trying to get metrics index for %s", tenantId), e);
            DefaultHandler.sendErrorResponse(ctx, request, "Error getting metrics index", HttpResponseStatus.INTERNAL_SERVER_ERROR);
        }
    }

    private void sendResponse(ChannelHandlerContext channel, FullHttpRequest request, String messageBody,
                              HttpResponseStatus status) {

        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status);

        if (messageBody != null && !messageBody.isEmpty()) {
            response.content().writeBytes(Unpooled.copiedBuffer(messageBody, Constants.DEFAULT_CHARSET));
        }

        HttpResponder.respond(channel, request, response);
        Tracker.getInstance().trackResponse(request, response);
    }

    public static String getSerializedJSON(List<SearchResult> searchResults) {
        ArrayNode resultArray = JsonNodeFactory.instance.arrayNode();
        for (SearchResult result : searchResults) {
            ObjectNode resultNode = JsonNodeFactory.instance.objectNode();
            resultNode.put("metric", result.getMetricName());

            String unit = result.getUnit();
            if (unit != null) {
                //Preaggreated metrics do not have units. Do not want to return null units in query results.
                resultNode.put("unit", unit);
            }

            // get enum values and add if applicable
            List<String> enumValues = result.getEnumValues();
            if (enumValues != null) {
                // sort for consistent result ordering
                Collections.sort(enumValues);
                ArrayNode enumValuesArray = JsonNodeFactory.instance.arrayNode();
                for (String val : enumValues) {
                    enumValuesArray.add(val);
                }
                resultNode.put("enum_values", enumValuesArray);
            }

            resultArray.add(resultNode);
        }
        return resultArray.toString();
    }
}
