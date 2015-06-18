package com.rackspacecloud.blueflood.outputs.handlers;

import com.rackspacecloud.blueflood.http.DefaultHandler;
import com.rackspacecloud.blueflood.http.HTTPRequestWithDecodedQueryParams;
import com.rackspacecloud.blueflood.http.HttpRequestHandler;
import com.rackspacecloud.blueflood.io.GenericElasticSearchIO;

import com.rackspacecloud.blueflood.utils.DateTimeParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.http.*;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


public class HttpEventsQueryHandler implements HttpRequestHandler {
    private static final Logger log = LoggerFactory.getLogger(HttpEventsQueryHandler.class);
    private GenericElasticSearchIO searchIO;

    public HttpEventsQueryHandler(GenericElasticSearchIO searchIO) {
        this.searchIO = searchIO;
    }


    @Override
    public void handle(ChannelHandlerContext ctx, HttpRequest request) {
        final String tenantId = request.getHeader("tenantId");
        HttpResponseStatus status = HttpResponseStatus.OK;

        ObjectMapper objectMapper = new ObjectMapper();
        String responseBody = null;
        try {
            HTTPRequestWithDecodedQueryParams requestWithParams = (HTTPRequestWithDecodedQueryParams) request;
            Map<String, List<String>> params = requestWithParams.getQueryParams();

            parseDateFieldInQuery(params, "from");
            parseDateFieldInQuery(params, "until");

            List<Map<String, Object>> searchResult = searchIO.search(tenantId, params);
            responseBody = objectMapper.writeValueAsString(searchResult);
        } catch (Exception e) {
            log.error(String.format("Exception %s", e.toString()));
            responseBody = String.format("Error: %s", e.getMessage());
            status = HttpResponseStatus.INTERNAL_SERVER_ERROR;
        } finally {
            DefaultHandler.sendResponse(ctx, request, responseBody, status);
        }
    }

    private void parseDateFieldInQuery(Map<String, List<String>> params, String name) {
        if (params.containsKey(name)) {
            String fromValue = extractDateFieldFromQuery(params.get(name));
            params.put(name, Arrays.asList(fromValue));
        }
    }

    private String extractDateFieldFromQuery(List<String> value) {
        DateTime dateTime = DateTimeParser.parse(value.get(0));
        return Long.toString(dateTime.getMillis() / 1000);
    }
}
