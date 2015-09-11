package com.rackspacecloud.blueflood.outputs.handlers;

import com.rackspacecloud.blueflood.exceptions.InvalidDataException;
import com.rackspacecloud.blueflood.http.DefaultHandler;
import com.codahale.metrics.Timer;
import com.rackspacecloud.blueflood.http.HTTPRequestWithDecodedQueryParams;
import com.rackspacecloud.blueflood.http.HttpRequestHandler;
import com.rackspacecloud.blueflood.http.HttpResponder;
import com.rackspacecloud.blueflood.io.EventsIO;
import com.rackspacecloud.blueflood.utils.DateTimeParser;
import com.rackspacecloud.blueflood.utils.Metrics;
import org.codehaus.jackson.map.ObjectMapper;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.http.*;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;

public class HttpEventsQueryHandler implements HttpRequestHandler {
    private static final Logger log = LoggerFactory.getLogger(HttpEventsQueryHandler.class);
    private EventsIO searchIO;
    private final Timer httpEventsFetchTimer = Metrics.timer(HttpEventsQueryHandler.class, "Handle HTTP request for fetching events");

    public HttpEventsQueryHandler(EventsIO searchIO) {
        this.searchIO = searchIO;
    }

    @Override
    public void handle(ChannelHandlerContext ctx, HttpRequest request) {

        if(request.getMethod().toString().equalsIgnoreCase("GET")) {
            final String tenantId = request.getHeader("tenantId");
            HttpResponseStatus status = HttpResponseStatus.OK;

            ObjectMapper objectMapper = new ObjectMapper();
            String responseBody = null;
            final Timer.Context httpEventsFetchTimerContext = httpEventsFetchTimer.time();
            try {
                HTTPRequestWithDecodedQueryParams requestWithParams = (HTTPRequestWithDecodedQueryParams) request;
                Map<String, List<String>> params = requestWithParams.getQueryParams();

                if (params == null || params.size() == 0) {
                    throw new InvalidDataException("Query should contain at least one query parameter");
                }

                parseDateFieldInQuery(params, "from");
                parseDateFieldInQuery(params, "until");
                List<Map<String, Object>> searchResult = searchIO.search(tenantId, params);
                responseBody = objectMapper.writeValueAsString(searchResult);
            } catch (InvalidDataException e) {
                log.error(String.format("Exception %s", e.toString()));
                responseBody = String.format("Error: %s", e.getMessage());
                status = HttpResponseStatus.BAD_REQUEST;
            } catch (Exception e) {
                log.error(String.format("Exception %s", e.toString()));
                responseBody = String.format("Error: %s", e.getMessage());
                status = HttpResponseStatus.INTERNAL_SERVER_ERROR;
            } finally {
                Map <String, String> headers = new HashMap<String, String>();
                headers.put("Access-Control-Allow-Origin", "*");
                DefaultHandler.sendResponse(ctx, request, responseBody, status, headers);
                httpEventsFetchTimerContext.stop();
            }
        }
        else if(request.getMethod().toString().equalsIgnoreCase("OPTIONS")){
            Map <String, String> headers = new HashMap<String, String>();
            headers.put("Access-Control-Allow-Origin", "*");
            headers.put("Access-Control-Allow-Methods", "GET");
            headers.put("Access-Control-Allow-Headers", "X-Auth-Token, Accept");
            headers.put("Access-Control-Max-Age", "1728000");
            DefaultHandler.sendResponse(ctx, request, null, HttpResponseStatus.NO_CONTENT, headers);
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