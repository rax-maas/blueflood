package com.rackspacecloud.blueflood.io;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

public class ElasticScrollCatchup {

    public interface Action<T> {
        void execute(int count, T context);
    }

    public interface Condition<T> {
        boolean test(int count, T context);
    }

    public interface Delayer {
        void delay();
    }

    public static class RepeatingAction<T> {
        Action<T> action = (count, context) -> {
        };
        Condition<T> condition = (count, context) -> false;
        Delayer delayer = () -> {
        };

        public RepeatingAction<T> withAction(Action<T> action) {
            this.action = action;
            return this;
        }

        public RepeatingAction<T> withCondition(Condition<T> condition) {
            this.condition = condition;
            return this;
        }

        public RepeatingAction<T> withDelayer(Delayer delayer) {
            this.delayer = delayer;
            return this;
        }

        public T start(T context) {
            int count = 1;
            while (condition.test(count, context)) {
                action.execute(count, context);
                delayer.delay();
                count++;
            }
            return context;
        }
    }

    /**
     * Holds the state for an entire Elasticsearch scroll, from start to end, meaning all documents across all pages.
     * It's a strictly internal class, so I'm going with direct field access to keep line count down.
     */
    static class ScrollContext {
        final DocumentHandler handler;
        boolean scrollComplete = false;
        String scrollId;
        int pageCount = 0;
        int documentCount = 0;

        public ScrollContext(DocumentHandler handler) {
            this.handler = handler;
        }
    }

    public static void main(String[] args) throws Exception {
        ElasticsearchScroller elasticsearchScroller = new ElasticsearchScroller("http://localhost:1234", "metric_metadata");
        elasticsearchScroller.setBatchSize(100000);
        // batch size experiments
        // at 250k: Saw 2905274 documents in   13 pages in 275210ms
        // at 1k:   Saw 2905274 documents in 2907 pages in 658827ms
        // at 20k:  Saw 2905274 documents in  147 pages in 305414ms
        // at 50k:  Saw 2905274 documents in   60 pages in 294768ms
        // at 100k: Saw 2905274 documents in   31 pages in 292903ms

        ScrollContext scrollContext = new ScrollContext(new DocumentHandler() {
            long lastUpdateTime;

            @Override
            public void handle(Map<?, ?> map, PageContext pageContext) {
//            System.out.println(map);
                if (lastUpdateTime < System.currentTimeMillis() - 30000) {
                    System.out.println("Seen " + pageContext.scrollContext.documentCount + " docs");
                    lastUpdateTime = System.currentTimeMillis();
                }
            }
        });
        long start = System.currentTimeMillis();
        CloseableHttpResponse response = elasticsearchScroller.initialRequest(scrollContext);
        elasticsearchScroller.handleElasticsearchResponse(response, scrollContext);
        while (!scrollContext.scrollComplete) {
            response = elasticsearchScroller.nextRequest(scrollContext);
            elasticsearchScroller.handleElasticsearchResponse(response, scrollContext);
        }
        long elapsed = System.currentTimeMillis() - start;
        System.out.printf("Saw %d documents in %d pages in %dms%n", scrollContext.documentCount, scrollContext.pageCount, elapsed);
    }

    // modernization phase 1
    public interface DocumentHandler {
        void handle(Map<?, ?> map, PageContext pageContext);
    }

    private static class ElasticsearchScroller {
        // todo: ensure client reuses connection
        private final CloseableHttpClient client = HttpClients.createDefault();
        private final String elasticsearchUri;
        private final String indexName;
        private int batchSize = 5000;

        public ElasticsearchScroller(String elasticsearchUri, String indexName) {
            this.elasticsearchUri = elasticsearchUri;
            this.indexName = indexName;
        }

        public void setBatchSize(int batchSize) {
            this.batchSize = batchSize;
        }

        public CloseableHttpResponse initialRequest(ScrollContext scrollContext) {
            try {
                return makeScrollRequest(elasticsearchUri, "/" + indexName + "/_search?scroll=1m&size=" + batchSize, getInitialQuery(), scrollContext);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }

        public CloseableHttpResponse nextRequest(ScrollContext scrollContext) {
            return makeScrollRequest(elasticsearchUri, "/_search/scroll?scroll=1m", getScrollBody(scrollContext.scrollId), scrollContext);
        }

        private CloseableHttpResponse makeScrollRequest(String elasticsearchUri, String path, String body, ScrollContext scrollContext) {
            HttpHost host = HttpHost.create(elasticsearchUri);
            HttpPost request = new HttpPost(path);
            request.setEntity(new StringEntity(body, StandardCharsets.UTF_8));
            try {
                return client.execute(host, request);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        private void handleElasticsearchResponse(CloseableHttpResponse response, ScrollContext scrollContext) throws Exception {
            HttpEntity entity = response.getEntity();
            boolean justDumpIt = false;
            // for debugging, break here and set the var to true
            if (justDumpIt) {
                entity.writeTo(System.out);
            }
            JsonFactory jsonFactory = new JsonFactory();
            JsonParser jsonParser = jsonFactory.createParser(entity.getContent());
            ObjectMapper objectMapper = new ObjectMapper(jsonFactory);
            PageContext ctx = new PageContext(jsonFactory, jsonParser, objectMapper, scrollContext);
            if (response.getStatusLine().getStatusCode() != 200) {
                handleError(response.getStatusLine(), jsonParser);
                EntityUtils.consume(entity);
                throw new RuntimeException("Unexpected response: " + response.getStatusLine());
            }
            JsonToken token = jsonParser.nextToken();
            if (token != JsonToken.START_OBJECT) {
                throw new IllegalStateException("Expected START_OBJECT, got " + token);
            }
            new ContentHandler(jsonParser).handleOuterResponse("simple", ctx);
            EntityUtils.consume(entity);

        }

        private String getInitialQuery() throws JsonProcessingException {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.writeValueAsString(new HashMap() {{
                put("query", new HashMap() {{
                    put("match_all", new HashMap() {{
                    }});
                }});
            }});
        }

        private String getScrollBody(String scrollId) {
            // for ES 1.7, just the scroll id goes in the body; be sure to include scroll=1m as a query param, or you
            // won't get another scroll_id
            return scrollId;
        }
    }

    /**
     * Holds the state for processing a single page of results. It also includes a reference to the higher level {@link
     * ScrollContext}.
     */
    private static class PageContext {
        final JsonFactory jsonFactory;
        final JsonParser jsonParser;
        final ObjectMapper objectMapper;
        final ScrollContext scrollContext;

        public PageContext(JsonFactory jsonFactory, JsonParser jsonParser, ObjectMapper objectMapper, ScrollContext scrollContext) {
            this.jsonFactory = jsonFactory;
            this.jsonParser = jsonParser;
            this.objectMapper = objectMapper;
            this.scrollContext = scrollContext;
        }
    }

    private static void handleError(StatusLine statusLine, JsonParser jsonParser) throws Exception {
        JsonToken token;
        while ((token = jsonParser.nextToken()) != null) {
            if (token != JsonToken.FIELD_NAME) {
                throw new Exception("Expected FIELD_NAME, got " + token);
            }
            if ("error".equals(jsonParser.getCurrentName())) {
                token = jsonParser.nextToken();
                if (token != JsonToken.VALUE_STRING) {
                    throw new Exception("Expected VALUE_STRING, got " + token);
                }
                System.out.println("error: " + jsonParser.getText());
                continue;
            }
            if ("status".equals(jsonParser.getCurrentName())) {
                token = jsonParser.nextToken();
                if (token != JsonToken.VALUE_NUMBER_INT) {
                    throw new Exception("Expected VALUE_NUMBER_INT, got " + token);
                }
                System.out.println("status " + jsonParser.getIntValue());
                continue;
            }
            token = jsonParser.nextToken();
            System.out.println("handle field: " + jsonParser.getCurrentName() + ", value is " + token);
        }
    }

    static class ContentHandler {

        final JsonParser jsonParser;

        public ContentHandler(JsonParser jsonParser) {
            this.jsonParser = jsonParser;
        }

        public void handleOuterResponse(String whichWay, PageContext ctx) throws Exception {
            ctx.scrollContext.pageCount++;
            if ("simple".equals(whichWay)) {
                handleOuterResponseSimpleWay(ctx);
            } else if ("pretty".equals(whichWay)) {
                handleOuterResponseObjectPrettyWay(ctx);
            } else {
                throw new Exception("Unknown way: " + whichWay);
            }
        }
        public void handleOuterResponseSimpleWay(PageContext ctx) throws Exception {
            JsonToken token;
            while ((token = jsonParser.nextToken()) != JsonToken.END_OBJECT) {
                if (token == JsonToken.FIELD_NAME && "_scroll_id".equals(jsonParser.getCurrentName())) {
                    token = jsonParser.nextToken();
                    if (token != JsonToken.VALUE_STRING) {
                        throw new Exception("Expected VALUE_STRING, got " + token);
                    }
                    ctx.scrollContext.scrollId = jsonParser.getText();
//            System.out.println("scrollId: " + scrollId);
                    continue;
                }
                if (token == JsonToken.FIELD_NAME && "timed_out".equals(jsonParser.getCurrentName())) {
                    token = jsonParser.nextToken();
                    if (token != JsonToken.VALUE_FALSE) {
                        throw new Exception("query timed out!");
                    }
                    continue;
                }
                if (token == JsonToken.FIELD_NAME && "took".equals(jsonParser.getCurrentName())) {
                    token = jsonParser.nextToken();
                    if (token != JsonToken.VALUE_NUMBER_INT) {
                        throw new Exception("Expected VALUE_NUMBER_INT, got " + token);
                    }
                    System.out.println("query took " + jsonParser.getIntValue() + "ms");
                    continue;
                }
                if (token == JsonToken.FIELD_NAME && "_shards".equals(jsonParser.getCurrentName())) {
                    token = jsonParser.nextToken();
                    if (token != JsonToken.START_OBJECT) {
                        throw new Exception("Expected START_OBJECT, got " + token);
                    }
//            System.out.println("Skip _shards");
                    jsonParser.skipChildren();
                    continue;
                }
                if (token == JsonToken.FIELD_NAME && "hits".equals(jsonParser.getCurrentName())) {
                    token = jsonParser.nextToken();
                    if (token != JsonToken.START_OBJECT) {
                        throw new Exception("Expected START_OBJECT, got " + token);
                    }
                    handleFirstHitsObject(ctx);
                    continue;
                }
                System.out.println("unhandled token: " + token);
            }
        }

        interface FieldHandler<T> {
            void handleField() throws Exception;

            FieldHandler<T> and(Next<T> next) throws Exception;
        }

        class FieldHandlerImpl<T> implements FieldHandler<T> {
            private final JsonToken expectedJsonToken;
            private final Callable<T> valueToApplyGetter;
            private Next<T> next;

            public FieldHandlerImpl(JsonToken expectedJsonToken, Callable<T> valueToApplyGetter) {
                this.expectedJsonToken = expectedJsonToken;
                this.valueToApplyGetter = valueToApplyGetter;
            }

            @Override
            public void handleField() throws Exception {
                JsonToken token = jsonParser.nextToken();
                if (token != expectedJsonToken) {
                    throw new Exception("Expected " + expectedJsonToken + ", got " + token);
                }
                next.apply(valueToApplyGetter.call());
            }

            @Override
            public FieldHandler<T> and(Next<T> next) throws Exception {
                this.next = next;
                return this;
            }
        }

        FieldHandler<String> expectString() {
            return new FieldHandlerImpl<>(JsonToken.VALUE_STRING, jsonParser::getText);
        }

        FieldHandler<Integer> expectInt() {
            return new FieldHandlerImpl<>(JsonToken.VALUE_NUMBER_INT, jsonParser::getIntValue);
        }

        FieldHandler<Void> expectObject() {
            return new FieldHandlerImpl<>(JsonToken.START_OBJECT, () -> null);
        }

        FieldHandler<JsonToken> expectOneOf(JsonToken... tokens) {
            return new FieldHandler<JsonToken>() {
                private Next<JsonToken> next;

                @Override
                public void handleField() throws Exception {
                    JsonToken token = jsonParser.nextToken();
                    if (Arrays.stream(tokens).noneMatch(t -> t == token)) {
                        throw new Exception("Expected one of " + Arrays.toString(tokens) + ", got " + token);
                    }
                    next.apply(token);
                }

                @Override
                public FieldHandler<JsonToken> and(Next<JsonToken> next) {
                    this.next = next;
                    return this;
                }
            };
        }

        interface Next<T> {
            void apply(T it) throws Exception;
        }

        public void handleOuterResponseObjectPrettyWay(PageContext ctx) throws Exception {
            while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
                // Experimental, shorter way of handling the parsing, but not sure it's worth the extra complexity
                Map<String, FieldHandler<?>> handlers = new HashMap<>();
                handlers.put("_scroll_id", expectString().and(it -> ctx.scrollContext.scrollId = it));
                handlers.put("took", expectInt().and(it -> {
                }));
                handlers.put("_shards", expectObject().and(it -> jsonParser.skipChildren()));
                handlers.put("timed_out", expectOneOf(JsonToken.VALUE_TRUE, JsonToken.VALUE_FALSE).and(it -> {
                    if (it == JsonToken.VALUE_TRUE) throw new Exception("Query timed out!");
                }));
                handlers.put("hits", expectObject().and(it -> handleFirstHitsObject(ctx)));
                FieldHandler<?> fieldHandler = handlers.get(jsonParser.getCurrentName());
                if (fieldHandler != null) {
                    fieldHandler.handleField();
                } else {
                    JsonToken token = jsonParser.nextToken();
                    System.out.println("unhandled field: " + jsonParser.getCurrentName() + ", value is " + token);
                }
            }
        }

        private void handleFirstHitsObject(PageContext ctx) throws Exception {
            JsonToken token;
            while ((token = jsonParser.nextToken()) != JsonToken.END_OBJECT) {
                if (token == JsonToken.FIELD_NAME && "total".equals(jsonParser.getCurrentName())) {
                    token = jsonParser.nextToken();
                    if (token != JsonToken.VALUE_NUMBER_INT) {
                        throw new Exception("Expected VALUE_NUMBER_INT, got " + token);
                    }
//                System.out.println("total hits: " + jsonParser.getIntValue());
                    continue;
                }
                if (token == JsonToken.FIELD_NAME && "max_score".equals(jsonParser.getCurrentName())) {
                    token = jsonParser.nextToken();

                    if (token != JsonToken.VALUE_NUMBER_FLOAT && token != JsonToken.VALUE_NULL) {
                        throw new Exception("Expected VALUE_NUMBER_FLOAT or VALUE_NULL, got " + token);
                    }
//                System.out.println("max_score: " + jsonParser.getFloatValue());
                    continue;
                }
                if (token == JsonToken.FIELD_NAME && "hits".equals(jsonParser.getCurrentName())) {
                    token = jsonParser.nextToken();
                    if (token != JsonToken.START_ARRAY) {
                        throw new Exception("Expected START_OBJECT, got " + token);
                    }
//                    System.out.println("handling hits");
                    handleHitsArray(ctx);
                    continue;
                }
                System.out.println("unhandled token: " + token);
            }
        }

        private void handleHitsArray(PageContext ctx) throws Exception {
            JsonToken token = jsonParser.nextToken();
            if (token == JsonToken.END_ARRAY) {
                // then we're done! how do i get out of here?
                ctx.scrollContext.scrollComplete = true;
            }
            while (token != JsonToken.END_ARRAY) {
                Map<?, ?> map = ctx.objectMapper.readValue(jsonParser, Map.class);
                ctx.scrollContext.handler.handle(map, ctx);
                ctx.scrollContext.documentCount++;
                token = jsonParser.nextToken();
            }
        }
    }
}
