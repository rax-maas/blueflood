package com.rackspacecloud.blueflood.http;

import com.google.common.base.Strings;
import io.netty.handler.codec.http.*;

/**
 * A small utility class to be used by our Ingest and Query handlers to check for
 * valid media-type related headers.
 */
public class MediaTypeChecker {

    private static final String MEDIA_TYPE_APPLICATION_JSON = "application/json";
    private static final String ACCEPT_ALL = "*/*";

    /**
     * Checks the Content-Type header to see if clients specify the right
     * media type
     * @param headers
     * @return
     */
    public boolean isContentTypeValid(HttpHeaders headers) {

        String contentType = headers.get(HttpHeaders.Names.CONTENT_TYPE);

        // if we get no Content-Type or we get application/json, then it's valid
        // any other, it's invalid
        return (Strings.isNullOrEmpty(contentType) ||
                contentType.toLowerCase().contains(MEDIA_TYPE_APPLICATION_JSON));
    }

    /**
     * Checks the Accept header to see if clients accept the correct
     * media type
     * @param headers
     * @return
     */
    public boolean isAcceptValid(HttpHeaders headers) {
        String accept = headers.get(HttpHeaders.Names.ACCEPT);

        // if we get no Accept (which means */*), or */*,
        // or application/json, then it's valid
        return ( Strings.isNullOrEmpty(accept) ||
                accept.contains(ACCEPT_ALL) ||
                accept.toLowerCase().contains(MEDIA_TYPE_APPLICATION_JSON));
    }
}
