package com.rackspacecloud.blueflood.http;

import io.netty.handler.codec.http.HttpHeaders;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Unit test for {@link MediaTypeChecker}
 */
public class MediaTypeCheckerTest {

    private MediaTypeChecker mediaTypeChecker = new MediaTypeChecker();

    @Test
    public void contentTypeEmptyShouldBeValid() {

        HttpHeaders mockHeaders = mock(HttpHeaders.class);
        when(mockHeaders.get(HttpHeaders.Names.CONTENT_TYPE)).thenReturn("");

        assertTrue("empty content-type should be valid", mediaTypeChecker.isContentTypeValid(mockHeaders));
    }

    @Test
    public void contentTypeJsonShouldBeValid() {

        HttpHeaders mockHeaders = mock(HttpHeaders.class);
        when(mockHeaders.get(HttpHeaders.Names.CONTENT_TYPE)).thenReturn("application/json");

        assertTrue("content-type application/json should be valid", mediaTypeChecker.isContentTypeValid(mockHeaders));
    }

    @Test
    public void contentTypeJsonMixedCaseShouldBeValid() {

        HttpHeaders mockHeaders = mock(HttpHeaders.class);
        when(mockHeaders.get(HttpHeaders.Names.CONTENT_TYPE)).thenReturn("aPpLiCaTiOn/JSON");

        assertTrue("content-type aPpLiCaTiOn/JSON should be valid", mediaTypeChecker.isContentTypeValid(mockHeaders));
    }


    @Test
    public void contentTypeJsonWithCharsetShouldBeValid() {

        HttpHeaders mockHeaders = mock(HttpHeaders.class);
        when(mockHeaders.get(HttpHeaders.Names.CONTENT_TYPE)).thenReturn("application/json; charset=wtf-8");

        assertTrue("content-type application/json should be valid", mediaTypeChecker.isContentTypeValid(mockHeaders));
    }

    @Test
    public void contentTypePdfShouldBeInvalid() {

        HttpHeaders mockHeaders = mock(HttpHeaders.class);
        when(mockHeaders.get(HttpHeaders.Names.CONTENT_TYPE)).thenReturn("application/pdf");

        assertFalse("content-type application/pdf should be invalid", mediaTypeChecker.isContentTypeValid(mockHeaders));
    }

    @Test
    public void acceptEmptyShouldBeValid() {

        HttpHeaders mockHeaders = mock(HttpHeaders.class);
        when(mockHeaders.get(HttpHeaders.Names.ACCEPT)).thenReturn("");

        assertTrue("empty accept should be valid", mediaTypeChecker.isAcceptValid(mockHeaders));
    }

    @Test
    public void acceptAllShouldBeValid() {

        HttpHeaders mockHeaders = mock(HttpHeaders.class);
        when(mockHeaders.get(HttpHeaders.Names.ACCEPT)).thenReturn("*/*");

        assertTrue("accept */* should be valid", mediaTypeChecker.isAcceptValid(mockHeaders));
    }

    @Test
    public void acceptJsonShouldBeValid() {

        HttpHeaders mockHeaders = mock(HttpHeaders.class);
        when(mockHeaders.get(HttpHeaders.Names.ACCEPT)).thenReturn("application/json");

        assertTrue("accept application/json should be valid", mediaTypeChecker.isAcceptValid(mockHeaders));
    }

    @Test
    public void acceptAllWithCharsetQualityShouldBeValid() {

        HttpHeaders mockHeaders = mock(HttpHeaders.class);
        when(mockHeaders.get(HttpHeaders.Names.ACCEPT)).thenReturn("text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8");

        assertTrue("accept text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8 should be valid", mediaTypeChecker.isAcceptValid(mockHeaders));
    }

    @Test
    public void acceptXmlShouldBeInvalid() {

        HttpHeaders mockHeaders = mock(HttpHeaders.class);
        when(mockHeaders.get(HttpHeaders.Names.ACCEPT)).thenReturn("text/html,application/xhtml+xml,application/xml;q=0.9");

        assertFalse("accept text/html,application/xhtml+xml,application/xml;q=0.9 should be invalid", mediaTypeChecker.isAcceptValid(mockHeaders));
    }

}
