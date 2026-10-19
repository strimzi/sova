/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.sova.http;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpPrincipal;

/**
 * Minimal {@link HttpExchange} stand-in for testing handlers without a running server.
 *
 * <p>Captures the response code and body so tests can assert on them.
 */
final class StubExchange extends HttpExchange {

    private final String method;
    private final String path;
    private final Headers responseHeaders = new Headers();
    private final ByteArrayOutputStream capture = new ByteArrayOutputStream();
    int responseCode = -1;

    StubExchange(String method, String path) {
        this.method = method;
        this.path = path;
    }

    String capturedBody() {
        return capture.toString();
    }

    @Override
    public Headers getRequestHeaders() {
        return new Headers();
    }

    @Override
    public Headers getResponseHeaders() {
        return responseHeaders;
    }

    @Override
    public URI getRequestURI() {
        return URI.create(path);
    }

    @Override
    public String getRequestMethod() {
        return method;
    }

    @Override
    public HttpContext getHttpContext() {
        return null;
    }

    @Override
    public void close() {
    }

    @Override
    public InputStream getRequestBody() {
        return InputStream.nullInputStream();
    }

    @Override
    public OutputStream getResponseBody() {
        return capture;
    }

    @Override
    public void sendResponseHeaders(int rCode, long responseLength) {
        this.responseCode = rCode;
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        return null;
    }

    @Override
    public int getResponseCode() {
        return responseCode;
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return null;
    }

    @Override
    public String getProtocol() {
        return "HTTP/1.1";
    }

    @Override
    public Object getAttribute(String name) {
        return null;
    }

    @Override
    public void setAttribute(String name, Object value) {
    }

    @Override
    public void setStreams(InputStream i, OutputStream o) {
    }

    @Override
    public HttpPrincipal getPrincipal() {
        return null;
    }
}
