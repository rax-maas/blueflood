/*
 * Copyright 2013 Rackspace
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.rackspacecloud.blueflood.http;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RouteMatcher {
    private final Map<Pattern, PatternRouteBinding> getBindings;
    private final Map<Pattern, PatternRouteBinding> putBindings;
    private final Map<Pattern, PatternRouteBinding> postBindings;
    private final Map<Pattern, PatternRouteBinding> deleteBindings;
    private final Map<Pattern, PatternRouteBinding> headBindings;
    private final Map<Pattern, PatternRouteBinding> optionsBindings;
    private final Map<Pattern, PatternRouteBinding> traceBindings;
    private final Map<Pattern, PatternRouteBinding> connectBindings;
    private final Map<Pattern, PatternRouteBinding> patchBindings;
    private HttpRequestHandler noRouteHandler;
    private HttpRequestHandler unsupportedMethodHandler;
    private HttpRequestHandler unsupportedVerbsHandler;
    private Map<Pattern, Set<String>> supportedMethodsForURLs;
    private List<Pattern> knownPatterns;

    private final Set<String> implementedVerbs;
    private static final Logger log = LoggerFactory.getLogger(RouteMatcher.class);

    public RouteMatcher() {
        this.getBindings = new HashMap<Pattern, PatternRouteBinding>();
        this.putBindings = new HashMap<Pattern, PatternRouteBinding>();
        this.postBindings = new HashMap<Pattern, PatternRouteBinding>();
        this.deleteBindings = new HashMap<Pattern, PatternRouteBinding>();
        this.headBindings = new HashMap<Pattern, PatternRouteBinding>();
        this.optionsBindings = new HashMap<Pattern, PatternRouteBinding>();
        this.connectBindings = new HashMap<Pattern, PatternRouteBinding>();
        this.patchBindings = new HashMap<Pattern, PatternRouteBinding>();
        this.traceBindings = new HashMap<Pattern, PatternRouteBinding>();
        this.implementedVerbs = new HashSet<String>();
        this.noRouteHandler = new NoRouteHandler();
        this.unsupportedMethodHandler = new UnsupportedMethodHandler(this);
        this.unsupportedVerbsHandler = new UnsupportedVerbsHandler();
        this.supportedMethodsForURLs = new HashMap<Pattern, Set<String>>();
        this.knownPatterns = new ArrayList<Pattern>();
    }

    public RouteMatcher withNoRouteHandler(HttpRequestHandler noRouteHandler) {
        this.noRouteHandler = noRouteHandler;

        return this;
    }

    public void route(ChannelHandlerContext context, HttpRequest request) {
        final String method = request.getMethod().getName();
        final String URI = request.getUri();

        // Method not implemented for any resource. So return 501.
        if (method == null || !implementedVerbs.contains(method)) {
            route(context, request, unsupportedVerbsHandler);
            return;
        }

        final Pattern pattern = getMatchingPatternForURL(URI);

        // No methods registered for this pattern i.e. URL isn't registered. Return 404.
        if (pattern == null) {
            route(context, request, noRouteHandler);
            return;
        }

        final Set<String> supportedMethods = getSupportedMethods(pattern);
        if (supportedMethods == null) {
            log.warn("No supported methods registered for a known pattern " + pattern);
            route(context, request, noRouteHandler);
            return;
        }

        // The method requested is not available for the resource. Return 405.
        if (!supportedMethods.contains(method)) {
            route(context, request, unsupportedMethodHandler);
            return;
        }
        PatternRouteBinding binding = null;
        if (method.equals(HttpMethod.GET.getName())) {
            binding = getBindings.get(pattern);
        } else if (method.equals(HttpMethod.PUT.getName())) {
            binding = putBindings.get(pattern);
        } else if (method.equals(HttpMethod.POST.getName())) {
            binding = postBindings.get(pattern);
        } else if (method.equals(HttpMethod.DELETE.getName())) {
            binding = deleteBindings.get(pattern);
        } else if (method.equals(HttpMethod.PATCH.getName())) {
            binding = deleteBindings.get(pattern);
        } else if (method.equals(HttpMethod.OPTIONS.getName())) {
            binding = optionsBindings.get(pattern);
         } else if (method.equals(HttpMethod.HEAD.getName())) {
            binding = headBindings.get(pattern);
        } else if (method.equals(HttpMethod.TRACE.getName())) {
            binding = traceBindings.get(pattern);
        } else if (method.equals(HttpMethod.CONNECT.getName())) {
            binding = connectBindings.get(pattern);
        }

        if (binding != null) {
            request = updateRequestHeaders(request, binding);
            route(context, request, binding.handler);
        } else {
            throw new RuntimeException("Cannot find a valid binding for URL " + URI);
        }
    }

    public void get(String pattern, HttpRequestHandler handler) {
        addBinding(pattern, HttpMethod.GET.getName(), handler, getBindings);
    }

    public void put(String pattern, HttpRequestHandler handler) {
        addBinding(pattern, HttpMethod.PUT.getName(), handler, putBindings);
    }

    public void post(String pattern, HttpRequestHandler handler) {
        addBinding(pattern, HttpMethod.POST.getName(), handler, postBindings);
    }

    public void delete(String pattern, HttpRequestHandler handler) {
        addBinding(pattern, HttpMethod.DELETE.getName(), handler, deleteBindings);
    }

    public void head(String pattern, HttpRequestHandler handler) {
        addBinding(pattern, HttpMethod.HEAD.getName(), handler, headBindings);
    }

    public void options(String pattern, HttpRequestHandler handler) {
        addBinding(pattern, HttpMethod.OPTIONS.getName(), handler, optionsBindings);
    }

    public void connect(String pattern, HttpRequestHandler handler) {
        addBinding(pattern, HttpMethod.CONNECT.getName(), handler, connectBindings);
    }

    public void patch(String pattern, HttpRequestHandler handler) {
        addBinding(pattern, HttpMethod.PATCH.getName(), handler, patchBindings);
    }

    public Set<String> getSupportedMethodsForURL(String URL) {
        final Pattern pattern = getMatchingPatternForURL(URL);
        return getSupportedMethods(pattern);
    }

    private HttpRequest updateRequestHeaders(HttpRequest request, PatternRouteBinding binding) {
        Matcher m = binding.pattern.matcher(request.getUri());
        if (m.matches()) {
            Map<String, String> headers = new HashMap<String, String>(m.groupCount());
            if (binding.paramsPositionMap != null) {
                for (String header : binding.paramsPositionMap.keySet()) {
                    headers.put(header, m.group(binding.paramsPositionMap.get(header)));
                }
            } else {
                for (int i = 0; i < m.groupCount(); i++) {
                    headers.put("param" + i, m.group(i + 1));
                }
            }

            for (Map.Entry<String, String> header : headers.entrySet()) {
                request.addHeader(header.getKey(), header.getValue());
            }
        }

        return request;
    }

    private void route(ChannelHandlerContext context, HttpRequest request, HttpRequestHandler handler) {
        if (handler == null) {
            handler = unsupportedVerbsHandler;
        }
        handler.handle(context, request);
    }

    private Pattern getMatchingPatternForURL(String URL) {
        for (Pattern pattern : knownPatterns) {
            if (pattern.matcher(URL).matches()) {
                return pattern;
            }
        }

        return null;
    }

    private Set<String> getSupportedMethods(Pattern pattern) {
        if (pattern == null) {
            return null;
        }

        return supportedMethodsForURLs.get(pattern);
    }

    private void addBinding(String URLPattern, String method, HttpRequestHandler handler,
                            Map<Pattern, PatternRouteBinding> bindings) {
        if (method == null || URLPattern == null || URLPattern.isEmpty() || method.isEmpty()) {
            return;
        }

        if (!method.isEmpty() && !URLPattern.isEmpty()) {
            implementedVerbs.add(method);
        }

        final PatternRouteBinding routeBinding = getPatternRouteBinding(URLPattern, handler);
        knownPatterns.add(routeBinding.pattern);

        Set<String> supportedMethods = supportedMethodsForURLs.get(routeBinding.pattern);
        if (supportedMethods == null) {
            supportedMethods = new HashSet<String>();
        }

        supportedMethods.add(method);
        supportedMethodsForURLs.put(routeBinding.pattern, supportedMethods);
        bindings.put(routeBinding.pattern, routeBinding);
    }

    private PatternRouteBinding getPatternRouteBinding(String URLPattern, HttpRequestHandler handler) {
        Pattern pattern = getMatchingPatternForURL(URLPattern);
        Map<String, Integer> groups = null;
      
        if(pattern == null) {
            // We need to search for any :<token name> tokens in the String and replace them with named capture groups
            Matcher m = Pattern.compile(":([A-Za-z][A-Za-z0-9_]*)").matcher(URLPattern);

            StringBuffer sb = new StringBuffer();
            groups = new HashMap<String, Integer>();
            int pos = 1;  // group 0 is the whole expression
            while (m.find()) {
                String group = m.group().substring(1);
                if (groups.containsKey(group)) {
                    throw new IllegalArgumentException("Cannot use identifier " + group + " more than once in pattern string");
                }
                m.appendReplacement(sb, "([^/]+)");
                groups.put(group, pos++);
            }
            m.appendTail(sb);

            final String regex = sb.toString();
            pattern = Pattern.compile(regex);
        }
        return new PatternRouteBinding(pattern, groups, handler);
    }

    private class PatternRouteBinding {
        final HttpRequestHandler handler;
        // TODO: Java 7 has named groups so you don't have to maintain this map explicitly.
        final Map<String, Integer> paramsPositionMap;
        final Pattern pattern;

        private PatternRouteBinding(Pattern pattern, Map<String, Integer> params, HttpRequestHandler handler) {
            this.pattern = pattern;
            this.paramsPositionMap = params;
            this.handler = handler;
        }
    }
}
