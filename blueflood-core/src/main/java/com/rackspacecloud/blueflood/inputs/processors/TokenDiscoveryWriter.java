package com.rackspacecloud.blueflood.inputs.processors;

import com.codahale.metrics.Meter;
import com.google.common.util.concurrent.ListenableFuture;
import com.rackspacecloud.blueflood.cache.LocatorCache;
import com.rackspacecloud.blueflood.cache.TokenCache;
import com.rackspacecloud.blueflood.concurrent.FunctionWithThreadPool;
import com.rackspacecloud.blueflood.io.TokenDiscoveryIO;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Token;
import com.rackspacecloud.blueflood.utils.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;

/**
 * A separate discovery writer for tokens. This class is modelled after {@link DiscoveryWriter}
 */
public class TokenDiscoveryWriter extends FunctionWithThreadPool<List<List<IMetric>>, Void> {

    private final List<TokenDiscoveryIO> tokenDiscoveryIOs = new ArrayList<>();
    private final Map<Class<? extends TokenDiscoveryIO>, Meter> writeErrorMeters = new HashMap<>();
    private static final Logger log = LoggerFactory.getLogger(TokenDiscoveryWriter.class);

    public TokenDiscoveryWriter(ThreadPoolExecutor executor) {
        super(executor);
        registerIOModules();
    }

    public void registerIO(TokenDiscoveryIO io) {
        tokenDiscoveryIOs.add(io);
        writeErrorMeters.put(io.getClass(),
                Metrics.meter(io.getClass(), "TokenDiscoveryWriter Write Errors")
        );
    }

    public void registerIOModules() {
        List<String> modules = Configuration.getInstance().getListProperty(CoreConfig.TOKEN_DISCOVERY_MODULES);

        ClassLoader classLoader = TokenDiscoveryIO.class.getClassLoader();
        for (String module : modules) {
            log.info("Loading token discovery module " + module);
            try {
                Class discoveryClass = classLoader.loadClass(module);
                TokenDiscoveryIO discoveryIOModule = (TokenDiscoveryIO) discoveryClass.newInstance();
                log.info("Registering token discovery module " + module);
                registerIO(discoveryIOModule);
            } catch (InstantiationException e) {
                log.error("Unable to create instance of token discovery class for: " + module, e);
            } catch (IllegalAccessException e) {
                log.error("Error starting token discovery module: " + module, e);
            } catch (ClassNotFoundException e) {
                log.error("Unable to locate token discovery module: " + module, e);
            } catch (RuntimeException e) {
                log.error("Error starting token discovery module: " + module, e);
            } catch (Throwable e) {
                log.error("Error starting token discovery module: " + module, e);
            }
        }
    }

    /**
     * This method takes a list of list of metrics and does the foloowing
     *
     * 1) Convert list of list of metrics to flat list of {@link Locator}'s
     * 2) Filter only {@link Locator}'s which are not in token discovery layer cache.
     * 3) For all of these {@link Locator}'s, generate a unique list of {@link Token}'s which are not in token cache
     *
     * @param input
     * @return
     */
    static Set<Token> generateAndConsolidateTokens(final List<List<IMetric>> input) {

        //converting list of lists of metrics to flat list of locators that are not current.
        Stream<Locator> locators = input.stream()
                                        .flatMap(List::stream)
                                        .map(IMetric::getLocator)
                                        .filter(locator -> !LocatorCache.getInstance().isLocatorCurrentInTokenDiscoveryLayer(locator));

        return Token.getConsolidatedTokens(locators)
                    .filter(token -> !TokenCache.getInstance().isTokenCurrent(token))
                    .collect(toSet());
    }

    public ListenableFuture<Boolean> processTokens(final List<List<IMetric>> input) {

        return getThreadPool().submit(() -> {
            boolean success = true;

            List<Token> tokens = new ArrayList<>();
            tokens.addAll(generateAndConsolidateTokens(input));

            if (tokens.size() > 0) {
                for (TokenDiscoveryIO io : tokenDiscoveryIOs) {
                    try {
                        io.insertDiscovery(tokens);
                    } catch (Exception ex) {
                        getLogger().error(ex.getMessage(), ex);
                        writeErrorMeters.get(io.getClass()).mark();
                        success = false;
                    }
                }
            }

            if (success && tokens.size() > 0) {
                tokens.stream()
                      .filter(token -> !token.isLeaf()) //do not cache leaf nodes
                      .forEach(token -> {

                          //updating cache
                          TokenCache.getInstance().setTokenCurrent(token);
                          LocatorCache.getInstance().setLocatorCurrentInTokenDiscoveryLayer(token.getLocator());
                      });
            }

            return success;
        });
    }

    @Override
    public Void apply(List<List<IMetric>> input) throws Exception {
        processTokens(input);
        return null;
    }
}
