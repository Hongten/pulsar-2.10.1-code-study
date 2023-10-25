/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.client.impl;

import static org.apache.pulsar.client.util.MathUtils.signSafeMod;
import static org.apache.pulsar.common.util.netty.ChannelFutures.toCompletableFuture;
import com.google.common.annotations.VisibleForTesting;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelException;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.resolver.AddressResolver;
import io.netty.resolver.dns.DnsAddressResolverGroup;
import io.netty.resolver.dns.DnsNameResolverBuilder;
import io.netty.util.concurrent.Future;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientException.InvalidServiceURL;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.netty.DnsResolverUtil;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: 10/17/23 连接池实现
public class ConnectionPool implements AutoCloseable {
    // TODO: 10/17/23 用于存客户端于broker或proxy的连接
    protected final ConcurrentHashMap<InetSocketAddress, ConcurrentMap<Integer, CompletableFuture<ClientCnx>>> pool;

    private final Bootstrap bootstrap;
    private final PulsarChannelInitializer channelInitializerHandler;
    private final ClientConfigurationData clientConfig;
    private final EventLoopGroup eventLoopGroup;
    // TODO: 10/17/23 每个主机最大连接数
    private final int maxConnectionsPerHosts;
    private final boolean isSniProxy;
    // TODO: 10/17/23 地址解析

    protected final AddressResolver<InetSocketAddress> addressResolver;
    private final boolean shouldCloseDnsResolver;

    public ConnectionPool(ClientConfigurationData conf, EventLoopGroup eventLoopGroup) throws PulsarClientException {
        this(conf, eventLoopGroup, () -> new ClientCnx(conf, eventLoopGroup));
    }

    public ConnectionPool(ClientConfigurationData conf, EventLoopGroup eventLoopGroup,
                          Supplier<ClientCnx> clientCnxSupplier) throws PulsarClientException {
        this(conf, eventLoopGroup, clientCnxSupplier, Optional.empty());
    }

    public ConnectionPool(ClientConfigurationData conf, EventLoopGroup eventLoopGroup,
                          Supplier<ClientCnx> clientCnxSupplier,
                          Optional<AddressResolver<InetSocketAddress>> addressResolver)
            throws PulsarClientException {
        this.eventLoopGroup = eventLoopGroup;
        this.clientConfig = conf;
        // TODO: 2/24/23 connectionsPerBroker=1
        this.maxConnectionsPerHosts = conf.getConnectionsPerBroker();
        this.isSniProxy = clientConfig.isUseTls() && clientConfig.getProxyProtocol() != null
                && StringUtils.isNotBlank(clientConfig.getProxyServiceUrl());

        pool = new ConcurrentHashMap<>();
        bootstrap = new Bootstrap();
        bootstrap.group(eventLoopGroup);
        bootstrap.channel(EventLoopUtil.getClientSocketChannelClass(eventLoopGroup));

        bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, conf.getConnectionTimeoutMs());
        bootstrap.option(ChannelOption.TCP_NODELAY, conf.isUseTcpNoDelay());
        bootstrap.option(ChannelOption.ALLOCATOR, PulsarByteBufAllocator.DEFAULT);

        try {
            channelInitializerHandler = new PulsarChannelInitializer(conf, clientCnxSupplier);
            bootstrap.handler(channelInitializerHandler);
        } catch (Exception e) {
            log.error("Failed to create channel initializer");
            throw new PulsarClientException(e);
        }

        this.shouldCloseDnsResolver = !addressResolver.isPresent();
        this.addressResolver = addressResolver.orElseGet(() -> createAddressResolver(conf, eventLoopGroup));
    }

    private static AddressResolver<InetSocketAddress> createAddressResolver(ClientConfigurationData conf,
                                                                            EventLoopGroup eventLoopGroup) {
        DnsNameResolverBuilder dnsNameResolverBuilder = new DnsNameResolverBuilder()
                .traceEnabled(true).channelType(EventLoopUtil.getDatagramChannelClass(eventLoopGroup));
        if (conf.getDnsLookupBindAddress() != null) {
            InetSocketAddress addr = new InetSocketAddress(conf.getDnsLookupBindAddress(),
                    conf.getDnsLookupBindPort());
            dnsNameResolverBuilder.localAddress(addr);
        }
        DnsResolverUtil.applyJdkDnsCacheSettings(dnsNameResolverBuilder);
        // use DnsAddressResolverGroup to create the AddressResolver since it contains a solution
        // to prevent cache stampede / thundering herds problem when a DNS entry expires while the system
        // is under high load
        return new DnsAddressResolverGroup(dnsNameResolverBuilder).getResolver(eventLoopGroup.next());
    }

    private static final Random random = new Random();

    public CompletableFuture<ClientCnx> getConnection(final InetSocketAddress address) {
        return getConnection(address, address);
    }

    void closeAllConnections() {
        pool.values().forEach(map -> map.values().forEach(future -> {
            if (future.isDone()) {
                if (!future.isCompletedExceptionally()) {
                    // Connection was already created successfully, the join will not throw any exception
                    future.join().close();
                } else {
                    // If the future already failed, there's nothing we have to do
                }
            } else {
                // The future is still pending: just register to make sure it gets closed if the operation will
                // succeed
                future.thenAccept(ClientCnx::close);
            }
        }));
    }

    /**
     * Get a connection from the pool.
     * <p>
     * The connection can either be created or be coming from the pool itself.
     * <p>
     * When specifying multiple addresses, the logicalAddress is used as a tag for the broker, while the physicalAddress
     * is where the connection is actually happening.
     * <p>
     * These two addresses can be different when the client is forced to connect through a proxy layer. Essentially, the
     * pool is using the logical address as a way to decide whether to reuse a particular connection.
     *
     * @param logicalAddress
     *            the address to use as the broker tag
     * @param physicalAddress
     *            the real address where the TCP connection should be made
     * @return a future that will produce the ClientCnx object
     */
    public CompletableFuture<ClientCnx> getConnection(InetSocketAddress logicalAddress,
            InetSocketAddress physicalAddress) {
        // TODO: 2/24/23 connectionsPerBroker=1， 如果每个主机最大连接数为0，则禁用连接池，直接创建连接
        if (maxConnectionsPerHosts == 0) {
            // Disable pooling
            return createConnection(logicalAddress, physicalAddress, -1);
        }

        // TODO: 10/18/23 如果不为0，则产生一个随机数，maxConnectionsPerHosts取余，生成一个randomKey 
        final int randomKey = signSafeMod(random.nextInt(), maxConnectionsPerHosts);

        // TODO: 10/18/23 连接池里如果logicalAddress作为key，没有对应的value，
        //  则创建新ConcurrentMap<Integer, CompletableFuture<ClientCnx>对象，如果randomKey作为key，没有对应的value，
        //  则使用createConnection(logicalAddress, physicalAddress, randomKey)方法创建一个CompletableFuture<ClientCnx>
        return pool.computeIfAbsent(logicalAddress, a -> new ConcurrentHashMap<>()) //
                .computeIfAbsent(randomKey, k -> createConnection(logicalAddress, physicalAddress, randomKey));
    }

    // TODO: 2/24/23 创建到对应broker的连接, 到达这里，才真正触及到连接 broker 的核心方法以及子方法（也是在ConnectionPool类里）
    private CompletableFuture<ClientCnx> createConnection(InetSocketAddress logicalAddress,
            InetSocketAddress physicalAddress, int connectionKey) {
        if (log.isDebugEnabled()) {
            log.debug("Connection for {} not found in cache", logicalAddress);
        }

        final CompletableFuture<ClientCnx> cnxFuture = new CompletableFuture<>();

        // TODO: 2/24/23 创建到对应broker的连接, DNS解析主机名，返回IP数组，以此连接IP，
        //  只要一连接成功就返回，否则继续重试下一IP，如果所有IP重试完，还是没连接上，则抛异常
        // Trigger async connect to broker
        createConnection(physicalAddress).thenAccept(channel -> {
            log.info("[{}] Connected to server", channel);

            // TODO: 10/18/23 如果连接关闭，则清理垃圾（主要是从ConnectionPool里移除对应的对象）
            channel.closeFuture().addListener(v -> {
                // Remove connection from pool when it gets closed
                if (log.isDebugEnabled()) {
                    log.debug("Removing closed connection from pool: {}", v);
                }
                cleanupConnection(logicalAddress, connectionKey, cnxFuture);
            });

            // TODO: 10/18/23 这里已经连接上broker，但是需要等待直到连接握手完成
            // We are connected to broker, but need to wait until the connect/connected handshake is
            // complete
            final ClientCnx cnx = (ClientCnx) channel.pipeline().get("handler");
            if (!channel.isActive() || cnx == null) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Connection was already closed by the time we got notified", channel);
                }
                cnxFuture.completeExceptionally(new ChannelException("Connection already closed"));
                return;
            }

            if (!logicalAddress.equals(physicalAddress)) {
                // We are connecting through a proxy. We need to set the target broker in the ClientCnx object so that
                // it can be specified when sending the CommandConnect.
                // That phase will happen in the ClientCnx.connectionActive() which will be invoked immediately after
                // this method.
                // TODO: 10/18/23 当正在通过代理连接时，需要在ClientCnx对象中设置目标broker，这为了在发送CommandConnect命令时指定目标broker。
                //  在阶段发生在CliectCnx.connectActive()里，在完成本方法调用后，将会立即调用CliectCnx.connectActive()
                cnx.setTargetBroker(logicalAddress);
            }

            // TODO: 10/18/23 保存远程主机名，主要在处理握手包时，TLS校验主机名是否正确
            cnx.setRemoteHostName(physicalAddress.getHostName());

            // TODO: 10/18/23 这里就是等待与broker握手完成
            cnx.connectionFuture().thenRun(() -> {
                // TODO: 2/24/23 连接broker完成
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Connection handshake completed", cnx.channel());
                }
                // TODO: 10/18/23 连接到broker 
                cnxFuture.complete(cnx);
            }).exceptionally(exception -> {
                log.warn("[{}] Connection handshake failed: {}", cnx.channel(), exception.getMessage());
                cnxFuture.completeExceptionally(exception);
                cleanupConnection(logicalAddress, connectionKey, cnxFuture);
                cnx.ctx().close();
                return null;
            });
        }).exceptionally(exception -> {
            eventLoopGroup.execute(() -> {
                log.warn("Failed to open connection to {} : {}", physicalAddress, exception.getMessage());
                cleanupConnection(logicalAddress, connectionKey, cnxFuture);
                cnxFuture.completeExceptionally(new PulsarClientException(exception));
            });
            return null;
        });

        return cnxFuture;
    }

    /**
     * Resolve DNS asynchronously and attempt to connect to any IP address returned by DNS server.
     */
    private CompletableFuture<Channel> createConnection(InetSocketAddress unresolvedAddress) {
        // TODO: 10/18/23 DNS解析出IP列表--> 以此尝试连接IP，只要有成功的就返回 
        CompletableFuture<List<InetSocketAddress>> resolvedAddress;
        try {
            if (isSniProxy) {
                URI proxyURI = new URI(clientConfig.getProxyServiceUrl());
                resolvedAddress =
                        resolveName(InetSocketAddress.createUnresolved(proxyURI.getHost(), proxyURI.getPort()));
            } else {
                resolvedAddress = resolveName(unresolvedAddress);
            }
            // TODO: 2/24/23 连接broker
            return resolvedAddress.thenCompose(
                    // TODO: 10/18/23 连接解析出的IP
                    inetAddresses -> connectToResolvedAddresses(inetAddresses.iterator(),
                            isSniProxy ? unresolvedAddress : null));
        } catch (URISyntaxException e) {
            log.error("Invalid Proxy url {}", clientConfig.getProxyServiceUrl(), e);
            return FutureUtil
                    .failedFuture(new InvalidServiceURL("Invalid url " + clientConfig.getProxyServiceUrl(), e));
        }
    }

    /**
     * Try to connect to a sequence of IP addresses until a successful connection can be made, or fail if no
     * address is working.
     */
    private CompletableFuture<Channel> connectToResolvedAddresses(Iterator<InetSocketAddress> unresolvedAddresses,
                                                                  InetSocketAddress sniHost) {
        CompletableFuture<Channel> future = new CompletableFuture<>();

        // Successfully connected to server
        connectToAddress(unresolvedAddresses.next(), sniHost)
                // TODO: 10/18/23 这里成功连接到服务器上
                .thenAccept(future::complete)
                .exceptionally(exception -> {
                    // TODO: 10/18/23 连接异常，切换尝试切换下一IP ，判定迭代器是否还有IP地址
                    if (unresolvedAddresses.hasNext()) {
                        // TODO: 10/18/23 如果有，则继续尝试连接新的IP地址，递归调用
                        // Try next IP address
                        connectToResolvedAddresses(unresolvedAddresses, sniHost).thenAccept(future::complete)
                                .exceptionally(ex -> {
                                    // TODO: 10/18/23 这里已经解除递归调用 
                                    // This is already unwinding the recursive call
                                    future.completeExceptionally(ex);
                                    return null;
                                });
                    } else {
                        // TODO: 10/18/23 这里表示没有IP地址了，这返回连接抛出的异常 
                        // Failed to connect to any IP address
                        future.completeExceptionally(exception);
                    }
                    return null;
                });

        return future;
    }

    CompletableFuture<List<InetSocketAddress>> resolveName(InetSocketAddress unresolvedAddress) {
        // TODO: 10/18/23 DNS解析IP列表
        CompletableFuture<List<InetSocketAddress>> future = new CompletableFuture<>();
        addressResolver.resolveAll(unresolvedAddress).addListener((Future<List<InetSocketAddress>> resolveFuture) -> {
            if (resolveFuture.isSuccess()) {
                future.complete(resolveFuture.get());
            } else {
                future.completeExceptionally(resolveFuture.cause());
            }
        });
        return future;
    }

    /**
     * Attempt to establish a TCP connection to an already resolved single IP address.
     */
    private CompletableFuture<Channel> connectToAddress(InetSocketAddress remoteAddress, InetSocketAddress sniHost) {
        // TODO: 10/18/23 业务最底层调用，连接远程主机，当然，在建立连接过程中，将会触发通道激活方法 
        if (clientConfig.isUseTls()) {
            return toCompletableFuture(bootstrap.register())
                    .thenCompose(channel -> channelInitializerHandler
                            .initTls(channel, sniHost != null ? sniHost : remoteAddress))
                    .thenCompose(channelInitializerHandler::initSocks5IfConfig)
                    .thenCompose(channel -> toCompletableFuture(channel.connect(remoteAddress)));
        } else {
            // TODO: 2/24/23 连接broker
            return toCompletableFuture(bootstrap.register())
                    .thenCompose(channelInitializerHandler::initSocks5IfConfig)
                    .thenCompose(channel -> toCompletableFuture(channel.connect(remoteAddress)));
        }
    }

    public void releaseConnection(ClientCnx cnx) {
        if (maxConnectionsPerHosts == 0) {
            //Disable pooling
            if (cnx.channel().isActive()) {
                if (log.isDebugEnabled()) {
                    log.debug("close connection due to pooling disabled.");
                }
                cnx.close();
            }
        }
    }

    @Override
    public void close() throws Exception {
        closeAllConnections();
        if (shouldCloseDnsResolver) {
            addressResolver.close();
        }
    }

    private void cleanupConnection(InetSocketAddress address, int connectionKey,
            CompletableFuture<ClientCnx> connectionFuture) {
        ConcurrentMap<Integer, CompletableFuture<ClientCnx>> map = pool.get(address);
        if (map != null) {
            map.remove(connectionKey, connectionFuture);
        }
    }

    @VisibleForTesting
    int getPoolSize() {
        return pool.values().stream().mapToInt(Map::size).sum();
    }

    private static final Logger log = LoggerFactory.getLogger(ConnectionPool.class);

}

