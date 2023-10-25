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
package org.apache.pulsar.client.impl.conf;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.swagger.annotations.ApiModelProperty;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Clock;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.ProxyProtocol;
import org.apache.pulsar.client.api.ServiceUrlProvider;
import org.apache.pulsar.client.impl.auth.AuthenticationDisabled;
import org.apache.pulsar.client.util.Secret;

/**
 * This is a simple holder of the client configuration values.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ClientConfigurationData implements Serializable, Cloneable {
    private static final long serialVersionUID = 1L;

    // todo serviceUrl, e.g pulsar://broker:6650"
    @ApiModelProperty(
            name = "serviceUrl",
            value = "Pulsar cluster HTTP URL to connect to a broker."
    )
    private String serviceUrl;
    @ApiModelProperty(
            name = "serviceUrlProvider",
            value = "The implementation class of ServiceUrlProvider used to generate ServiceUrl."
    )
    @JsonIgnore
    private transient ServiceUrlProvider serviceUrlProvider;

    // todo 权限相关
    @ApiModelProperty(
            name = "authentication",
            value = "Authentication settings of the client."
    )
    @JsonIgnore
    private Authentication authentication;

    @ApiModelProperty(
            name = "authPluginClassName",
            value = "Class name of authentication plugin of the client."
    )
    private String authPluginClassName;

    @ApiModelProperty(
            name = "authParams",
            value = "Authentication parameter of the client."
    )
    @Secret
    private String authParams;

    @ApiModelProperty(
            name = "authParamMap",
            value = "Authentication map of the client."
    )
    @Secret
    private Map<String, String> authParamMap;

    // todo 客户端超时时间，默认为30s
    @ApiModelProperty(
            name = "operationTimeoutMs",
            value = "Client operation timeout (in milliseconds)."
    )
    private long operationTimeoutMs = 30000;

    // todo lookup timeout, 默认为-1， 应该是无限，最终应该是上面的时间先超时 30s
    @ApiModelProperty(
            name = "lookupTimeoutMs",
            value = "Client lookup timeout (in milliseconds)."
    )
    private long lookupTimeoutMs = -1;

    // todo 多久打印一下数据，比如消费者的消费速度、字节数、消费者总共接收了多少消息等
    @ApiModelProperty(
            name = "statsIntervalSeconds",
            value = "Interval to print client stats (in seconds)."
    )
    private long statsIntervalSeconds = 60;

    // todo IO线程数，默认为1
    @ApiModelProperty(
            name = "numIoThreads",
            value = "Number of IO threads."
    )
    private int numIoThreads = 1;

    // todo 消费者线程数
    @ApiModelProperty(
            name = "numListenerThreads",
            value = "Number of consumer listener threads."
    )
    private int numListenerThreads = 1;

    // todo 创建连接池使用，默认为1，即连接数为1
    @ApiModelProperty(
            name = "connectionsPerBroker",
            value = "Number of connections established between the client and each Broker."
                    + " A value of 0 means to disable connection pooling."
    )
    private int connectionsPerBroker = 1;

    // todo tcp nagle算法
    @ApiModelProperty(
            name = "useTcpNoDelay",
            value = "Whether to use TCP NoDelay option."
    )
    private boolean useTcpNoDelay = true;

    // todo TLS协议相关
    @ApiModelProperty(
            name = "useTls",
            value = "Whether to use TLS."
    )
    private boolean useTls = false;

    @ApiModelProperty(
            name = "tlsTrustCertsFilePath",
            value = "Path to the trusted TLS certificate file."
    )
    private String tlsTrustCertsFilePath = "";

    @ApiModelProperty(
            name = "tlsAllowInsecureConnection",
            value = "Whether the client accepts untrusted TLS certificates from the broker."
    )
    private boolean tlsAllowInsecureConnection = false;

    @ApiModelProperty(
            name = "tlsHostnameVerificationEnable",
            value = "Whether the hostname is validated when the proxy creates a TLS connection with brokers."
    )
    private boolean tlsHostnameVerificationEnable = false;
    // todo 用于创建Semaphore，控制客户端向服务端请求速率 例如查询topic属于哪个broker，查询分区数
    @ApiModelProperty(
            name = "concurrentLookupRequest",
            value = "The number of concurrent lookup requests that can be sent on each broker connection. "
                    + "Setting a maximum prevents overloading a broker."
    )
    private int concurrentLookupRequest = 5000;

    @ApiModelProperty(
            name = "maxLookupRequest",
            value = "Maximum number of lookup requests allowed on "
                    + "each broker connection to prevent overloading a broker."
    )
    private int maxLookupRequest = 50000;

    // todo 重定向次数 查询topic属于哪个broker时随机选个broker看看是否存在，不存在定向到其他broker
    @ApiModelProperty(
            name = "maxLookupRedirects",
            value = "Maximum times of redirected lookup requests."
    )
    private int maxLookupRedirects = 20;

    // todo 当前请求过多，对服务端造成限流，会拒绝客户端请求 场景：还是查询topic属于哪个broker和查询分区数
    @ApiModelProperty(
            name = "maxNumberOfRejectedRequestPerConnection",
            value = "Maximum number of rejected requests of a broker in a certain time frame (30 seconds) "
                    + "after the current connection is closed and the client "
                    + "creating a new connection to connect to a different broker."
    )
    private int maxNumberOfRejectedRequestPerConnection = 50;

    // todo channel建立后，创建一个定时心跳任务保活channel，该参数是多久发一次心跳请求
    @ApiModelProperty(
            name = "keepAliveIntervalSeconds",
            value = "Seconds of keeping alive interval for each client broker connection."
    )
    private int keepAliveIntervalSeconds = 30;

    // todo 客户端与服务端的连接超时时间, 10s
    @ApiModelProperty(
            name = "connectionTimeoutMs",
            value = "Duration of waiting for a connection to a broker to be established."
                    + "If the duration passes without a response from a broker, the connection attempt is dropped."
    )
    private int connectionTimeoutMs = 10000;
    // todo pulsar有admin web接口，该参数是http请求的超时时间
    @ApiModelProperty(
            name = "requestTimeoutMs",
            value = "Maximum duration for completing a request."
    )
    private int requestTimeoutMs = 60000;

    // todo 退避初始时间 例如：连接服务端失败了，是不是要重试，那过多久重试呢，重试后又失败了，再过多久重试呢，比如初始1秒，
    //  再失败通过一套计算方式，得出下次重试3秒后，再失败10秒后 就是干这个用的
    @ApiModelProperty(
            name = "initialBackoffIntervalNanos",
            value = "Initial backoff interval (in nanosecond)."
    )
    private long initialBackoffIntervalNanos = TimeUnit.MILLISECONDS.toNanos(100);

    // todo 接上面：不能一直重试，因为越来越久，总得有个封顶然后再从初识或某个点开始 这个就是封顶值
    @ApiModelProperty(
            name = "maxBackoffIntervalNanos",
            value = "Max backoff interval (in nanosecond)."
    )
    private long maxBackoffIntervalNanos = TimeUnit.SECONDS.toNanos(60);

    // todo EpollEventLoop调用select时的等待策略
    @ApiModelProperty(
            name = "enableBusyWait",
            value = "Whether to enable BusyWait for EpollEventLoopGroup."
    )
    private boolean enableBusyWait = false;

    @ApiModelProperty(
            name = "listenerName",
            value = "Listener name for lookup. Clients can use listenerName to choose one of the listeners "
                    + "as the service URL to create a connection to the broker as long as the network is accessible."
                    + "\"advertisedListeners\" must enabled in broker side."
    )
    private String listenerName;

    @ApiModelProperty(
            name = "useKeyStoreTls",
            value = "Set TLS using KeyStore way."
    )
    private boolean useKeyStoreTls = false;
    @ApiModelProperty(
            name = "sslProvider",
            value = "The TLS provider used by an internal client to authenticate with other Pulsar brokers."
    )
    private String sslProvider = null;

    @ApiModelProperty(
            name = "tlsTrustStoreType",
            value = "TLS TrustStore type configuration. You need to set this configuration when client authentication"
                    + " is required."
    )
    private String tlsTrustStoreType = "JKS";

    @ApiModelProperty(
            name = "tlsTrustStorePath",
            value = "Path of TLS TrustStore."
    )
    private String tlsTrustStorePath = null;

    @ApiModelProperty(
            name = "tlsTrustStorePassword",
            value = "Password of TLS TrustStore."
    )

    @Secret
    private String tlsTrustStorePassword = null;

    @ApiModelProperty(
            name = "tlsCiphers",
            value = "Set of TLS Ciphers."
    )
    private Set<String> tlsCiphers = new TreeSet<>();

    @ApiModelProperty(
            name = "tlsProtocols",
            value = "Protocols of TLS."
    )
    private Set<String> tlsProtocols = new TreeSet<>();

    // todo 发送消息后，再没有收到服务端回调数据还在内存中。用于控制同一时刻内存中存在的消息大小限制
    // todo kafka侧则为32M
    @ApiModelProperty(
            name = "memoryLimitBytes",
            value = "Limit of client memory usage (in byte). The 64M default can guarantee a high producer throughput."
    )
    private long memoryLimitBytes = 64 * 1024 * 1024;

    @ApiModelProperty(
            name = "proxyServiceUrl",
            value = "URL of proxy service. proxyServiceUrl and proxyProtocol must be mutually inclusive."
    )
    private String proxyServiceUrl;

    @ApiModelProperty(
            name = "proxyProtocol",
            value = "Protocol of proxy service. proxyServiceUrl and proxyProtocol must be mutually inclusive."
    )
    private ProxyProtocol proxyProtocol;

    // todo 是否开启事务
    @ApiModelProperty(
            name = "enableTransaction",
            value = "Whether to enable transaction."
    )
    private boolean enableTransaction = false;

    @JsonIgnore
    private Clock clock = Clock.systemDefaultZone();

    @ApiModelProperty(
            name = "dnsLookupBindAddress",
            value = "The Pulsar client dns lookup bind address, default behavior is bind on 0.0.0.0"
    )
    private String dnsLookupBindAddress = null;

    @ApiModelProperty(
            name = "dnsLookupBindPort",
            value = "The Pulsar client dns lookup bind port, takes effect when dnsLookupBindAddress is configured,"
                    + " default value is 0."
    )
    private int dnsLookupBindPort = 0;

    // socks5
    @ApiModelProperty(
            name = "socks5ProxyAddress",
            value = "Address of SOCKS5 proxy."
    )
    private InetSocketAddress socks5ProxyAddress;

    @ApiModelProperty(
            name = "socks5ProxyUsername",
            value = "User name of SOCKS5 proxy."
    )
    private String socks5ProxyUsername;

    @ApiModelProperty(
            name = "socks5ProxyUsername",
            value = "Password of SOCKS5 proxy."
    )

    @Secret
    private String socks5ProxyPassword;

    public Authentication getAuthentication() {
        if (authentication == null) {
            this.authentication = AuthenticationDisabled.INSTANCE;
        }
        return authentication;
    }

    public void setAuthentication(Authentication authentication) {
        this.authentication = authentication;
    }
    public boolean isUseTls() {
        if (useTls) {
            return true;
        }
        if (getServiceUrl() != null
                && (this.getServiceUrl().startsWith("pulsar+ssl") || this.getServiceUrl().startsWith("https"))) {
            this.useTls = true;
            return true;
        }
        return false;
    }

    public long getLookupTimeoutMs() {
        if (lookupTimeoutMs >= 0) {
            return lookupTimeoutMs;
        } else {
            // TODO: 10/23/23 默认为30s
            return operationTimeoutMs;
        }
    }

    public ClientConfigurationData clone() {
        try {
            return (ClientConfigurationData) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException("Failed to clone ClientConfigurationData");
        }
    }

    public InetSocketAddress getSocks5ProxyAddress() {
        if (Objects.nonNull(socks5ProxyAddress)) {
            return socks5ProxyAddress;
        }
        String proxyAddress = System.getProperty("socks5Proxy.address");
        return Optional.ofNullable(proxyAddress).map(address -> {
            try {
                URI uri = URI.create(address);
                return new InetSocketAddress(uri.getHost(), uri.getPort());
            } catch (Exception e) {
                throw new RuntimeException("Invalid config [socks5Proxy.address]", e);
            }
        }).orElse(null);
    }

    public String getSocks5ProxyUsername() {
        return Objects.nonNull(socks5ProxyUsername) ? socks5ProxyUsername : System.getProperty("socks5Proxy.username");
    }

    public String getSocks5ProxyPassword() {
        return Objects.nonNull(socks5ProxyPassword) ? socks5ProxyPassword : System.getProperty("socks5Proxy.password");
    }
}
