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

import static org.apache.commons.lang3.StringUtils.isBlank;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.netty.channel.EventLoopGroup;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.client.api.RegexSubscriptionMode;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.TableViewBuilder;
import org.apache.pulsar.client.api.schema.KeyValueSchema;
import org.apache.pulsar.client.api.schema.SchemaInfoProvider;
import org.apache.pulsar.client.api.transaction.TransactionBuilder;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.client.impl.conf.ReaderConfigurationData;
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;
import org.apache.pulsar.client.impl.schema.AutoProduceBytesSchema;
import org.apache.pulsar.client.impl.schema.generic.MultiVersionSchemaInfoProvider;
import org.apache.pulsar.client.impl.transaction.TransactionBuilderImpl;
import org.apache.pulsar.client.impl.transaction.TransactionCoordinatorClientImpl;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace.Mode;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PulsarClientImpl implements PulsarClient {

    private static final Logger log = LoggerFactory.getLogger(PulsarClientImpl.class);

    // default limits for producers when memory limit controller is disabled
    private static final int NO_MEMORY_LIMIT_DEFAULT_MAX_PENDING_MESSAGES = 1000;
    private static final int NO_MEMORY_LIMIT_DEFAULT_MAX_PENDING_MESSAGES_ACROSS_PARTITIONS = 50000;

    // TODO: 10/17/23 配置数据
    protected final ClientConfigurationData conf;
    private final boolean createdExecutorProviders;
    // TODO: 10/17/23 查找服务
    private LookupService lookup;
    // TODO: 10/17/23 连接池
    private final ConnectionPool cnxPool;
    // TODO: 10/17/23 定时器
    @Getter
    private final Timer timer;
    private boolean needStopTimer;
    // TODO: 10/17/23 线程池提供者
    private final ExecutorProvider externalExecutorProvider;
    private final ExecutorProvider internalExecutorProvider;
    private final boolean createdEventLoopGroup;
    private final boolean createdCnxPool;

    public enum State {
        Open, Closing, Closed
    }

    private final AtomicReference<State> state = new AtomicReference<>();
    // TODO: 10/17/23 生产者对象池
    // These sets are updated from multiple threads, so they require a threadsafe data structure
    private final Set<ProducerBase<?>> producers = Collections.newSetFromMap(new ConcurrentHashMap<>());
    // TODO: 10/17/23 消费者对象池
    private final Set<ConsumerBase<?>> consumers = Collections.newSetFromMap(new ConcurrentHashMap<>());

    // TODO: 10/17/23 producerId 生成器
    private final AtomicLong producerIdGenerator = new AtomicLong();
    // TODO: 10/17/23 consumerId生成器
    private final AtomicLong consumerIdGenerator = new AtomicLong();
    // TODO: 10/17/23 requestId生成器
    private final AtomicLong requestIdGenerator =
            new AtomicLong(ThreadLocalRandom.current().nextLong(0, Long.MAX_VALUE / 2));

    protected final EventLoopGroup eventLoopGroup;
    private final MemoryLimitController memoryLimitController;

    private final LoadingCache<String, SchemaInfoProvider> schemaProviderLoadingCache =
            CacheBuilder.newBuilder().maximumSize(100000)
                    .expireAfterAccess(30, TimeUnit.MINUTES)
                    .build(new CacheLoader<String, SchemaInfoProvider>() {

                        @Override
                        public SchemaInfoProvider load(String topicName) {
                            return newSchemaProvider(topicName);
                        }
                    });

    private final Clock clientClock;

    @Getter
    private TransactionCoordinatorClientImpl tcClient;

    public PulsarClientImpl(ClientConfigurationData conf) throws PulsarClientException {
        this(conf, null, null, null, null, null);
    }

    public PulsarClientImpl(ClientConfigurationData conf, EventLoopGroup eventLoopGroup) throws PulsarClientException {
        this(conf, eventLoopGroup, null, null, null, null);
    }

    public PulsarClientImpl(ClientConfigurationData conf, EventLoopGroup eventLoopGroup, ConnectionPool cnxPool)
            throws PulsarClientException {
        this(conf, eventLoopGroup, cnxPool, null, null, null);
    }

    public PulsarClientImpl(ClientConfigurationData conf, EventLoopGroup eventLoopGroup, ConnectionPool cnxPool,
                            Timer timer)
            throws PulsarClientException {
        this(conf, eventLoopGroup, cnxPool, timer, null, null);
    }

    @Builder(builderClassName = "PulsarClientImplBuilder")
    private PulsarClientImpl(ClientConfigurationData conf, EventLoopGroup eventLoopGroup, ConnectionPool connectionPool,
                             Timer timer, ExecutorProvider externalExecutorProvider,
                             ExecutorProvider internalExecutorProvider) throws PulsarClientException {
        EventLoopGroup eventLoopGroupReference = null;
        ConnectionPool connectionPoolReference = null;
        try {
            this.createdEventLoopGroup = eventLoopGroup == null;
            this.createdCnxPool = connectionPool == null;
            if ((externalExecutorProvider == null) != (internalExecutorProvider == null)) {
                throw new IllegalArgumentException(
                        "Both externalExecutorProvider and internalExecutorProvider must be specified or unspecified.");
            }
            this.createdExecutorProviders = externalExecutorProvider == null;
            eventLoopGroupReference = eventLoopGroup != null ? eventLoopGroup : getEventLoopGroup(conf);
            this.eventLoopGroup = eventLoopGroupReference;
            if (conf == null || isBlank(conf.getServiceUrl()) || this.eventLoopGroup == null) {
                throw new PulsarClientException.InvalidConfigurationException("Invalid client configuration");
            }
            setAuth(conf);
            this.conf = conf;
            clientClock = conf.getClock();
            // TODO: 10/17/23 启动认证
            conf.getAuthentication().start();
            connectionPoolReference =
                    connectionPool != null ? connectionPool : new ConnectionPool(conf, this.eventLoopGroup);
            this.cnxPool = connectionPoolReference;
            this.externalExecutorProvider = externalExecutorProvider != null ? externalExecutorProvider :
                    new ExecutorProvider(conf.getNumListenerThreads(), "pulsar-external-listener");
            this.internalExecutorProvider = internalExecutorProvider != null ? internalExecutorProvider :
                    new ExecutorProvider(conf.getNumIoThreads(), "pulsar-client-internal");
            // TODO: 10/17/23 根据协议选择使用哪种方式提供查找服务
            if (conf.getServiceUrl().startsWith("http")) {
                lookup = new HttpLookupService(conf, this.eventLoopGroup);
            } else {
                lookup = new BinaryProtoLookupService(this, conf.getServiceUrl(), conf.getListenerName(),
                        conf.isUseTls(), this.externalExecutorProvider.getExecutor());
            }
            if (timer == null) {
                // TODO: 10/17/23 定时器创建
                this.timer = new HashedWheelTimer(getThreadFactory("pulsar-timer"), 1, TimeUnit.MILLISECONDS);
                needStopTimer = true;
            } else {
                this.timer = timer;
            }

            // TODO: 10/17/23 是否开启事务
            if (conf.isEnableTransaction()) {
                tcClient = new TransactionCoordinatorClientImpl(this);
                try {
                    tcClient.start();
                } catch (Throwable e) {
                    log.error("Start transactionCoordinatorClient error.", e);
                    throw new PulsarClientException(e);
                }
            }

            // TODO: 10/17/23 类似kafka的32M
            memoryLimitController = new MemoryLimitController(conf.getMemoryLimitBytes());
            state.set(State.Open);
        } catch (Throwable t) {
            shutdown();
            shutdownEventLoopGroup(eventLoopGroupReference);
            closeCnxPool(connectionPoolReference);
            throw t;
        }
    }

    private void setAuth(ClientConfigurationData conf) throws PulsarClientException {
        if (StringUtils.isBlank(conf.getAuthPluginClassName())
                || (StringUtils.isBlank(conf.getAuthParams()) && conf.getAuthParamMap() == null)) {
            return;
        }

        // TODO: 10/17/23 如果配置了认证，需要加载认证模块
        if (StringUtils.isNotBlank(conf.getAuthParams())) {
            conf.setAuthentication(AuthenticationFactory.create(conf.getAuthPluginClassName(), conf.getAuthParams()));
        } else if (conf.getAuthParamMap() != null) {
            conf.setAuthentication(AuthenticationFactory.create(conf.getAuthPluginClassName(), conf.getAuthParamMap()));
        }
    }

    public ClientConfigurationData getConfiguration() {
        return conf;
    }

    public Clock getClientClock() {
        return clientClock;
    }

    public AtomicReference<State> getState() {
        return state;
    }

    @Override
    public ProducerBuilder<byte[]> newProducer() {
        return new ProducerBuilderImpl<>(this, Schema.BYTES);
    }

    @Override
    public <T> ProducerBuilder<T> newProducer(Schema<T> schema) {
        ProducerBuilderImpl<T> producerBuilder = new ProducerBuilderImpl<>(this, schema);
        if (!memoryLimitController.isMemoryLimited()) {
            // set default limits for producers when memory limit controller is disabled
            producerBuilder.maxPendingMessages(NO_MEMORY_LIMIT_DEFAULT_MAX_PENDING_MESSAGES);
            producerBuilder.maxPendingMessagesAcrossPartitions(
                    NO_MEMORY_LIMIT_DEFAULT_MAX_PENDING_MESSAGES_ACROSS_PARTITIONS);
        }
        return producerBuilder;
    }

    // TODO: 11/2/23 consumer有两个接口实现
    @Override
    public ConsumerBuilder<byte[]> newConsumer() {
        // TODO: 11/2/23  一个默认Schema.BYTES，用于兼容原来的客户端
        return new ConsumerBuilderImpl<>(this, Schema.BYTES);
    }

    // TODO: 11/2/23  一个自定义Schema类型的，用于自定义。
    @Override
    public <T> ConsumerBuilder<T> newConsumer(Schema<T> schema) {
        return new ConsumerBuilderImpl<>(this, schema);
    }

    @Override
    public ReaderBuilder<byte[]> newReader() {
        return new ReaderBuilderImpl<>(this, Schema.BYTES);
    }

    @Override
    public <T> ReaderBuilder<T> newReader(Schema<T> schema) {
        return new ReaderBuilderImpl<>(this, schema);
    }

    @Override
    public <T> TableViewBuilder<T> newTableViewBuilder(Schema<T> schema) {
        return new TableViewBuilderImpl<>(this, schema);
    }

    public CompletableFuture<Producer<byte[]>> createProducerAsync(ProducerConfigurationData conf) {
        return createProducerAsync(conf, Schema.BYTES, null);
    }

    public <T> CompletableFuture<Producer<T>> createProducerAsync(ProducerConfigurationData conf,  Schema<T> schema) {
        return createProducerAsync(conf, schema, null);
    }

    // TODO: 10/17/23 总结：创建生产者，默认情况下使用BYTES的schema，然后调用createProducerAsync
    @SuppressWarnings("rawtypes")
    public <T> CompletableFuture<Producer<T>> createProducerAsync(ProducerConfigurationData conf, Schema<T> schema,
                                                                  ProducerInterceptors interceptors) {
        // TODO: 10/17/23 检查配置 
        if (conf == null) {
            return FutureUtil.failedFuture(
                new PulsarClientException.InvalidConfigurationException("Producer configuration undefined"));
        }

        // TODO: 10/17/23  如果Schema为自动消费者Schema实例，由于这里是生产者，故抛异常
        if (schema instanceof AutoConsumeSchema) {
            return FutureUtil.failedFuture(
                    new PulsarClientException.InvalidConfigurationException(
                            "AutoConsumeSchema is only used by consumers to detect schemas automatically"));
        }

        // TODO: 10/17/23 Pulsar 客户端是否初始化完成，如果状态不是Open，则抛异常：客户端已关闭。
        if (state.get() != State.Open) {
            return FutureUtil.failedFuture(
                    new PulsarClientException.AlreadyClosedException("Client already closed : state = " + state.get()));
        }

        String topic = conf.getTopicName();

        // TODO: 10/17/23 topic校验
        if (!TopicName.isValid(topic)) {
            return FutureUtil.failedFuture(
                new PulsarClientException.InvalidTopicNameException("Invalid topic name: '" + topic + "'"));
        }

        // TODO: 10/17/23 自动获取Schema
        if (schema instanceof AutoProduceBytesSchema) {
            AutoProduceBytesSchema autoProduceBytesSchema = (AutoProduceBytesSchema) schema;
            if (autoProduceBytesSchema.schemaInitialized()) {
                // TODO: 2/24/23 创建producer
                return createProducerAsync(topic, conf, schema, interceptors);
            }
            // TODO: 10/17/23 这里就是查找服务，通过Topic查询注册的Schema，如果没注册，则默认Schema.BYTES
            return lookup.getSchema(TopicName.get(conf.getTopicName()))
                    .thenCompose(schemaInfoOptional -> {
                        if (schemaInfoOptional.isPresent()) {
                            autoProduceBytesSchema.setSchema(Schema.getSchema(schemaInfoOptional.get()));
                        } else {
                            autoProduceBytesSchema.setSchema(Schema.BYTES);
                        }
                        return createProducerAsync(topic, conf, schema, interceptors);
                    });
        } else {
            return createProducerAsync(topic, conf, schema, interceptors);
        }

    }

    private <T> CompletableFuture<Producer<T>> createProducerAsync(String topic,
                                                                   ProducerConfigurationData conf,
                                                                   Schema<T> schema,
                                                                   ProducerInterceptors interceptors) {
        CompletableFuture<Producer<T>> producerCreatedFuture = new CompletableFuture<>();

        // TODO: 2/24/23 请求topic的metadata
        getPartitionedTopicMetadata(topic).thenAccept(metadata -> {
            if (log.isDebugEnabled()) {
                // TODO: 2/24/23 从broker侧知道，broker返回partition的number
                log.debug("[{}] Received topic metadata. partitions: {}", topic, metadata.partitions);
            }

            ProducerBase<T> producer;
            if (metadata.partitions > 0) {
                // TODO: 2/24/23 根据每个partition创建一个ProducerImpl实例
                producer = newPartitionedProducerImpl(topic, conf, schema, interceptors, producerCreatedFuture,
                        metadata);
            } else {
                // TODO: 2/24/23 由于partition只有1个partition，因此只会创建一个ProducerImpl实例
                producer = newProducerImpl(topic, -1, conf, schema, interceptors, producerCreatedFuture,
                        Optional.empty());
            }
            // TODO: 10/17/23 生产者对象池加入创建的生产者对象
            producers.add(producer);
        }).exceptionally(ex -> {
            log.warn("[{}] Failed to get partitioned topic metadata: {}", topic, ex.getMessage());
            producerCreatedFuture.completeExceptionally(ex);
            return null;
        });

        return producerCreatedFuture;
    }

    /**
     * Factory method for creating PartitionedProducerImpl instance.
     *
     * Allows overriding the PartitionedProducerImpl instance in tests.
     *
     * @param topic topic name
     * @param conf producer configuration
     * @param schema topic schema
     * @param interceptors producer interceptors
     * @param producerCreatedFuture future for signaling completion of async producer creation
     * @param metadata partitioned topic metadata
     * @param <T> message type class
     * @return new PartitionedProducerImpl instance
     */
    protected <T> PartitionedProducerImpl<T> newPartitionedProducerImpl(String topic,
                                                                        ProducerConfigurationData conf,
                                                                        Schema<T> schema,
                                                                        ProducerInterceptors interceptors,
                                                                        CompletableFuture<Producer<T>>
                                                                                producerCreatedFuture,
                                                                        PartitionedTopicMetadata metadata) {
        return new PartitionedProducerImpl<>(PulsarClientImpl.this, topic, conf, metadata.partitions,
                producerCreatedFuture, schema, interceptors);
    }

    /**
     * Factory method for creating ProducerImpl instance.
     *
     * Allows overriding the ProducerImpl instance in tests.
     *
     * @param topic topic name
     * @param partitionIndex partition index of a partitioned topic. the value -1 is used for non-partitioned topics.
     * @param conf producer configuration
     * @param schema topic schema
     * @param interceptors producer interceptors
     * @param producerCreatedFuture future for signaling completion of async producer creation
     * @param <T> message type class
     *
     * @return a producer instance
     */
    protected <T> ProducerImpl<T> newProducerImpl(String topic, int partitionIndex,
                                                  ProducerConfigurationData conf,
                                                  Schema<T> schema,
                                                  ProducerInterceptors interceptors,
                                                  CompletableFuture<Producer<T>> producerCreatedFuture,
                                                  Optional<String> overrideProducerName) {
        return new ProducerImpl<>(PulsarClientImpl.this, topic, conf, producerCreatedFuture, partitionIndex, schema,
                interceptors, overrideProducerName);
    }

    public CompletableFuture<Consumer<byte[]>> subscribeAsync(ConsumerConfigurationData<byte[]> conf) {
        return subscribeAsync(conf, Schema.BYTES, null);
    }

    public <T> CompletableFuture<Consumer<T>> subscribeAsync(ConsumerConfigurationData<T> conf, Schema<T> schema,
                                                             ConsumerInterceptors<T> interceptors) {
        // TODO: 10/17/23 客户端状态检测
        if (state.get() != State.Open) {
            return FutureUtil.failedFuture(new PulsarClientException.AlreadyClosedException("Client already closed"));
        }

        if (conf == null) {
            return FutureUtil.failedFuture(
                    new PulsarClientException.InvalidConfigurationException("Consumer configuration undefined"));
        }

        // TODO: 10/17/23 topic名称检测
        for (String topic : conf.getTopicNames()) {
            if (!TopicName.isValid(topic)) {
                return FutureUtil.failedFuture(
                        new PulsarClientException.InvalidTopicNameException("Invalid topic name: '" + topic + "'"));
            }
        }

        // TODO: 10/17/23 subscription name检测
        if (isBlank(conf.getSubscriptionName())) {
            return FutureUtil
                    .failedFuture(new PulsarClientException.InvalidConfigurationException("Empty subscription name"));
        }

        // TODO: 10/17/23 读取压缩特性仅仅被用订阅持久化 Topic，并且订阅类型为独占或故障转移模式
        if (conf.isReadCompacted() && (!conf.getTopicNames().stream()
                .allMatch(topic -> TopicName.get(topic).getDomain() == TopicDomain.persistent)
                || (conf.getSubscriptionType() != SubscriptionType.Exclusive
                        && conf.getSubscriptionType() != SubscriptionType.Failover))) {
            return FutureUtil.failedFuture(new PulsarClientException.InvalidConfigurationException(
                    "Read compacted can only be used with exclusive or failover persistent subscriptions"));
        }

        // TODO: 10/17/23 事件监听器只允许在故障转移订阅模式下
        if (conf.getConsumerEventListener() != null && conf.getSubscriptionType() != SubscriptionType.Failover) {
            return FutureUtil.failedFuture(new PulsarClientException.InvalidConfigurationException(
                    "Active consumer listener is only supported for failover subscription"));
        }

        // TODO: 10/17/23 如果已经设置了 正则表达式订阅 Topic，就不能指定具体的 Topic 
        if (conf.getTopicsPattern() != null) {
            // If use topicsPattern, we should not use topic(), and topics() method.
            if (!conf.getTopicNames().isEmpty()){
                return FutureUtil
                    .failedFuture(new IllegalArgumentException("Topic names list must be null when use topicsPattern"));
            }
            // TODO: 10/17/23 正则订阅 
            return patternTopicSubscribeAsync(conf, schema, interceptors);
        } else if (conf.getTopicNames().size() == 1) {
            // TODO: 10/17/23 单个topic订阅 
            return singleTopicSubscribeAsync(conf, schema, interceptors);
        } else {
            // TODO: 10/17/23 多个topic订阅 
            return multiTopicSubscribeAsync(conf, schema, interceptors);
        }
    }

    private <T> CompletableFuture<Consumer<T>> singleTopicSubscribeAsync(ConsumerConfigurationData<T> conf,
                                                                         Schema<T> schema,
                                                                         ConsumerInterceptors<T> interceptors) {
        // TODO: 10/17/23 获取schema
        return preProcessSchemaBeforeSubscribe(this, schema, conf.getSingleTopic())
                .thenCompose(schemaClone -> doSingleTopicSubscribeAsync(conf, schemaClone, interceptors));
    }

    private <T> CompletableFuture<Consumer<T>> doSingleTopicSubscribeAsync(ConsumerConfigurationData<T> conf,
                                                                           Schema<T> schema,
                                                                           ConsumerInterceptors<T> interceptors) {
        CompletableFuture<Consumer<T>> consumerSubscribedFuture = new CompletableFuture<>();

        // TODO: 10/17/23 获取到topic 
        String topic = conf.getSingleTopic();

        // TODO: 10/17/23 获取topic的metadata信息 
        getPartitionedTopicMetadata(topic).thenAccept(metadata -> {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Received topic metadata. partitions: {}", topic, metadata.partitions);
            }

            ConsumerBase<T> consumer;
            if (metadata.partitions > 0) {
                // TODO: 10/17/23 多分区topic的consumer创建
                consumer = MultiTopicsConsumerImpl.createPartitionedConsumer(PulsarClientImpl.this, conf,
                        externalExecutorProvider, consumerSubscribedFuture, metadata.partitions, schema, interceptors);
            } else {
                int partitionIndex = TopicName.getPartitionIndex(topic);
                // TODO: 10/17/23 单分区topic的consumer创建 
                consumer = ConsumerImpl.newConsumerImpl(PulsarClientImpl.this, topic, conf, externalExecutorProvider,
                        partitionIndex, false, consumerSubscribedFuture, null, schema, interceptors,
                        true /* createTopicIfDoesNotExist */);
            }
            // TODO: 10/17/23 消费者对象池加入新创建的consumer对象
            consumers.add(consumer);
        }).exceptionally(ex -> {
            log.warn("[{}] Failed to get partitioned topic metadata", topic, ex);
            consumerSubscribedFuture.completeExceptionally(ex);
            return null;
        });

        return consumerSubscribedFuture;
    }

    private <T> CompletableFuture<Consumer<T>> multiTopicSubscribeAsync(ConsumerConfigurationData<T> conf,
                                                                        Schema<T> schema,
                                                                        ConsumerInterceptors<T> interceptors) {
        CompletableFuture<Consumer<T>> consumerSubscribedFuture = new CompletableFuture<>();

        // TODO: 10/17/23 多个topic订阅，这里是真正的多topic订阅，但可能其中0或多个topic是多分区
        ConsumerBase<T> consumer = new MultiTopicsConsumerImpl<>(PulsarClientImpl.this, conf,
                externalExecutorProvider, consumerSubscribedFuture, schema, interceptors,
                true /* createTopicIfDoesNotExist */);

        // TODO: 10/17/23 消费者对象池加入刚创建的consumer对象
        consumers.add(consumer);

        return consumerSubscribedFuture;
    }

    public CompletableFuture<Consumer<byte[]>> patternTopicSubscribeAsync(ConsumerConfigurationData<byte[]> conf) {
        return patternTopicSubscribeAsync(conf, Schema.BYTES, null);
    }

    private <T> CompletableFuture<Consumer<T>> patternTopicSubscribeAsync(ConsumerConfigurationData<T> conf,
            Schema<T> schema, ConsumerInterceptors<T> interceptors) {
        // TODO: 10/17/23 正则表达式
        String regex = conf.getTopicsPattern().pattern();
        Mode subscriptionMode = convertRegexSubscriptionMode(conf.getRegexSubscriptionMode());
        TopicName destination = TopicName.get(regex);
        // TODO: 10/17/23 有正则表达式解析出 Namespace ，例如：persistent://public/default/.*
        NamespaceName namespaceName = destination.getNamespaceObject();

        CompletableFuture<Consumer<T>> consumerSubscribedFuture = new CompletableFuture<>();
        // TODO: 10/17/23 根据 Namespace 和 订阅模式来查找所有符合条件的 Topic
        lookup.getTopicsUnderNamespace(namespaceName, subscriptionMode)
            .thenAccept(topics -> {
                if (log.isDebugEnabled()) {
                    log.debug("Get topics under namespace {}, topics.size: {}", namespaceName, topics.size());
                    topics.forEach(topicName ->
                        log.debug("Get topics under namespace {}, topic: {}", namespaceName, topicName));
                }

                // TODO: 10/17/23  根据正则filter出topic
                List<String> topicsList = topicsPatternFilter(topics, conf.getTopicsPattern());
                // TODO: 10/17/23 把filter出来的topic加入到我们配置的 topic里面
                conf.getTopicNames().addAll(topicsList);
                // TODO: 10/17/23 创建 consumer
                ConsumerBase<T> consumer = new PatternMultiTopicsConsumerImpl<>(conf.getTopicsPattern(),
                        PulsarClientImpl.this,
                        conf,
                        externalExecutorProvider,
                        consumerSubscribedFuture,
                        schema, subscriptionMode, interceptors);
                // TODO: 10/17/23 消费者对象池加入新创建的consumer对象
                consumers.add(consumer);
            })
            .exceptionally(ex -> {
                log.warn("[{}] Failed to get topics under namespace", namespaceName);
                consumerSubscribedFuture.completeExceptionally(ex);
                return null;
            });

        return consumerSubscribedFuture;
    }

    // get topics that match 'topicsPattern' from original topics list
    // return result should contain only topic names, without partition part
    public static List<String> topicsPatternFilter(List<String> original, Pattern topicsPattern) {
        final Pattern shortenedTopicsPattern = topicsPattern.toString().contains("://")
            ? Pattern.compile(topicsPattern.toString().split("\\:\\/\\/")[1]) : topicsPattern;

        // TODO: 10/17/23 从原Topic列表中匹配 ‘Topic正则表达式’，结果返回仅仅是 Topic 名，无分区部分 
        return original.stream()
            .map(TopicName::get)
            .map(TopicName::toString)
            .filter(topic -> shortenedTopicsPattern.matcher(topic.split("\\:\\/\\/")[1]).matches())
            .collect(Collectors.toList());
    }

    public CompletableFuture<Reader<byte[]>> createReaderAsync(ReaderConfigurationData<byte[]> conf) {
        return createReaderAsync(conf, Schema.BYTES);
    }

    public <T> CompletableFuture<Reader<T>> createReaderAsync(ReaderConfigurationData<T> conf, Schema<T> schema) {
        // TODO: 10/17/23 客户端状态检测 
        if (state.get() != State.Open) {
            return FutureUtil.failedFuture(new PulsarClientException.AlreadyClosedException("Client already closed"));
        }
        // TODO: 10/17/23 配置检测 
        if (conf == null) {
            return FutureUtil.failedFuture(
                    new PulsarClientException.InvalidConfigurationException("Consumer configuration undefined"));
        }
        // TODO: 10/17/23 topic检测 
        for (String topic : conf.getTopicNames()) {
            if (!TopicName.isValid(topic)) {
                return FutureUtil.failedFuture(new PulsarClientException
                        .InvalidTopicNameException("Invalid topic name: '" + topic + "'"));
            }
        }

        // TODO: 10/17/23 开始msgid检测 
        if (conf.getStartMessageId() == null) {
            return FutureUtil
                    .failedFuture(new PulsarClientException.InvalidConfigurationException("Invalid startMessageId"));
        }

        // TODO: 10/17/23 topic数量为1 
        if (conf.getTopicNames().size() == 1) {
            // TODO: 10/17/23 schema检测 
            return preProcessSchemaBeforeSubscribe(this, schema, conf.getTopicName())
                    .thenCompose(schemaClone -> createSingleTopicReaderAsync(conf, schemaClone));
        }
        // TODO: 10/17/23 topic数量为多个 
        return createMultiTopicReaderAsync(conf, schema);
    }

    protected <T> CompletableFuture<Reader<T>> createMultiTopicReaderAsync(
            ReaderConfigurationData<T> conf, Schema<T> schema) {
        CompletableFuture<Reader<T>> readerFuture = new CompletableFuture<>();
        CompletableFuture<Consumer<T>> consumerSubscribedFuture = new CompletableFuture<>();
        // TODO: 10/17/23 多个topic的reader创建
        MultiTopicsReaderImpl<T> reader = new MultiTopicsReaderImpl<>(this,
                conf, externalExecutorProvider, consumerSubscribedFuture, schema);
        // TODO: 10/17/23 reader的consumer
        ConsumerBase<T> consumer = reader.getMultiTopicsConsumer();
        // TODO: 10/17/23 把consumer放入到消费者对象池
        consumers.add(consumer);
        consumerSubscribedFuture.thenRun(() -> readerFuture.complete(reader))
                .exceptionally(ex -> {
                    log.warn("Failed to create multiTopicReader", ex);
                    readerFuture.completeExceptionally(ex);
                    return null;
                });
        return readerFuture;
    }

    protected <T> CompletableFuture<Reader<T>> createSingleTopicReaderAsync(
            ReaderConfigurationData<T> conf, Schema<T> schema) {
        // TODO: 10/17/23 单个topicReader创建 
        String topic = conf.getTopicName();

        CompletableFuture<Reader<T>> readerFuture = new CompletableFuture<>();

        // TODO: 10/17/23 topic metadata获取 
        getPartitionedTopicMetadata(topic).thenAccept(metadata -> {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Received topic metadata. partitions: {}", topic, metadata.partitions);
            }
            if (metadata.partitions > 0
                    && MultiTopicsConsumerImpl.isIllegalMultiTopicsMessageId(conf.getStartMessageId())) {
                readerFuture.completeExceptionally(
                        new PulsarClientException("The partitioned topic startMessageId is illegal"));
                return;
            }
            CompletableFuture<Consumer<T>> consumerSubscribedFuture = new CompletableFuture<>();
            Reader<T> reader;
            ConsumerBase<T> consumer;
            if (metadata.partitions > 0) {
                // TODO: 10/17/23 多分区topic reader创建 
                reader = new MultiTopicsReaderImpl<>(PulsarClientImpl.this,
                        conf, externalExecutorProvider, consumerSubscribedFuture, schema);
                consumer = ((MultiTopicsReaderImpl<T>) reader).getMultiTopicsConsumer();
            } else {
                // TODO: 10/17/23 单分区topic reader创建 
                reader = new ReaderImpl<>(PulsarClientImpl.this, conf, externalExecutorProvider,
                        consumerSubscribedFuture, schema);
                consumer = ((ReaderImpl<T>) reader).getConsumer();
            }

            // TODO: 10/17/23 消费者对象池放入刚刚创建的consumer对象 
            consumers.add(consumer);

            consumerSubscribedFuture.thenRun(() -> readerFuture.complete(reader)).exceptionally(ex -> {
                log.warn("[{}] Failed to get create topic reader", topic, ex);
                readerFuture.completeExceptionally(ex);
                return null;
            });
        }).exceptionally(ex -> {
            log.warn("[{}] Failed to get partitioned topic metadata", topic, ex);
            readerFuture.completeExceptionally(ex);
            return null;
        });

        return readerFuture;
    }

    /**
     * Read the schema information for a given topic.
     *
     * If the topic does not exist or it has no schema associated, it will return an empty response
     */
    public CompletableFuture<Optional<SchemaInfo>> getSchema(String topic) {
        TopicName topicName;
        try {
            topicName = TopicName.get(topic);
        } catch (Throwable t) {
            return FutureUtil
                    .failedFuture(
                            new PulsarClientException.InvalidTopicNameException("Invalid topic name: '" + topic + "'"));
        }

        return lookup.getSchema(topicName);
    }

    @Override
    public void close() throws PulsarClientException {
        try {
            // TODO: 10/17/23 调用异步close方法
            closeAsync().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw PulsarClientException.unwrap(e);
        } catch (ExecutionException e) {
            PulsarClientException unwrapped = PulsarClientException.unwrap(e);
            if (unwrapped instanceof PulsarClientException.AlreadyClosedException) {
                // this is not a problem
                return;
            }
            throw unwrapped;
        }
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        log.info("Client closing. URL: {}", lookup.getServiceUrl());
        // TODO: 10/17/23 校验状态，如果不是Open状态，抛异常
        if (!state.compareAndSet(State.Open, State.Closing)) {
            return FutureUtil.failedFuture(new PulsarClientException.AlreadyClosedException("Client already closed"));
        }

        final CompletableFuture<Void> closeFuture = new CompletableFuture<>();
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        // TODO: 10/17/23 依次（异步）关闭生产者，消费者
        producers.forEach(p -> futures.add(p.closeAsync()));
        consumers.forEach(c -> futures.add(c.closeAsync()));

        // TODO: 10/17/23 等待所有的生产者消费者正常关闭，这样就优雅的关闭客户端了
        // Need to run the shutdown sequence in a separate thread to prevent deadlocks
        // If there are consumers or producers that need to be shutdown we cannot use the same thread
        // to shutdown the EventLoopGroup as well as that would be trying to shutdown itself thus a deadlock
        // would happen
        FutureUtil.waitForAll(futures).thenRun(() -> new Thread(() -> {
            // All producers & consumers are now closed, we can stop the client safely
            try {
                shutdown();
                closeFuture.complete(null);
                state.set(State.Closed);
            } catch (PulsarClientException e) {
                closeFuture.completeExceptionally(e);
            }
        }, "pulsar-client-shutdown-thread").start()).exceptionally(exception -> {
            closeFuture.completeExceptionally(exception);
            return null;
        });

        return closeFuture;
    }

    @Override
    public void shutdown() throws PulsarClientException {
        try {
            // We will throw the last thrown exception only, though logging all of them.
            Throwable throwable = null;
            if (lookup != null) {
                try {
                    // TODO: 10/17/23 关闭lookup服务
                    lookup.close();
                } catch (Throwable t) {
                    log.warn("Failed to shutdown lookup", t);
                    throwable = t;
                }
            }
            if (tcClient != null) {
                try {
                    // TODO: 10/17/23 事务关闭
                    tcClient.close();
                } catch (Throwable t) {
                    log.warn("Failed to close tcClient");
                    throwable = t;
                }
            }

            // close the service url provider allocated resource.
            if (conf != null && conf.getServiceUrlProvider() != null) {
                conf.getServiceUrlProvider().close();
            }

            try {
                // Shutting down eventLoopGroup separately because in some cases, cnxPool might be using different
                // eventLoopGroup.
                shutdownEventLoopGroup(eventLoopGroup);
            } catch (PulsarClientException e) {
                log.warn("Failed to shutdown eventLoopGroup", e);
                throwable = e;
            }
            try {
                // TODO: 10/17/23 连接池关闭
                closeCnxPool(cnxPool);
            } catch (PulsarClientException e) {
                log.warn("Failed to shutdown cnxPool", e);
                throwable = e;
            }
            if (timer != null && needStopTimer) {
                try {
                    // TODO: 10/17/23 定时器关闭
                    timer.stop();
                } catch (Throwable t) {
                    log.warn("Failed to shutdown timer", t);
                    throwable = t;
                }
            }
            try {
                // TODO: 10/17/23 执行者关闭
                shutdownExecutors();
            } catch (PulsarClientException e) {
                throwable = e;
            }
            if (conf != null && conf.getAuthentication() != null) {
                try {
                    // TODO: 10/17/23 认证关闭
                    conf.getAuthentication().close();
                } catch (Throwable t) {
                    log.warn("Failed to close authentication", t);
                    throwable = t;
                }
            }
            if (throwable != null) {
                throw throwable;
            }
        } catch (Throwable t) {
            log.warn("Failed to shutdown Pulsar client", t);
            throw PulsarClientException.unwrap(t);
        }
    }

    private void closeCnxPool(ConnectionPool cnxPool) throws PulsarClientException {
        if (createdCnxPool && cnxPool != null) {
            try {
                cnxPool.close();
            } catch (Throwable t) {
                throw PulsarClientException.unwrap(t);
            }
        }
    }

    private void shutdownEventLoopGroup(EventLoopGroup eventLoopGroup) throws PulsarClientException {
        if (createdEventLoopGroup && eventLoopGroup != null && !eventLoopGroup.isShutdown()) {
            try {
                eventLoopGroup.shutdownGracefully().get();
            } catch (Throwable t) {
                throw PulsarClientException.unwrap(t);
            }
        }
    }

    private void shutdownExecutors() throws PulsarClientException {
        if (createdExecutorProviders) {
            PulsarClientException pulsarClientException = null;

            if (externalExecutorProvider != null && !externalExecutorProvider.isShutdown()) {
                try {
                    externalExecutorProvider.shutdownNow();
                } catch (Throwable t) {
                    log.warn("Failed to shutdown externalExecutorProvider", t);
                    pulsarClientException = PulsarClientException.unwrap(t);
                }
            }
            if (internalExecutorProvider != null && !internalExecutorProvider.isShutdown()) {
                try {
                    internalExecutorProvider.shutdownNow();
                } catch (Throwable t) {
                    log.warn("Failed to shutdown internalExecutorService", t);
                    pulsarClientException = PulsarClientException.unwrap(t);
                }
            }

            if (pulsarClientException != null) {
                throw pulsarClientException;
            }
        }
    }

    @Override
    public boolean isClosed() {
        State currentState = state.get();
        return currentState == State.Closed || currentState == State.Closing;
    }

    @Override
    public synchronized void updateServiceUrl(String serviceUrl) throws PulsarClientException {
        log.info("Updating service URL to {}", serviceUrl);

        conf.setServiceUrl(serviceUrl);
        lookup.updateServiceUrl(serviceUrl);
        cnxPool.closeAllConnections();
    }

    public void updateAuthentication(Authentication authentication) throws IOException {
        log.info("Updating authentication to {}", authentication);
        if (conf.getAuthentication() != null) {
            conf.getAuthentication().close();
        }
        conf.setAuthentication(authentication);
        conf.getAuthentication().start();
    }

    public void updateTlsTrustCertsFilePath(String tlsTrustCertsFilePath) {
        log.info("Updating tlsTrustCertsFilePath to {}", tlsTrustCertsFilePath);
        conf.setTlsTrustCertsFilePath(tlsTrustCertsFilePath);
    }

    public void updateTlsTrustStorePathAndPassword(String tlsTrustStorePath, String tlsTrustStorePassword) {
        log.info("Updating tlsTrustStorePath to {}, tlsTrustStorePassword to *****", tlsTrustStorePath);
        conf.setTlsTrustStorePath(tlsTrustStorePath);
        conf.setTlsTrustStorePassword(tlsTrustStorePassword);
    }

    public CompletableFuture<ClientCnx> getConnection(final String topic) {
        TopicName topicName = TopicName.get(topic);
        // TODO: 2/24/23 lookup topic所在的broker, 然后创建producer到broker的连接,
        //  从查找服务里读取存活的broker 地址，异步返回 ClientCnx ，根据成功与失败回调 Connection 接口中2个方法。
        return lookup.getBroker(topicName)
                .thenCompose(pair -> getConnection(pair.getLeft(), pair.getRight()));
    }

    // TODO: 10/18/23 从连接池获取连接,这连接可能是从连接池里获取，或者是直接创建
    //  关于两个地址的原因，当没有代理层时，2个地址是相同的，其实第一个地址作为broker 的标记，
    //  第二个地址去连接，当有代理层时，两个地址不一样的，实际上，连接池使用logicalAddress作为决定是否重用特定连接。
    public CompletableFuture<ClientCnx> getConnection(final InetSocketAddress logicalAddress,
                                                      final InetSocketAddress physicalAddress) {
        // TODO: 2/24/23 producer得到topic所在的broker地址之后，创建到对应broker的连接，连接建立之后，
        //  回调ProducerImpl的connectionOpened方法
        return cnxPool.getConnection(logicalAddress, physicalAddress);
    }

    /** visible for pulsar-functions. **/
    public Timer timer() {
        return timer;
    }

    public ExecutorProvider externalExecutorProvider() {
        return externalExecutorProvider;
    }

    long newProducerId() {
        return producerIdGenerator.getAndIncrement();
    }

    long newConsumerId() {
        return consumerIdGenerator.getAndIncrement();
    }

    public long newRequestId() {
        return requestIdGenerator.getAndIncrement();
    }

    public ConnectionPool getCnxPool() {
        return cnxPool;
    }

    public EventLoopGroup eventLoopGroup() {
        return eventLoopGroup;
    }

    public LookupService getLookup() {
        return lookup;
    }

    public void reloadLookUp() throws PulsarClientException {
        if (conf.getServiceUrl().startsWith("http")) {
            lookup = new HttpLookupService(conf, eventLoopGroup);
        } else {
            lookup = new BinaryProtoLookupService(this, conf.getServiceUrl(), conf.getListenerName(), conf.isUseTls(),
                    externalExecutorProvider.getExecutor());
        }
    }

    public CompletableFuture<Integer> getNumberOfPartitions(String topic) {
        return getPartitionedTopicMetadata(topic).thenApply(metadata -> metadata.partitions);
    }

    public CompletableFuture<PartitionedTopicMetadata> getPartitionedTopicMetadata(String topic) {

        CompletableFuture<PartitionedTopicMetadata> metadataFuture = new CompletableFuture<>();

        try {
            TopicName topicName = TopicName.get(topic);
            AtomicLong opTimeoutMs = new AtomicLong(conf.getLookupTimeoutMs());
            Backoff backoff = new BackoffBuilder()
                    .setInitialTime(conf.getInitialBackoffIntervalNanos(), TimeUnit.NANOSECONDS) // 100ms
                    .setMandatoryStop(opTimeoutMs.get() * 2, TimeUnit.MILLISECONDS)
                    .setMax(conf.getMaxBackoffIntervalNanos(), TimeUnit.NANOSECONDS)// 60s
                    .create();
            // TODO: 2/24/23 获取topic的metadata信息
            getPartitionedTopicMetadata(topicName, backoff, opTimeoutMs,
                                        metadataFuture, new ArrayList<>());
        } catch (IllegalArgumentException e) {
            return FutureUtil.failedFuture(new PulsarClientException.InvalidConfigurationException(e.getMessage()));
        }
        return metadataFuture;
    }

    private void getPartitionedTopicMetadata(TopicName topicName,
                                             Backoff backoff,
                                             AtomicLong remainingTime,
                                             CompletableFuture<PartitionedTopicMetadata> future,
                                             List<Throwable> previousExceptions) {
        long startTime = System.nanoTime();
        // TODO: 2/24/23 获取topic的metadata信息
        lookup.getPartitionedTopicMetadata(topicName).thenAccept(future::complete).exceptionally(e -> {
            remainingTime.addAndGet(-1 * TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime));
            long nextDelay = Math.min(backoff.next(), remainingTime.get());
            // skip retry scheduler when set lookup throttle in client or server side which will lead to
            // `TooManyRequestsException`
            boolean isLookupThrottling = !PulsarClientException.isRetriableError(e.getCause())
                || e.getCause() instanceof PulsarClientException.AuthenticationException;
            if (nextDelay <= 0 || isLookupThrottling) {
                PulsarClientException.setPreviousExceptions(e, previousExceptions);
                future.completeExceptionally(e);
                return null;
            }
            previousExceptions.add(e);

            ((ScheduledExecutorService) externalExecutorProvider.getExecutor()).schedule(() -> {
                log.warn("[topic: {}] Could not get connection while getPartitionedTopicMetadata -- "
                        + "Will try again in {} ms", topicName, nextDelay);
                remainingTime.addAndGet(-nextDelay);
                getPartitionedTopicMetadata(topicName, backoff, remainingTime, future, previousExceptions);
            }, nextDelay, TimeUnit.MILLISECONDS);
            return null;
        });
    }

    @Override
    public CompletableFuture<List<String>> getPartitionsForTopic(String topic) {
        // TODO: 10/23/23 通过 Topic 来获取其分区数
        return getPartitionedTopicMetadata(topic).thenApply(metadata -> {
            if (metadata.partitions > 0) {
                TopicName topicName = TopicName.get(topic);
                List<String> partitions = new ArrayList<>(metadata.partitions);
                for (int i = 0; i < metadata.partitions; i++) {
                    partitions.add(topicName.getPartition(i).toString());
                }
                return partitions;
            } else {
                return Collections.singletonList(topic);
            }
        });
    }

    private static EventLoopGroup getEventLoopGroup(ClientConfigurationData conf) {
        ThreadFactory threadFactory = getThreadFactory("pulsar-client-io");
        return EventLoopUtil.newEventLoopGroup(conf.getNumIoThreads(), conf.isEnableBusyWait(), threadFactory);
    }

    private static ThreadFactory getThreadFactory(String poolName) {
        return new DefaultThreadFactory(poolName, Thread.currentThread().isDaemon());
    }

    void cleanupProducer(ProducerBase<?> producer) {
        producers.remove(producer);
    }

    void cleanupConsumer(ConsumerBase<?> consumer) {
        consumers.remove(consumer);
    }

    @VisibleForTesting
    int producersCount() {
        return producers.size();
    }

    @VisibleForTesting
    int consumersCount() {
        return consumers.size();
    }

    private static Mode convertRegexSubscriptionMode(RegexSubscriptionMode regexSubscriptionMode) {
        switch (regexSubscriptionMode) {
        case PersistentOnly:
            return Mode.PERSISTENT;
        case NonPersistentOnly:
            return Mode.NON_PERSISTENT;
        case AllTopics:
            return Mode.ALL;
        default:
            return null;
        }
    }

    private SchemaInfoProvider newSchemaProvider(String topicName) {
        return new MultiVersionSchemaInfoProvider(TopicName.get(topicName), this);
    }

    public LoadingCache<String, SchemaInfoProvider> getSchemaProviderLoadingCache() {
        return schemaProviderLoadingCache;
    }

    public MemoryLimitController getMemoryLimitController() {
        return memoryLimitController;
    }

    @SuppressWarnings("unchecked")
    protected <T> CompletableFuture<Schema<T>> preProcessSchemaBeforeSubscribe(PulsarClientImpl pulsarClientImpl,
                                                                      Schema<T> schema,
                                                                      String topicName) {
        // TODO: 10/17/23  如果 schema 是 AutoConsumeSchema 的实例，尝试自动获取 Schema 类型
        if (schema != null && schema.supportSchemaVersioning()) {
            final SchemaInfoProvider schemaInfoProvider;
            try {
                schemaInfoProvider = pulsarClientImpl.getSchemaProviderLoadingCache().get(topicName);
            } catch (ExecutionException e) {
                log.error("Failed to load schema info provider for topic {}", topicName, e);
                return FutureUtil.failedFuture(e.getCause());
            }
            schema = schema.clone();
            if (schema.requireFetchingSchemaInfo()) {
                @SuppressWarnings("rawtypes") Schema finalSchema = schema;
                return schemaInfoProvider.getLatestSchema().thenCompose(schemaInfo -> {
                    if (null == schemaInfo) {
                        if (!(finalSchema instanceof AutoConsumeSchema)
                            && !(finalSchema instanceof KeyValueSchema)) {
                            // no schema info is found
                            return FutureUtil.failedFuture(
                                    new PulsarClientException.NotFoundException(
                                            "No latest schema found for topic " + topicName));
                        }
                    }
                    try {
                        log.info("Configuring schema for topic {} : {}", topicName, schemaInfo);
                        finalSchema.configureSchemaInfo(topicName, "topic", schemaInfo);
                    } catch (RuntimeException re) {
                        return FutureUtil.failedFuture(re);
                    }
                    finalSchema.setSchemaInfoProvider(schemaInfoProvider);
                    return CompletableFuture.completedFuture(finalSchema);
                });
            } else {
                schema.setSchemaInfoProvider(schemaInfoProvider);
            }
        }
        return CompletableFuture.completedFuture(schema);
    }

    public ExecutorService getInternalExecutorService() {
        return internalExecutorProvider.getExecutor();
    }
    //
    // Transaction related API
    //

    // This method should be exposed in the PulsarClient interface. Only expose it when all the transaction features
    // are completed.
    // @Override
    public TransactionBuilder newTransaction() throws PulsarClientException {
        if (!conf.isEnableTransaction()) {
            throw new PulsarClientException.InvalidConfigurationException("Transactions are not enabled");
        }
        return new TransactionBuilderImpl(this, tcClient);
    }
}
