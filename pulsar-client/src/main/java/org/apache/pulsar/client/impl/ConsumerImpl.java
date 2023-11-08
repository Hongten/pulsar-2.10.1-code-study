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

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.pulsar.common.protocol.Commands.DEFAULT_CONSUMER_EPOCH;
import static org.apache.pulsar.common.protocol.Commands.hasChecksum;
import static org.apache.pulsar.common.util.Runnables.catchingAndLoggingThrowables;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Iterables;
import com.scurrilous.circe.checksum.Crc32cIntChecksum;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.Timeout;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageCrypto;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientException.TopicDoesNotExistException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.impl.crypto.MessageCryptoBc;
import org.apache.pulsar.client.impl.transaction.TransactionImpl;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.client.util.RetryMessageUtil;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.EncryptionContext;
import org.apache.pulsar.common.api.EncryptionContext.EncryptionKey;
import org.apache.pulsar.common.api.proto.BrokerEntryMetadata;
import org.apache.pulsar.common.api.proto.CommandAck.AckType;
import org.apache.pulsar.common.api.proto.CommandAck.ValidationError;
import org.apache.pulsar.common.api.proto.CommandMessage;
import org.apache.pulsar.common.api.proto.CommandSubscribe.InitialPosition;
import org.apache.pulsar.common.api.proto.CompressionType;
import org.apache.pulsar.common.api.proto.EncryptionKeys;
import org.apache.pulsar.common.api.proto.KeyValue;
import org.apache.pulsar.common.api.proto.MessageIdData;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.api.proto.ProtocolVersion;
import org.apache.pulsar.common.api.proto.SingleMessageMetadata;
import org.apache.pulsar.common.compression.CompressionCodec;
import org.apache.pulsar.common.compression.CompressionCodecProvider;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.common.util.CompletableFutureCancellationHandler;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.SafeCollectionUtils;
import org.apache.pulsar.common.util.collections.BitSetRecyclable;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.apache.pulsar.common.util.collections.GrowableArrayBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: 2/22/23 最常用，最基础的消费者实现类，可以消费一个非分区topic或者一个分区
public class ConsumerImpl<T> extends ConsumerBase<T> implements ConnectionHandler.Connection {
    // TODO: 11/2/23  最大未确认消息数
    private static final int MAX_REDELIVER_UNACKNOWLEDGED = 1000;

    // TODO: 11/2/23 消费者ID 
    final long consumerId;

    // TODO: 11/2/23 已传递给应用程序的消息数。每隔一段时间，这数字将发送 broker 通知我们准备（存储到消息队列中）获得更多的消息。（可用许可数） 
    // Number of messages that have delivered to the application. Every once in a while, this number will be sent to the
    // broker to notify that we are ready to get (and store in the incoming messages queue) more messages
    @SuppressWarnings("rawtypes")
    private static final AtomicIntegerFieldUpdater<ConsumerImpl> AVAILABLE_PERMITS_UPDATER = AtomicIntegerFieldUpdater
            .newUpdater(ConsumerImpl.class, "availablePermits");
    // TODO: 11/2/23 这里的字段 'availablePermits'需要和 AVAILABLE_PERMITS_UPDATER配合使用
    @SuppressWarnings("unused")
    private volatile int availablePermits = 0;

    // TODO: 11/2/23 上一次出队列消息
    protected volatile MessageId lastDequeuedMessageId = MessageId.earliest;
    // TODO: 11/2/23 上一次在broker消息ID
    private volatile MessageId lastMessageIdInBroker = MessageId.earliest;

    private final long lookupDeadline;

    @SuppressWarnings("rawtypes")
    private static final AtomicLongFieldUpdater<ConsumerImpl> SUBSCRIBE_DEADLINE_UPDATER = AtomicLongFieldUpdater
            .newUpdater(ConsumerImpl.class, "subscribeDeadline");
    @SuppressWarnings("unused")
    private volatile long subscribeDeadline = 0; // gets set on first successful connection

    // TODO: 11/2/23 分区索引号
    private final int partitionIndex;
    private final boolean hasParentConsumer;
    private final boolean parentConsumerHasListener;

    private final int receiverQueueRefillThreshold;

    // TODO: 11/2/23 未确认消息跟踪器， 每个消息接收后首先都要放入此容器里，然后其监控是否消费超时，
    //  如果超时，则移除并重新通知 broker 推送过来，
    //  而且还会跟踪消息未消费次数，如果超过次数，则会放入死信队列，然后发送系统设置的死信Topic ，
    //  另外此消息会被系统确认“消费”（意味着不再推送重新消费了，要消息只能到死信 Topic 里取）。
    private final UnAckedMessageTracker unAckedMessageTracker;
    // TODO: 11/2/23 确认组提交
    private final AcknowledgmentsGroupingTracker acknowledgmentsGroupingTracker;
    private final NegativeAcksTracker negativeAcksTracker;

    // todo 消费状态
    protected final ConsumerStatsRecorder stats;
    // TODO: 11/2/23 消息优先级 
    private final int priorityLevel;
    // TODO: 11/2/23 订阅模式 
    private final SubscriptionMode subscriptionMode;
    // TODO: 11/2/23 批量消息ID 
    private volatile BatchMessageIdImpl startMessageId;
    // TODO: 11/2/23 批量消息ID重置使用

    private volatile BatchMessageIdImpl seekMessageId;
    private final AtomicBoolean duringSeek;

    private final BatchMessageIdImpl initialStartMessageId;

    private final long startMessageRollbackDurationInSec;
    // TODO: 11/2/23 消费 Topic 末尾标志
    private volatile boolean hasReachedEndOfTopic;
    // TODO: 11/2/23 消息解密器 
    private final MessageCrypto msgCrypto;
    // TODO: 11/2/23 元数据信息 
    private final Map<String, String> metadata;
    // TODO: 11/2/23 压缩读取
    private final boolean readCompacted;
    private final boolean resetIncludeHead;

    // TODO: 11/2/23 订阅初始化Position
    private final SubscriptionInitialPosition subscriptionInitialPosition;
    // TODO: 11/2/23 连接处理器 
    private final ConnectionHandler connectionHandler;

    // TODO: 11/2/23 topic名称 
    private final TopicName topicName;
    // TODO: 11/2/23 Topic名称无分区信息 
    private final String topicNameWithoutPartition;
    // TODO: 11/2/23 死信消息容器 
    private final Map<MessageIdImpl, List<MessageImpl<T>>> possibleSendToDeadLetterTopicMessages;
    // TODO: 11/2/23 死信队列策略 
    private final DeadLetterPolicy deadLetterPolicy;

    // TODO: 11/2/23 死信消息生产者 
    private volatile CompletableFuture<Producer<byte[]>> deadLetterProducer;

    // TODO: 11/2/23 重试队列生产者 
    private volatile Producer<T> retryLetterProducer;
    private final ReadWriteLock createProducerLock = new ReentrantReadWriteLock();

    // TODO: 11/2/23 是否暂停 
    protected volatile boolean paused;

    // TODO: 11/2/23 消息块缓存 
    protected ConcurrentOpenHashMap<String, ChunkedMessageCtx> chunkedMessagesMap =
            ConcurrentOpenHashMap.<String, ChunkedMessageCtx>newBuilder().build();
    private int pendingChunkedMessageCount = 0;
    protected long expireTimeOfIncompleteChunkedMessageMillis = 0;
    private boolean expireChunkMessageTaskScheduled = false;
    private final int maxPendingChunkedMessage;
    // if queue size is reasonable (most of the time equal to number of producers try to publish messages concurrently
    // on the topic) then it guards against broken chunked message which was not fully published
    private final boolean autoAckOldestChunkedMessageOnQueueFull;
    // it will be used to manage N outstanding chunked message buffers
    private final BlockingQueue<String> pendingChunkedMessageUuidQueue;

    // TODO: 11/2/23 topic不存在是否需要创建 
    private final boolean createTopicIfDoesNotExist;
    private final boolean poolMessages;

    private final AtomicReference<ClientCnx> clientCnxUsedForConsumerRegistration = new AtomicReference<>();
    private final List<Throwable> previousExceptions = new CopyOnWriteArrayList<Throwable>();

    // TODO: 11/2/23 ConsumerImpl 构造方法基本与 ProducerImpl 类似，
    //  都是调用先调用父类构造方法，然后初始化基础数据和一些服务，最后开始连接 broker 或 Proxy，完成构造初始化。
    static <T> ConsumerImpl<T> newConsumerImpl(PulsarClientImpl client,
                                               String topic,
                                               ConsumerConfigurationData<T> conf,
                                               ExecutorProvider executorProvider,
                                               int partitionIndex,
                                               boolean hasParentConsumer,
                                               CompletableFuture<Consumer<T>> subscribeFuture,
                                               MessageId startMessageId,
                                               Schema<T> schema,
                                               ConsumerInterceptors<T> interceptors,
                                               boolean createTopicIfDoesNotExist) {
        return newConsumerImpl(client, topic, conf, executorProvider, partitionIndex, hasParentConsumer, false,
                subscribeFuture, startMessageId, schema, interceptors, createTopicIfDoesNotExist, 0);
    }

    static <T> ConsumerImpl<T> newConsumerImpl(PulsarClientImpl client,
                                               String topic,
                                               ConsumerConfigurationData<T> conf,
                                               ExecutorProvider executorProvider,
                                               int partitionIndex,
                                               boolean hasParentConsumer,
                                               boolean parentConsumerHasListener,
                                               CompletableFuture<Consumer<T>> subscribeFuture,
                                               MessageId startMessageId,
                                               Schema<T> schema,
                                               ConsumerInterceptors<T> interceptors,
                                               boolean createTopicIfDoesNotExist,
                                               long startMessageRollbackDurationInSec) {
        // TODO: 11/2/23  默认为1000 的队列大小
        if (conf.getReceiverQueueSize() == 0) {
            // TODO: 11/2/23 零队列的消费实现
            return new ZeroQueueConsumerImpl<>(client, topic, conf, executorProvider, partitionIndex, hasParentConsumer,
                    subscribeFuture,
                    startMessageId, schema, interceptors,
                    createTopicIfDoesNotExist);
        } else {
            // TODO: 11/2/23 默认为1000 队列大小的消费实现
            return new ConsumerImpl<>(client, topic, conf, executorProvider, partitionIndex, hasParentConsumer,
                    parentConsumerHasListener,
                    subscribeFuture, startMessageId,
                    startMessageRollbackDurationInSec /* rollback time in sec to start msgId */,
                    schema, interceptors, createTopicIfDoesNotExist);
        }
    }

    protected ConsumerImpl(PulsarClientImpl client, String topic, ConsumerConfigurationData<T> conf,
           ExecutorProvider executorProvider, int partitionIndex, boolean hasParentConsumer,
           boolean parentConsumerHasListener, CompletableFuture<Consumer<T>> subscribeFuture, MessageId startMessageId,
           long startMessageRollbackDurationInSec, Schema<T> schema, ConsumerInterceptors<T> interceptors,
           boolean createTopicIfDoesNotExist) {
        super(client, topic, conf, conf.getReceiverQueueSize(), executorProvider, subscribeFuture, schema,
                interceptors);
        // TODO: 11/2/23 消费者ID生成
        this.consumerId = client.newConsumerId();
        // TODO: 11/2/23 订阅模式 
        this.subscriptionMode = conf.getSubscriptionMode();
        // TODO: 11/2/23 起始消费id
        this.startMessageId = startMessageId != null ? new BatchMessageIdImpl((MessageIdImpl) startMessageId) : null;
        this.initialStartMessageId = this.startMessageId;
        this.startMessageRollbackDurationInSec = startMessageRollbackDurationInSec;
        // TODO: 11/2/23 可用许可，设置为0 
        AVAILABLE_PERMITS_UPDATER.set(this, 0);
        // TODO: 11/2/23 lookup超时时间 
        this.lookupDeadline = System.currentTimeMillis() + client.getConfiguration().getLookupTimeoutMs();
        // TODO: 11/2/23 消费者 Topic 分区索引
        this.partitionIndex = partitionIndex;
        this.hasParentConsumer = hasParentConsumer;
        // TODO: 11/2/23 从新拉取数据的判断标准 500 水位，即当消费者消费数据小于了这个水位，就可以重新拉取数据
        this.receiverQueueRefillThreshold = conf.getReceiverQueueSize() / 2;
        this.parentConsumerHasListener = parentConsumerHasListener;
        // TODO: 11/2/23 优先级
        this.priorityLevel = conf.getPriorityLevel();
        // TODO: 11/2/23 是否压缩 
        this.readCompacted = conf.isReadCompacted();
        // TODO: 11/2/23 /设置订阅初始化时，游标开始位置 ，默认情况下，是从topic的末端消费
        this.subscriptionInitialPosition = conf.getSubscriptionInitialPosition();
        this.negativeAcksTracker = new NegativeAcksTracker(this, conf);
        this.resetIncludeHead = conf.isResetIncludeHead();
        this.createTopicIfDoesNotExist = createTopicIfDoesNotExist;
        this.maxPendingChunkedMessage = conf.getMaxPendingChunkedMessage();
        this.pendingChunkedMessageUuidQueue = new GrowableArrayBlockingQueue<>();
        this.expireTimeOfIncompleteChunkedMessageMillis = conf.getExpireTimeOfIncompleteChunkedMessageMillis();
        this.autoAckOldestChunkedMessageOnQueueFull = conf.isAutoAckOldestChunkedMessageOnQueueFull();
        this.poolMessages = conf.isPoolMessages();
        // TODO: 11/2/23 是否开始的时候暂停消费 
        this.paused = conf.isStartPaused();

        // todo 如果客户端设置了 .statsInterval(20, TimeUnit.MICROSECONDS)
        if (client.getConfiguration().getStatsIntervalSeconds() > 0) {
            // todo [5118b] Prefetched messages: 0 --- Consume throughput received: 9.99 msgs/s --- 0.00 Mbit/s --- Ack sent rate: 1.00 ack/s --- Failed messages: 0 --- batch messages: 0 ---Failed acks: 0
            stats = new ConsumerStatsRecorderImpl(client, conf, this);
        } else {
            // todo 默认是关闭的
            stats = ConsumerStatsDisabled.INSTANCE;
        }

        duringSeek = new AtomicBoolean(false);

        // TODO: 11/2/23 默认为0 ， 消息确认超时时间
        if (conf.getAckTimeoutMillis() != 0) {
            // TODO: 11/2/23 时间单位 
            if (conf.getAckTimeoutRedeliveryBackoff() != null) {
                this.unAckedMessageTracker = new UnAckedMessageRedeliveryTracker(client, this, conf);
            } else {
                this.unAckedMessageTracker = new UnAckedMessageTracker(client, this, conf);
            }
        } else {
            this.unAckedMessageTracker = UnAckedMessageTracker.UNACKED_MESSAGE_TRACKER_DISABLED;
        }

        // TODO: 11/2/23 如果还没创建，则创建消息解密器 
        // Create msgCrypto if not created already
        if (conf.getCryptoKeyReader() != null) {
            if (conf.getMessageCrypto() != null) {
                this.msgCrypto = conf.getMessageCrypto();
            } else {
                // default to use MessageCryptoBc;
                MessageCrypto msgCryptoBc;
                try {
                    msgCryptoBc = new MessageCryptoBc(
                            String.format("[%s] [%s]", topic, subscription),
                            false);
                } catch (Exception e) {
                    log.error("MessageCryptoBc may not included in the jar. e:", e);
                    msgCryptoBc = null;
                }
                this.msgCrypto = msgCryptoBc;
            }
        } else {
            this.msgCrypto = null;
        }

        // TODO: 11/2/23 初始化属性或设置属性为不可变更
        if (conf.getProperties().isEmpty()) {
            metadata = Collections.emptyMap();
        } else {
            metadata = Collections.unmodifiableMap(new HashMap<>(conf.getProperties()));
        }

        // TODO: 11/2/23 连接处理器，这里跟 Producer 一样 
        this.connectionHandler = new ConnectionHandler(this,
                        new BackoffBuilder()
                                .setInitialTime(client.getConfiguration().getInitialBackoffIntervalNanos(),
                                        TimeUnit.NANOSECONDS)
                                .setMax(client.getConfiguration().getMaxBackoffIntervalNanos(), TimeUnit.NANOSECONDS)
                                .setMandatoryStop(0, TimeUnit.MILLISECONDS)
                                .create(),
                this);

        // TODO: 11/2/23  读取 Topic 信息，判定 Topic 是持久化或非持久化的
        this.topicName = TopicName.get(topic);
        // TODO: 11/2/23 持久化topic
        if (this.topicName.isPersistent()) {
            this.acknowledgmentsGroupingTracker =
                new PersistentAcknowledgmentsGroupingTracker(this, conf, client.eventLoopGroup());
        } else {
            // TODO: 11/2/23 非持久化topic
            this.acknowledgmentsGroupingTracker =
                NonPersistentAcknowledgmentGroupingTracker.of();
        }

        // TODO: 11/2/23 私信队列策略
        if (conf.getDeadLetterPolicy() != null) {
            possibleSendToDeadLetterTopicMessages = new ConcurrentHashMap<>();
            // TODO: 11/2/23 死信队列topic有提供
            /**
             * 这里是否不是这样优化更好呢?
             * this.deadLetterPolicy = DeadLetterPolicy.builder()
             * .maxRedeliverCount(conf.getDeadLetterPolicy().getMaxRedeliverCount())
             * .deadLetterTopic(StringUtils.isNotBlank(conf.getDeadLetterPolicy().getDeadLetterTopic()) ?
             *  conf.getDeadLetterPolicy().getDeadLetterTopic() :
             *  String.format("%s-%s" + RetryMessageUtil.DLQ_GROUP_TOPIC_SUFFIX,topic, subscription))
             * .build();
             *
             */
            if (StringUtils.isNotBlank(conf.getDeadLetterPolicy().getDeadLetterTopic())) {
                this.deadLetterPolicy = DeadLetterPolicy.builder()
                        .maxRedeliverCount(conf.getDeadLetterPolicy().getMaxRedeliverCount())
                        .deadLetterTopic(conf.getDeadLetterPolicy().getDeadLetterTopic())
                        .build();
            } else {
                this.deadLetterPolicy = DeadLetterPolicy.builder()
                        .maxRedeliverCount(conf.getDeadLetterPolicy().getMaxRedeliverCount())
                        .deadLetterTopic(String.format("%s-%s" + RetryMessageUtil.DLQ_GROUP_TOPIC_SUFFIX,
                                topic, subscription))
                        .build();
            }

            /**
             * 这里是否不是这样优化更好呢?
             * this.deadLetterPolicy.setRetryLetterTopic(StringUtils.isNotBlank(conf.getDeadLetterPolicy().getRetryLetterTopic()) ?
             * conf.getDeadLetterPolicy().getRetryLetterTopic() :
             * String.format( "%s-%s" + RetryMessageUtil.RETRY_GROUP_TOPIC_SUFFIX, topic, subscription));
             */
            if (StringUtils.isNotBlank(conf.getDeadLetterPolicy().getRetryLetterTopic())) {
                this.deadLetterPolicy.setRetryLetterTopic(conf.getDeadLetterPolicy().getRetryLetterTopic());
            } else {
                this.deadLetterPolicy.setRetryLetterTopic(String.format(
                        "%s-%s" + RetryMessageUtil.RETRY_GROUP_TOPIC_SUFFIX,
                        topic, subscription));
            }

            if (StringUtils.isNotBlank(conf.getDeadLetterPolicy().getInitialSubscriptionName())) {
                this.deadLetterPolicy.setInitialSubscriptionName(
                        conf.getDeadLetterPolicy().getInitialSubscriptionName());
            }

        } else {
            deadLetterPolicy = null;
            possibleSendToDeadLetterTopicMessages = null;
        }

        // TODO: 11/2/23 topic的full name
        topicNameWithoutPartition = topicName.getPartitionedTopicName();

        // TODO: 11/2/23  连接broker
        // TODO: 11/2/23 这里跟 Producer 构造方法一样，发起与 broker 连接，直到获取到连接结果。
        //  这里唯一不同的是就是对 Connection 两个方法的实现
        grabCnx();
    }

    public ConnectionHandler getConnectionHandler() {
        return connectionHandler;
    }

    public UnAckedMessageTracker getUnAckedMessageTracker() {
        return unAckedMessageTracker;
    }

    @Override
    public CompletableFuture<Void> unsubscribeAsync() {
        if (getState() == State.Closing || getState() == State.Closed) {
            return FutureUtil
                    .failedFuture(new PulsarClientException.AlreadyClosedException("Consumer was already closed"));
        }
        final CompletableFuture<Void> unsubscribeFuture = new CompletableFuture<>();
        if (isConnected()) {
            setState(State.Closing);
            long requestId = client.newRequestId();
            ByteBuf unsubscribe = Commands.newUnsubscribe(consumerId, requestId);
            ClientCnx cnx = cnx();
            cnx.sendRequestWithId(unsubscribe, requestId).thenRun(() -> {
                closeConsumerTasks();
                deregisterFromClientCnx();
                client.cleanupConsumer(this);
                log.info("[{}][{}] Successfully unsubscribed from topic", topic, subscription);
                setState(State.Closed);
                unsubscribeFuture.complete(null);
            }).exceptionally(e -> {
                log.error("[{}][{}] Failed to unsubscribe: {}", topic, subscription, e.getCause().getMessage());
                setState(State.Ready);
                unsubscribeFuture.completeExceptionally(
                    PulsarClientException.wrap(e.getCause(),
                        String.format("Failed to unsubscribe the subscription %s of topic %s",
                            topicName.toString(), subscription)));
                return null;
            });
        } else {
            unsubscribeFuture.completeExceptionally(
                new PulsarClientException(
                    String.format("The client is not connected to the broker when unsubscribing the "
                            + "subscription %s of the topic %s", subscription, topicName.toString())));
        }
        return unsubscribeFuture;
    }

    // TODO: 11/3/23 接收broker端发过来的消息
    @Override
    protected Message<T> internalReceive() throws PulsarClientException {
        Message<T> message;
        try {
            // TODO: 11/3/23 获取队列的头部消息
            message = incomingMessages.take();
            // TODO: 11/3/23 消息处理
            messageProcessed(message);
            // TODO: 11/6/23 如果消息的ConsumerEpoch不合法，则获取skip掉，获取下一条消息
            if (!isValidConsumerEpoch(message)) {
                return internalReceive();
            }
            return beforeConsume(message);
        } catch (InterruptedException e) {
            stats.incrementNumReceiveFailed();
            throw PulsarClientException.unwrap(e);
        }
    }

    private boolean isValidConsumerEpoch(Message<T> message) {
        return isValidConsumerEpoch((MessageImpl<T>) message);
    }

    @Override
    protected CompletableFuture<Message<T>> internalReceiveAsync() {
        CompletableFutureCancellationHandler cancellationHandler = new CompletableFutureCancellationHandler();
        CompletableFuture<Message<T>> result = cancellationHandler.createFuture();
        internalPinnedExecutor.execute(() -> {
            Message<T> message = incomingMessages.poll();
            if (message == null) {
                pendingReceives.add(result);
                cancellationHandler.setCancelAction(() -> pendingReceives.remove(result));
            } else {
                messageProcessed(message);
                if (!isValidConsumerEpoch(message)) {
                    pendingReceives.add(result);
                    cancellationHandler.setCancelAction(() -> pendingReceives.remove(result));
                    return;
                }
                result.complete(beforeConsume(message));
            }
        });

        return result;
    }

    @Override
    protected Message<T> internalReceive(long timeout, TimeUnit unit) throws PulsarClientException {
        Message<T> message;
        // TODO: 11/6/23 系统时间
        long callTime = System.nanoTime();
        try {
            // TODO: 11/6/23 从 incomingMessages 获取头部msg
            message = incomingMessages.poll(timeout, unit);
            // TODO: 11/6/23 消息为null，说明 incomingMessages 为空
            if (message == null) {
                return null;
            }
            // TODO: 11/6/23 消息处理
            messageProcessed(message);
            // TODO: 11/6/23 consumerEpoch是否合法
            if (!isValidConsumerEpoch(message)) {
                long executionTime = System.nanoTime() - callTime;
                long timeoutInNanos = unit.toNanos(timeout);
                if (executionTime >= timeoutInNanos) {
                    return null;
                } else {
                    return internalReceive(timeoutInNanos - executionTime, TimeUnit.NANOSECONDS);
                }
            }
            return beforeConsume(message);
        } catch (InterruptedException e) {
            State state = getState();
            if (state != State.Closing && state != State.Closed) {
                stats.incrementNumReceiveFailed();
                throw PulsarClientException.unwrap(e);
            } else {
                return null;
            }
        }
    }

    @Override
    protected Messages<T> internalBatchReceive() throws PulsarClientException {
        try {
            return internalBatchReceiveAsync().get();
        } catch (InterruptedException | ExecutionException e) {
            State state = getState();
            if (state != State.Closing && state != State.Closed) {
                stats.incrementNumBatchReceiveFailed();
                throw PulsarClientException.unwrap(e);
            } else {
                return null;
            }
        }
    }

    @Override
    protected CompletableFuture<Messages<T>> internalBatchReceiveAsync() {
        CompletableFutureCancellationHandler cancellationHandler = new CompletableFutureCancellationHandler();
        CompletableFuture<Messages<T>> result = cancellationHandler.createFuture();
        internalPinnedExecutor.execute(() -> {
            if (hasEnoughMessagesForBatchReceive()) {
                MessagesImpl<T> messages = getNewMessagesImpl();
                Message<T> msgPeeked = incomingMessages.peek();
                while (msgPeeked != null && messages.canAdd(msgPeeked)) {
                    Message<T> msg = incomingMessages.poll();
                    if (msg != null) {
                        messageProcessed(msg);
                        if (!isValidConsumerEpoch(msg)) {
                            msgPeeked = incomingMessages.peek();
                            continue;
                        }
                        Message<T> interceptMsg = beforeConsume(msg);
                        messages.add(interceptMsg);
                    }
                    msgPeeked = incomingMessages.peek();
                }
                result.complete(messages);
            } else {
                OpBatchReceive<T> opBatchReceive = OpBatchReceive.of(result);
                pendingBatchReceives.add(opBatchReceive);
                cancellationHandler.setCancelAction(() -> pendingBatchReceives.remove(opBatchReceive));
            }
        });
        return result;
    }

    @Override
    protected CompletableFuture<Void> doAcknowledge(MessageId messageId, AckType ackType,
                                                    Map<String, Long> properties,
                                                    TransactionImpl txn) {
        checkArgument(messageId instanceof MessageIdImpl);
        // TODO: 11/6/23 检查客户端状态
        if (getState() != State.Ready && getState() != State.Connecting) {
            stats.incrementNumAcksFailed();
            PulsarClientException exception = new PulsarClientException("Consumer not ready. State: " + getState());
            if (AckType.Individual.equals(ackType)) { // TODO: 11/6/23 单个消息确认
                onAcknowledge(messageId, exception);
            } else if (AckType.Cumulative.equals(ackType)) { // TODO: 11/6/23 累积消息确认
                onAcknowledgeCumulative(messageId, exception);
            }
            return FutureUtil.failedFuture(exception);
        }

        // TODO: 11/6/23 开启事务
        if (txn != null) {
            return doTransactionAcknowledgeForResponse(messageId, ackType, null, properties,
                    new TxnID(txn.getTxnIdMostBits(), txn.getTxnIdLeastBits()));
        }
        // TODO: 11/6/23 没开事务
        return acknowledgmentsGroupingTracker.addAcknowledgment((MessageIdImpl) messageId, ackType, properties);
    }

    @Override
    protected CompletableFuture<Void> doAcknowledge(List<MessageId> messageIdList,
                                                    AckType ackType,
                                                    Map<String, Long> properties,
                                                    TransactionImpl txn) {
        return this.acknowledgmentsGroupingTracker.addListAcknowledgment(messageIdList, ackType, properties);
    }


    @SuppressWarnings("unchecked")
    @Override
    protected CompletableFuture<Void> doReconsumeLater(Message<?> message, AckType ackType,
                                                       Map<String, String> customProperties,
                                                       long delayTime,
                                                       TimeUnit unit) {
        MessageId messageId = message.getMessageId();
        if (messageId == null) {
            return FutureUtil.failedFuture(new PulsarClientException
                    .InvalidMessageException("Cannot handle message with null messageId"));
        }

        if (messageId instanceof TopicMessageIdImpl) {
            messageId = ((TopicMessageIdImpl) messageId).getInnerMessageId();
        }
        checkArgument(messageId instanceof MessageIdImpl);
        if (getState() != State.Ready && getState() != State.Connecting) {
            stats.incrementNumAcksFailed();
            PulsarClientException exception = new PulsarClientException("Consumer not ready. State: " + getState());
            if (AckType.Individual.equals(ackType)) {
                onAcknowledge(messageId, exception);
            } else if (AckType.Cumulative.equals(ackType)) {
                onAcknowledgeCumulative(messageId, exception);
            }
            return FutureUtil.failedFuture(exception);
        }
        if (delayTime < 0) {
            delayTime = 0;
        }
        if (retryLetterProducer == null) {
            createProducerLock.writeLock().lock();
            try {
                if (retryLetterProducer == null) {
                    retryLetterProducer = client.newProducer(schema)
                            .topic(this.deadLetterPolicy.getRetryLetterTopic())
                            .enableBatching(false)
                            .blockIfQueueFull(false)
                            .create();
                }
            } catch (Exception e) {
                log.error("Create retry letter producer exception with topic: {}",
                        deadLetterPolicy.getRetryLetterTopic(), e);
            } finally {
                createProducerLock.writeLock().unlock();
            }
        }
        CompletableFuture<Void> result = new CompletableFuture<>();
        if (retryLetterProducer != null) {
            try {
                MessageImpl<T> retryMessage = (MessageImpl<T>) getMessageImpl(message);
                String originMessageIdStr = getOriginMessageIdStr(message);
                String originTopicNameStr = getOriginTopicNameStr(message);
                SortedMap<String, String> propertiesMap =
                        getPropertiesMap(message, originMessageIdStr, originTopicNameStr);
                if (customProperties != null) {
                    propertiesMap.putAll(customProperties);
                }
                int reconsumetimes = 1;
                if (propertiesMap.containsKey(RetryMessageUtil.SYSTEM_PROPERTY_RECONSUMETIMES)) {
                    reconsumetimes = Integer.parseInt(
                            propertiesMap.get(RetryMessageUtil.SYSTEM_PROPERTY_RECONSUMETIMES));
                    reconsumetimes = reconsumetimes + 1;
                }
                propertiesMap.put(RetryMessageUtil.SYSTEM_PROPERTY_RECONSUMETIMES, String.valueOf(reconsumetimes));
                propertiesMap.put(RetryMessageUtil.SYSTEM_PROPERTY_DELAY_TIME,
                        String.valueOf(unit.toMillis(delayTime)));

                MessageId finalMessageId = messageId;
                if (reconsumetimes > this.deadLetterPolicy.getMaxRedeliverCount()
                        && StringUtils.isNotBlank(deadLetterPolicy.getDeadLetterTopic())) {
                    initDeadLetterProducerIfNeeded();
                    deadLetterProducer.thenAccept(dlqProducer -> {
                        TypedMessageBuilder<byte[]> typedMessageBuilderNew =
                                dlqProducer.newMessage(Schema.AUTO_PRODUCE_BYTES(retryMessage.getReaderSchema().get()))
                                        .value(retryMessage.getData())
                                        .properties(propertiesMap);
                        typedMessageBuilderNew.sendAsync().thenAccept(msgId -> {
                            doAcknowledge(finalMessageId, ackType, Collections.emptyMap(), null).thenAccept(v -> {
                                result.complete(null);
                            }).exceptionally(ex -> {
                                result.completeExceptionally(ex);
                                return null;
                            });
                        }).exceptionally(ex -> {
                            result.completeExceptionally(ex);
                            return null;
                        });
                    }).exceptionally(ex -> {
                        result.completeExceptionally(ex);
                        deadLetterProducer = null;
                        return null;
                    });
                } else {
                    TypedMessageBuilder<T> typedMessageBuilderNew = retryLetterProducer.newMessage()
                            .value(retryMessage.getValue())
                            .properties(propertiesMap);
                    if (delayTime > 0) {
                        typedMessageBuilderNew.deliverAfter(delayTime, unit);
                    }
                    if (message.hasKey()) {
                        typedMessageBuilderNew.key(message.getKey());
                    }
                    typedMessageBuilderNew.sendAsync()
                            .thenCompose(__ -> doAcknowledge(finalMessageId, ackType, Collections.emptyMap(), null))
                            .thenAccept(v -> result.complete(null))
                            .exceptionally(ex -> {
                                result.completeExceptionally(ex);
                                return null;
                            });
                }
            } catch (Exception e) {
                log.error("Send to retry letter topic exception with topic: {}, messageId: {}",
                        retryLetterProducer.getTopic(), messageId, e);
                Set<MessageId> messageIds = Collections.singleton(messageId);
                unAckedMessageTracker.remove(messageId);
                redeliverUnacknowledgedMessages(messageIds);
            }
        }
        MessageId finalMessageId = messageId;
        result.exceptionally(ex -> {
            Set<MessageId> messageIds = Collections.singleton(finalMessageId);
            unAckedMessageTracker.remove(finalMessageId);
            redeliverUnacknowledgedMessages(messageIds);
            return null;
        });
        return result;
    }

    private SortedMap<String, String> getPropertiesMap(Message<?> message,
                                                       String originMessageIdStr,
                                                       String originTopicNameStr) {
        SortedMap<String, String> propertiesMap = new TreeMap<>();
        if (message.getProperties() != null) {
            propertiesMap.putAll(message.getProperties());
        }
        propertiesMap.putIfAbsent(RetryMessageUtil.SYSTEM_PROPERTY_REAL_TOPIC, originTopicNameStr);
        //Compatible with the old version, will be deleted in the future
        propertiesMap.putIfAbsent(RetryMessageUtil.SYSTEM_PROPERTY_ORIGIN_MESSAGE_ID, originMessageIdStr);
        propertiesMap.putIfAbsent(RetryMessageUtil.PROPERTY_ORIGIN_MESSAGE_ID, originMessageIdStr);
        return propertiesMap;
    }

    private String getOriginMessageIdStr(Message<?> message) {
        if (message instanceof TopicMessageImpl) {
            return ((TopicMessageIdImpl) message.getMessageId()).getInnerMessageId().toString();
        } else if (message instanceof MessageImpl) {
            return message.getMessageId().toString();
        }
        return null;
    }

    private String getOriginTopicNameStr(Message<?> message) {
        if (message instanceof TopicMessageImpl) {
            return ((TopicMessageIdImpl) message.getMessageId()).getTopicName();
        } else if (message instanceof MessageImpl) {
            return message.getTopicName();
        }
        return null;
    }

    private MessageImpl<?> getMessageImpl(Message<?> message) {
        if (message instanceof TopicMessageImpl) {
            return (MessageImpl<?>) ((TopicMessageImpl<?>) message).getMessage();
        } else if (message instanceof MessageImpl) {
            return (MessageImpl<?>) message;
        }
        return null;
    }

    @Override
    public void negativeAcknowledge(MessageId messageId) {
        negativeAcksTracker.add(messageId);

        // Ensure the message is not redelivered for ack-timeout, since we did receive an "ack"
        unAckedMessageTracker.remove(messageId);
    }

    @Override
    public void negativeAcknowledge(Message<?> message) {
        negativeAcksTracker.add(message);

        // Ensure the message is not redelivered for ack-timeout, since we did receive an "ack"
        unAckedMessageTracker.remove(message.getMessageId());
    }

    @Override
    public void connectionOpened(final ClientCnx cnx) {
        // TODO: 11/2/23 当消费者连接到了broker后，连接就打开 
        previousExceptions.clear();

        // TODO: 11/2/23 状态检测
        if (getState() == State.Closing || getState() == State.Closed) {
            setState(State.Closed);
            closeConsumerTasks();
            deregisterFromClientCnx();
            client.cleanupConsumer(this);
            clearReceiverQueue();
            return;
        }

        log.info("[{}][{}] Subscribing to topic on cnx {}, consumerId {}",
                topic, subscription, cnx.ctx().channel(), consumerId);

        // TODO: 11/2/23 request id获取 
        long requestId = client.newRequestId();
        if (duringSeek.get()) {
            acknowledgmentsGroupingTracker.flushAndClean();
        }

        SUBSCRIBE_DEADLINE_UPDATER
                .compareAndSet(this, 0L, System.currentTimeMillis()
                        + client.getConfiguration().getOperationTimeoutMs());

        int currentSize;
        synchronized (this) {
            currentSize = incomingMessages.size();
            startMessageId = clearReceiverQueue();
            if (possibleSendToDeadLetterTopicMessages != null) {
                possibleSendToDeadLetterTopicMessages.clear();
            }
        }

        // TODO: 11/2/23 是否为持久化订阅模式
        boolean isDurable = subscriptionMode == SubscriptionMode.Durable;
        final MessageIdData startMessageIdData;

        // For regular durable subscriptions, the message id from where to restart will be determined by the broker.
        // For non-durable we are going to restart from the next entry.
        if (!isDurable && startMessageId != null) {
            // TODO: 11/2/23 对于非持久化订阅，重启时直接取下一消息实体 
            startMessageIdData = new MessageIdData()
                    .setLedgerId(startMessageId.getLedgerId())
                    .setEntryId(startMessageId.getEntryId())
                    .setBatchIndex(startMessageId.getBatchIndex());
        } else {
            // TODO: 11/2/23  对于通常的持久化订阅，重启时，broker 决定起始消息ID。
            startMessageIdData = null;
        }

        // TODO: 11/2/23 schema信息
        SchemaInfo si = schema.getSchemaInfo();
        if (si != null && (SchemaType.BYTES == si.getType() || SchemaType.NONE == si.getType())) {
            // don't set schema for Schema.BYTES
            // TODO: 11/2/23 为了兼容，当 Schema 类型为 Schema.BYTES 时，SchemaInfo 为 null。
            si = null;
        }
        // startMessageRollbackDurationInSec should be consider only once when consumer connects to first time
        long startMessageRollbackDuration = (startMessageRollbackDurationInSec > 0
                && startMessageId != null
                && startMessageId.equals(initialStartMessageId)) ? startMessageRollbackDurationInSec : 0;

        // synchronized this, because redeliverUnAckMessage eliminate the epoch inconsistency between them
        synchronized (this) {
            // TODO: 11/2/23 原子设置 cnx
            setClientCnx(cnx);
            // TODO: 11/2/23 创建新订阅命令
            ByteBuf request = Commands.newSubscribe(topic, subscription, consumerId, requestId, getSubType(),
                    priorityLevel, consumerName, isDurable, startMessageIdData, metadata, readCompacted,
                    conf.isReplicateSubscriptionState(),
                    InitialPosition.valueOf(subscriptionInitialPosition.getValue()),
                    startMessageRollbackDuration, si, createTopicIfDoesNotExist, conf.getKeySharedPolicy(),
                    // Use the current epoch to subscribe.
                    conf.getSubscriptionProperties(), CONSUMER_EPOCH.get(this));
            // todo 每次创建订阅命令的时候，就会在客户端生成一个CONSUMER_EPOCH，
            //  当消费者接收到了broker端发过来的数据时，就会对这个CONSUMER_EPOCH 对比

            // TODO: 11/2/23 发送订阅请求
            cnx.sendRequestWithId(request, requestId).thenRun(() -> {
                synchronized (ConsumerImpl.this) {
                    // TODO: 11/2/23 尝试把cnx状态转变成 Ready 状态
                    if (changeToReadyState()) {
                        // TODO: 11/2/23 转换成功，则把可用许可设置为0
                        consumerIsReconnectedToBroker(cnx, currentSize);
                    } else {
                        // TODO: 11/2/23 如果重连的时候，转换失败，设置消费者状态为 Closed 状态，并关闭连接确保 broker 清理消费者（资源）
                        // Consumer was closed while reconnecting, close the connection to make sure the broker
                        // drops the consumer on its side
                        setState(State.Closed);
                        deregisterFromClientCnx();
                        client.cleanupConsumer(this);
                        cnx.channel().close();
                        return;
                    }
                }
                // TODO: 11/2/23 重置重连超时时间
                resetBackoff();

                // TODO: 11/2/23 尝试把 subscribeFuture 设置 complete 状态，如果成功，则认为是第一次连接
                boolean firstTimeConnect = subscribeFuture.complete(this);
                // if the consumer is not partitioned or is re-connected and is partitioned, we send the flow
                // command to receive messages.
                // TODO: 11/2/23 如果消费者（订阅的Topic）是无分区 或 是分区但已重连过，并且接收队列不为 0，
                //  发送流控命令到 broker，（通知 broker 可以推送消息到消费者了）
                if (!(firstTimeConnect && hasParentConsumer) && conf.getReceiverQueueSize() != 0) {
                    // TODO: 11/2/23   receiverQueueSize = 1000
                    increaseAvailablePermits(cnx, conf.getReceiverQueueSize());
                }
            }).exceptionally((e) -> {
                // TODO: 11/2/23 如果出现异常，需要把Cnx从客户端移除
                deregisterFromClientCnx();
                if (getState() == State.Closing || getState() == State.Closed) {
                    // Consumer was closed while reconnecting, close the connection to make sure the broker
                    // drops the consumer on its side
                    cnx.channel().close();
                    return null;
                }
                log.warn("[{}][{}] Failed to subscribe to topic on {}", topic,
                        subscription, cnx.channel().remoteAddress());

                if (e.getCause() instanceof PulsarClientException
                        && PulsarClientException.isRetriableError(e.getCause())
                        && System.currentTimeMillis() < SUBSCRIBE_DEADLINE_UPDATER.get(ConsumerImpl.this)) {
                    // TODO: 11/2/23 尝试重连broker
                    reconnectLater(e.getCause());
                } else if (!subscribeFuture.isDone()) {
                    // unable to create new consumer, fail operation
                    setState(State.Failed);
                    closeConsumerTasks();
                    subscribeFuture.completeExceptionally(
                            PulsarClientException.wrap(e, String.format("Failed to subscribe the topic %s "
                                            + "with subscription name %s when connecting to the broker",
                                    topicName.toString(), subscription)));
                    client.cleanupConsumer(this);
                } else if (e.getCause() instanceof TopicDoesNotExistException) {
                    // The topic was deleted after the consumer was created, and we're
                    // not allowed to recreate the topic. This can happen in few cases:
                    //  * Regex consumer getting error after topic gets deleted
                    //  * Regular consumer after topic is manually delete and with
                    //    auto-topic-creation set to false
                    // No more retries are needed in this case.
                    setState(State.Failed);
                    closeConsumerTasks();
                    client.cleanupConsumer(this);
                    // TODO: 11/2/23 topic不存在异常处理
                    log.warn("[{}][{}] Closed consumer because topic does not exist anymore {}",
                            topic, subscription, cnx.channel().remoteAddress());
                } else {
                    // TODO: 11/2/23 继续重连broker
                    // consumer was subscribed and connected but we got some error, keep trying
                    reconnectLater(e.getCause());
                }
                return null;
            });
        }
    }

    protected void consumerIsReconnectedToBroker(ClientCnx cnx, int currentQueueSize) {
        log.info("[{}][{}] Subscribed to topic on {} -- consumer: {}", topic, subscription,
                cnx.channel().remoteAddress(), consumerId);
        // TODO: 11/2/23 可用许可设置为0
        AVAILABLE_PERMITS_UPDATER.set(this, 0);
    }

    /**
     * todo 清除内部接收队列并返回队列中第一条消息的消息ID，该消息是应用尚未看到（处理）的
     * Clear the internal receiver queue and returns the message id of what was the 1st message in the queue that was
     * not seen by the application.
     */
    private BatchMessageIdImpl clearReceiverQueue() {
        List<Message<?>> currentMessageQueue = new ArrayList<>(incomingMessages.size());
        // TODO: 11/3/23 把 incomingMessages 里面的所有message都拿出来放入到 currentMessageQueue， 
        //  把阻塞队列数据全部按顺序放入当前消息队列，就是ArrayList容器
        incomingMessages.drainTo(currentMessageQueue);
        // TODO: 11/3/23 重置 INCOMING_MESSAGES_SIZE_UPDATER = 0
        resetIncomingMessageSize();

        if (duringSeek.compareAndSet(true, false)) {
            return seekMessageId;
            // TODO: 11/3/23 如果订阅模式为持久化，就返回  startMessageId
        } else if (subscriptionMode == SubscriptionMode.Durable) {
            return startMessageId;
        }

        // TODO: 11/3/23 如果不为空 
        if (!currentMessageQueue.isEmpty()) {
            // TODO: 11/3/23 取第一条消息
            MessageIdImpl nextMessageInQueue = (MessageIdImpl) currentMessageQueue.get(0).getMessageId();
            BatchMessageIdImpl previousMessage;
            if (nextMessageInQueue instanceof BatchMessageIdImpl) {
                // TODO: 11/3/23 获取前一个消息在当前批量消息中
                // Get on the previous message within the current batch
                previousMessage = new BatchMessageIdImpl(nextMessageInQueue.getLedgerId(),
                        nextMessageInQueue.getEntryId(), nextMessageInQueue.getPartitionIndex(),
                        ((BatchMessageIdImpl) nextMessageInQueue).getBatchIndex() - 1);
            } else {
                // TODO: 11/3/23 获取前一个消息在当前消息实体中
                // Get on previous message in previous entry
                previousMessage = new BatchMessageIdImpl(nextMessageInQueue.getLedgerId(),
                        nextMessageInQueue.getEntryId() - 1, nextMessageInQueue.getPartitionIndex(), -1);
            }
            // TODO: 11/3/23 把 currentMessageQueue 中的所有消息都释放
            // release messages if they are pooled messages
            currentMessageQueue.forEach(Message::release);
            return previousMessage;
        } else if (!lastDequeuedMessageId.equals(MessageId.earliest)) {
            // TODO: 11/3/23 如果队列为空，最后出队消息不等于最早消息ID，则使用最后出队消息作为起始消息
            // If the queue was empty we need to restart from the message just after the last one that has been dequeued
            // in the past
            return new BatchMessageIdImpl((MessageIdImpl) lastDequeuedMessageId);
        } else {
            // TODO: 11/3/23 如果没消息接收过或已经被消费者处理，下一个消息还是用起始消息
            // No message was received or dequeued by this consumer. Next message would still be the startMessageId
            return startMessageId;
        }
    }

    /**
     * send the flow command to have the broker start pushing messages.
     */
    private void sendFlowPermitsToBroker(ClientCnx cnx, int numMessages) {
        if (cnx != null && numMessages > 0) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] Adding {} additional permits", topic, subscription, numMessages);
            }
            if (log.isDebugEnabled()) {
                // TODO: 11/3/23 发送流控命令到 broker
                cnx.ctx().writeAndFlush(Commands.newFlow(consumerId, numMessages))
                        .addListener(writeFuture -> {
                            if (!writeFuture.isSuccess()) {
                                log.debug("Consumer {} failed to send {} permits to broker: {}",
                                        consumerId, numMessages, writeFuture.cause().getMessage());
                            } else {
                                log.debug("Consumer {} sent {} permits to broker", consumerId, numMessages);
                            }
                        });
            } else {
                cnx.ctx().writeAndFlush(Commands.newFlow(consumerId, numMessages), cnx.ctx().voidPromise());
            }
        }
    }

    @Override
    public void connectionFailed(PulsarClientException exception) {
        boolean nonRetriableError = !PulsarClientException.isRetriableError(exception);
        boolean timeout = System.currentTimeMillis() > lookupDeadline;
        if (nonRetriableError || timeout) {
            exception.setPreviousExceptions(previousExceptions);
            if (subscribeFuture.completeExceptionally(exception)) {
                setState(State.Failed);
                if (nonRetriableError) {
                    log.info("[{}] Consumer creation failed for consumer {} with unretriableError {}",
                            topic, consumerId, exception);
                } else {
                    log.info("[{}] Consumer creation failed for consumer {} after timeout", topic, consumerId);
                }
                closeConsumerTasks();
                deregisterFromClientCnx();
                client.cleanupConsumer(this);
            }
        } else {
            previousExceptions.add(exception);
        }
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        CompletableFuture<Void> closeFuture = new CompletableFuture<>();

        if (getState() == State.Closing || getState() == State.Closed) {
            closeConsumerTasks();
            failPendingReceive().whenComplete((r, t) -> closeFuture.complete(null));
            return closeFuture;
        }

        if (!isConnected()) {
            log.info("[{}] [{}] Closed Consumer (not connected)", topic, subscription);
            setState(State.Closed);
            closeConsumerTasks();
            deregisterFromClientCnx();
            client.cleanupConsumer(this);
            failPendingReceive().whenComplete((r, t) -> closeFuture.complete(null));
            return closeFuture;
        }

        stats.getStatTimeout().ifPresent(Timeout::cancel);

        setState(State.Closing);

        closeConsumerTasks();

        long requestId = client.newRequestId();

        ClientCnx cnx = cnx();
        if (null == cnx) {
            cleanupAtClose(closeFuture, null);
        } else {
            ByteBuf cmd = Commands.newCloseConsumer(consumerId, requestId);
            cnx.sendRequestWithId(cmd, requestId).handle((v, exception) -> {
                final ChannelHandlerContext ctx = cnx.ctx();
                boolean ignoreException = ctx == null || !ctx.channel().isActive();
                if (ignoreException && exception != null) {
                    log.debug("Exception ignored in closing consumer", exception);
                }
                cleanupAtClose(closeFuture, ignoreException ? null : exception);
                return null;
            });
        }

        return closeFuture;
    }

    private void cleanupAtClose(CompletableFuture<Void> closeFuture, Throwable exception) {
        log.info("[{}] [{}] Closed consumer", topic, subscription);
        setState(State.Closed);
        closeConsumerTasks();
        deregisterFromClientCnx();
        client.cleanupConsumer(this);

        // fail all pending-receive futures to notify application
        failPendingReceive().whenComplete((r, t) -> {
            if (exception != null) {
                closeFuture.completeExceptionally(exception);
            } else {
                closeFuture.complete(null);
            }
        });
    }

    private void closeConsumerTasks() {
        unAckedMessageTracker.close();
        if (possibleSendToDeadLetterTopicMessages != null) {
            possibleSendToDeadLetterTopicMessages.clear();
        }
        acknowledgmentsGroupingTracker.close();
        if (batchReceiveTimeout != null) {
            batchReceiveTimeout.cancel();
        }
        negativeAcksTracker.close();
        stats.getStatTimeout().ifPresent(Timeout::cancel);
    }

    void activeConsumerChanged(boolean isActive) {
        if (consumerEventListener == null) {
            return;
        }

        externalPinnedExecutor.execute(() -> {
            if (isActive) {
                consumerEventListener.becameActive(this, partitionIndex);
            } else {
                consumerEventListener.becameInactive(this, partitionIndex);
            }
        });
    }

    protected boolean isBatch(MessageMetadata messageMetadata) {
        // if message is not decryptable then it can't be parsed as a batch-message. so, add EncyrptionCtx to message
        // and return undecrypted payload
        return !isMessageUndecryptable(messageMetadata)
                && (messageMetadata.hasNumMessagesInBatch() || messageMetadata.getNumMessagesInBatch() != 1);
    }

    protected <V> MessageImpl<V> newSingleMessage(final int index,
                                                  final int numMessages,
                                                  final BrokerEntryMetadata brokerEntryMetadata,
                                                  final MessageMetadata msgMetadata,
                                                  final SingleMessageMetadata singleMessageMetadata,
                                                  final ByteBuf payload,
                                                  final MessageIdImpl messageId,
                                                  final Schema<V> schema,
                                                  final boolean containMetadata,
                                                  final BitSetRecyclable ackBitSet,
                                                  final BatchMessageAcker acker,
                                                  final int redeliveryCount,
                                                  final long consumerEpoch) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] [{}] processing message num - {} in batch", subscription, consumerName, index);
        }

        ByteBuf singleMessagePayload = null;
        try {
            if (containMetadata) {
                singleMessagePayload =
                        Commands.deSerializeSingleMessageInBatch(payload, singleMessageMetadata, index, numMessages);
            }

            // If the topic is non-persistent, we should not ignore any messages.
            if (this.topicName.isPersistent() && isSameEntry(messageId) && isPriorBatchIndex(index)) {
                // If we are receiving a batch message, we need to discard messages that were prior
                // to the startMessageId
                if (log.isDebugEnabled()) {
                    log.debug("[{}] [{}] Ignoring message from before the startMessageId: {}", subscription,
                            consumerName, startMessageId);
                }
                return null;
            }

            if (singleMessageMetadata != null && singleMessageMetadata.isCompactedOut()) {
                // message has been compacted out, so don't send to the user
                return null;
            }

            if (ackBitSet != null && !ackBitSet.get(index)) {
                return null;
            }

            BatchMessageIdImpl batchMessageIdImpl = new BatchMessageIdImpl(messageId.getLedgerId(),
                    messageId.getEntryId(), getPartitionIndex(), index, numMessages, acker);

            final ByteBuf payloadBuffer = (singleMessagePayload != null) ? singleMessagePayload : payload;
            final MessageImpl<V> message = MessageImpl.create(topicName.toString(), batchMessageIdImpl,
                    msgMetadata, singleMessageMetadata, payloadBuffer,
                    createEncryptionContext(msgMetadata), cnx(), schema, redeliveryCount, poolMessages, consumerEpoch);
            message.setBrokerEntryMetadata(brokerEntryMetadata);
            return message;
        } catch (IOException | IllegalStateException e) {
            throw new IllegalStateException(e);
        } finally {
            if (singleMessagePayload != null) {
                singleMessagePayload.release();
            }
        }
    }

    protected <V> MessageImpl<V> newMessage(final MessageIdImpl messageId,
                                            final BrokerEntryMetadata brokerEntryMetadata,
                                            final MessageMetadata messageMetadata,
                                            final ByteBuf payload,
                                            final Schema<V> schema,
                                            final int redeliveryCount,
                                            final long consumerEpoch) {
        final MessageImpl<V> message = MessageImpl.create(topicName.toString(), messageId, messageMetadata, payload,
                createEncryptionContext(messageMetadata), cnx(), schema, redeliveryCount, poolMessages, consumerEpoch);
        message.setBrokerEntryMetadata(brokerEntryMetadata);
        return message;
    }

    private void executeNotifyCallback(final MessageImpl<T> message) {
        // Enqueue the message so that it can be retrieved when application calls receive()
        // if the conf.getReceiverQueueSize() is 0 then discard message if no one is waiting for it.
        // if asyncReceive is waiting then notify callback without adding to incomingMessages queue
        internalPinnedExecutor.execute(() -> {
            if (hasNextPendingReceive()) {
                notifyPendingReceivedCallback(message, null);
            } else if (enqueueMessageAndCheckBatchReceive(message) && hasPendingBatchReceive()) {
                // TODO: 11/6/23 批量消息
                notifyPendingBatchReceivedCallBack();
            }
        });
    }

    private void processPayloadByProcessor(final BrokerEntryMetadata brokerEntryMetadata,
                                           final MessageMetadata messageMetadata,
                                           final ByteBuf byteBuf,
                                           final MessageIdImpl messageId,
                                           final Schema<T> schema,
                                           final int redeliveryCount,
                                           final List<Long> ackSet,
                                           long consumerEpoch) {
        final MessagePayloadImpl payload = MessagePayloadImpl.create(byteBuf);
        final MessagePayloadContextImpl entryContext = MessagePayloadContextImpl.get(
                brokerEntryMetadata, messageMetadata, messageId, this, redeliveryCount, ackSet, consumerEpoch);
        final AtomicInteger skippedMessages = new AtomicInteger(0);
        try {
            conf.getPayloadProcessor().process(payload, entryContext, schema, message -> {
                if (message != null) {
                    executeNotifyCallback((MessageImpl<T>) message);
                } else {
                    skippedMessages.incrementAndGet();
                }
            });
        } catch (Throwable throwable) {
            log.warn("[{}] [{}] unable to obtain message in batch", subscription, consumerName, throwable);
            discardCorruptedMessage(messageId, cnx(), ValidationError.BatchDeSerializeError);
        } finally {
            entryContext.recycle();
            payload.release(); // byteBuf.release() is called in this method
        }

        if (skippedMessages.get() > 0) {
            increaseAvailablePermits(cnx(), skippedMessages.get());
        }

        tryTriggerListener();
    }

    // TODO: 11/6/23 处理从broker端接收到的msg 
    void messageReceived(CommandMessage cmdMessage, ByteBuf headersAndPayload, ClientCnx cnx) {
        List<Long> ackSet = Collections.emptyList();
        // TODO: 11/6/23 检查此消息是否已确认，如果已确认，则忽略（丢弃）
        if (cmdMessage.getAckSetsCount() > 0) {
            ackSet = new ArrayList<>(cmdMessage.getAckSetsCount());
            for (int i = 0; i < cmdMessage.getAckSetsCount(); i++) {
                ackSet.add(cmdMessage.getAckSetAt(i));
            }
        }
        int redeliveryCount = cmdMessage.getRedeliveryCount();
        MessageIdData messageId = cmdMessage.getMessageId(); // TODO: 11/6/23 [LedgerId, EntryId]
        long consumerEpoch = DEFAULT_CONSUMER_EPOCH;
        // if broker send messages to client with consumerEpoch, we should set consumerEpoch to message
        if (cmdMessage.hasConsumerEpoch()) {
            consumerEpoch = cmdMessage.getConsumerEpoch();
        }
        if (log.isDebugEnabled()) {
            log.debug("[{}][{}] Received message: {}/{}", topic, subscription, messageId.getLedgerId(),
                    messageId.getEntryId());
        }

        // TODO: 11/6/23 进行消息校验，是否已损坏
        if (!verifyChecksum(headersAndPayload, messageId)) {
            // TODO: 11/6/23 消息校验失败，则丢弃这个消息，并通知 broker 重发， error类型：ChecksumMismatch
            // discard message with checksum error
            discardCorruptedMessage(messageId, cnx, ValidationError.ChecksumMismatch);
            return;
        }

        BrokerEntryMetadata brokerEntryMetadata;
        MessageMetadata msgMetadata;
        try {
            // TODO: 11/6/23 尝试解析消息
            brokerEntryMetadata = Commands.parseBrokerEntryMetadataIfExist(headersAndPayload);
            msgMetadata = Commands.parseMessageMetadata(headersAndPayload);
        } catch (Throwable t) {
            discardCorruptedMessage(messageId, cnx, ValidationError.ChecksumMismatch);
            return;
        }

        // TODO: 11/6/23 消息条数
        final int numMessages = msgMetadata.getNumMessagesInBatch();
        // TODO: 11/6/23 消息块大小 ，如果没有分块发送，则块大小为0， 否则，块大小大于1（1， >=2， 3， 4）
        final int numChunks = msgMetadata.hasNumChunksFromMsg() ? msgMetadata.getNumChunksFromMsg() : 0;
        // TODO: 11/6/23 是否为分块发送消息 , 分块大小大于1， 并且定于类型不是Shared的类型
        final boolean isChunkedMessage = numChunks > 1 && conf.getSubscriptionType() != SubscriptionType.Shared;

        // TODO: 11/6/23 根据ledgerId， entryId和分区id构建MessageIdImpl实例
        MessageIdImpl msgId = new MessageIdImpl(messageId.getLedgerId(), messageId.getEntryId(), getPartitionIndex());
        // TODO: 11/6/23 消息去重处理
        if (acknowledgmentsGroupingTracker.isDuplicate(msgId)) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] Ignoring message as it was already being acked earlier by same consumer {}/{}",
                        topic, subscription, consumerName, msgId);
            }

            increaseAvailablePermits(cnx, numMessages);
            return;
        }

        // TODO: 11/6/23 如果需要，解密消息
        ByteBuf decryptedPayload = decryptPayloadIfNeeded(messageId, redeliveryCount, msgMetadata, headersAndPayload,
                cnx);

        // TODO: 11/6/23 消息是否已解密
        boolean isMessageUndecryptable = isMessageUndecryptable(msgMetadata);

        // TODO: 11/6/23 消息已丢弃或 CryptoKeyReader 接口没有实现
        if (decryptedPayload == null) {
            // Message was discarded or CryptoKeyReader isn't implemented
            return;
        }

        // TODO: 11/6/23 解压已解密的消息，并且释放已解密的字节缓存 
        // uncompress decryptedPayload and release decryptedPayload-ByteBuf
        ByteBuf uncompressedPayload = (isMessageUndecryptable || isChunkedMessage) ? decryptedPayload.retain()
                : uncompressPayloadIfNeeded(messageId, msgMetadata, decryptedPayload, cnx, true);
        decryptedPayload.release();
        if (uncompressedPayload == null) {
            // TODO: 11/6/23 一旦消息解压错误，则丢弃
            // Message was discarded on decompression error
            return;
        }

        if (conf.getPayloadProcessor() != null) {
            // uncompressedPayload is released in this method so we don't need to call release() again
            processPayloadByProcessor(brokerEntryMetadata, msgMetadata,
                    uncompressedPayload, msgId, schema, redeliveryCount, ackSet, consumerEpoch);
            return;
        }

        // TODO: 11/6/23 如果消息没有解密，它不能解析成一个批量消息，故增加 加密上下文到消息，返回未加密的消息体
        // if message is not decryptable then it can't be parsed as a batch-message. so, add EncyrptionCtx to message
        // and return undecrypted payload
        if (isMessageUndecryptable || (numMessages == 1 && !msgMetadata.hasNumMessagesInBatch())) {

            // TODO: 11/6/23 如果是分块消息
            // right now, chunked messages are only supported by non-shared subscription
            if (isChunkedMessage) {
                // TODO: 11/6/23 处理分块消息，对消息进行解压缩
                uncompressedPayload = processMessageChunk(uncompressedPayload, msgMetadata, msgId, messageId, cnx);
                if (uncompressedPayload == null) {
                    return;
                }

                // TODO: 11/6/23 最后一个分块 消息接收后，即所有的分块消息都被接收
                // last chunk received: so, stitch chunked-messages and clear up chunkedMsgBuffer
                if (log.isDebugEnabled()) {
                    log.debug("Chunked message completed chunkId {}, total-chunks {}, msgId {} sequenceId {}",
                            msgMetadata.getChunkId(), msgMetadata.getNumChunksFromMsg(), msgId,
                            msgMetadata.getSequenceId());
                }

                // TODO: 11/6/23 从 chunkedMessagesMap 里面移除 msgUUID
                // remove buffer from the map, set the chunk message id
                ChunkedMessageCtx chunkedMsgCtx = chunkedMessagesMap.remove(msgMetadata.getUuid());
                if (chunkedMsgCtx.chunkedMessageIds.length > 0) {
                    // TODO: 11/6/23  把所有的消息块组装起来，创建一个ChunkMessageIdImpl实例
                    msgId = new ChunkMessageIdImpl(chunkedMsgCtx.chunkedMessageIds[0],
                            chunkedMsgCtx.chunkedMessageIds[chunkedMsgCtx.chunkedMessageIds.length - 1]);
                }
                // TODO: 11/6/23 把 组装好的消息放入到 unAckedChunkedMessageIdSequenceMap
                // add chunked messageId to unack-message tracker, and reduce pending-chunked-message count
                unAckedChunkedMessageIdSequenceMap.put(msgId, chunkedMsgCtx.chunkedMessageIds);
                pendingChunkedMessageCount--;
                chunkedMsgCtx.recycle();
            }

            // If the topic is non-persistent, we should not ignore any messages.
            if (this.topicName.isPersistent() && isSameEntry(msgId) && isPriorEntryIndex(messageId.getEntryId())) {
                // We need to discard entries that were prior to startMessageId
                if (log.isDebugEnabled()) {
                    log.debug("[{}] [{}] Ignoring message from before the startMessageId: {}", subscription,
                            consumerName, startMessageId);
                }

                uncompressedPayload.release();
                return;
            }

            // TODO: 11/6/23 构建一个消息
            final MessageImpl<T> message =
                    newMessage(msgId, brokerEntryMetadata, msgMetadata, uncompressedPayload,
                            schema, redeliveryCount, consumerEpoch);
            uncompressedPayload.release();

            // TODO: 11/6/23 如果满足死信队列的要求，把消息放入到死信队列
            if (deadLetterPolicy != null && possibleSendToDeadLetterTopicMessages != null
                    && redeliveryCount >= deadLetterPolicy.getMaxRedeliverCount()) {
                possibleSendToDeadLetterTopicMessages.put((MessageIdImpl) message.getMessageId(),
                        Collections.singletonList(message));
            }
            executeNotifyCallback(message);
        } else {
            // TODO: 11/6/23 处理批量消息入队列，批处理解压缩所有的消息
            // handle batch message enqueuing; uncompressed payload has all messages in batch
            receiveIndividualMessagesFromBatch(brokerEntryMetadata, msgMetadata, redeliveryCount, ackSet,
                    uncompressedPayload, messageId, cnx, consumerEpoch);

            uncompressedPayload.release();
        }
        tryTriggerListener();

    }

    private ByteBuf processMessageChunk(ByteBuf compressedPayload, MessageMetadata msgMetadata, MessageIdImpl msgId,
            MessageIdData messageId, ClientCnx cnx) {
        // TODO: 11/6/23 分块消息处理

        // Lazy task scheduling to expire incomplete chunk message
        if (!expireChunkMessageTaskScheduled && expireTimeOfIncompleteChunkedMessageMillis > 0) {
            internalPinnedExecutor
                    .scheduleAtFixedRate(catchingAndLoggingThrowables(this::removeExpireIncompleteChunkedMessages),
                            expireTimeOfIncompleteChunkedMessageMillis, expireTimeOfIncompleteChunkedMessageMillis, // 默认1 min
                            TimeUnit.MILLISECONDS);
            expireChunkMessageTaskScheduled = true;
        }

        if (msgMetadata.getChunkId() == 0) {
            ByteBuf chunkedMsgBuffer = PulsarByteBufAllocator.DEFAULT.buffer(msgMetadata.getTotalChunkMsgSize(),
                    msgMetadata.getTotalChunkMsgSize());
            int totalChunks = msgMetadata.getNumChunksFromMsg();
            chunkedMessagesMap.computeIfAbsent(msgMetadata.getUuid(),
                    (key) -> ChunkedMessageCtx.get(totalChunks, chunkedMsgBuffer));
            pendingChunkedMessageCount++;
            // TODO: 11/6/23 maxPendingChunkedMessage默认为10， 这里好像有问题：如果一个消息块大小超过了10，那么这个消息就一直组装不成一个消息！！！
            if (maxPendingChunkedMessage > 0 && pendingChunkedMessageCount > maxPendingChunkedMessage) {
                removeOldestPendingChunkedMessage();
            }
            pendingChunkedMessageUuidQueue.add(msgMetadata.getUuid());
        }

        ChunkedMessageCtx chunkedMsgCtx = chunkedMessagesMap.get(msgMetadata.getUuid());
        // TODO: 11/6/23  扔掉乱序的msg
        // discard message if chunk is out-of-order
        if (chunkedMsgCtx == null || chunkedMsgCtx.chunkedMsgBuffer == null
                || msgMetadata.getChunkId() != (chunkedMsgCtx.lastChunkedMessageId + 1)
                || msgMetadata.getChunkId() >= msgMetadata.getTotalChunkMsgSize()) {
            // means we lost the first chunk: should never happen
            log.info("Received unexpected chunk messageId {}, last-chunk-id{}, chunkId = {}, total-chunks {}", msgId,
                    (chunkedMsgCtx != null ? chunkedMsgCtx.lastChunkedMessageId : null), msgMetadata.getChunkId(),
                    msgMetadata.getTotalChunkMsgSize());
            if (chunkedMsgCtx != null) {
                if (chunkedMsgCtx.chunkedMsgBuffer != null) {
                    ReferenceCountUtil.safeRelease(chunkedMsgCtx.chunkedMsgBuffer);
                }
                chunkedMsgCtx.recycle();
            }
            chunkedMessagesMap.remove(msgMetadata.getUuid());
            compressedPayload.release();
            increaseAvailablePermits(cnx);
            if (expireTimeOfIncompleteChunkedMessageMillis > 0
                    && System.currentTimeMillis() > (msgMetadata.getPublishTime()
                            + expireTimeOfIncompleteChunkedMessageMillis)) {
                doAcknowledge(msgId, AckType.Individual, Collections.emptyMap(), null);
            } else {
                trackMessage(msgId);
            }
            return null;
        }

        chunkedMsgCtx.chunkedMessageIds[msgMetadata.getChunkId()] = msgId;
        // append the chunked payload and update lastChunkedMessage-id
        chunkedMsgCtx.chunkedMsgBuffer.writeBytes(compressedPayload);
        chunkedMsgCtx.lastChunkedMessageId = msgMetadata.getChunkId();

        // if final chunk is not received yet then release payload and return
        if (msgMetadata.getChunkId() != (msgMetadata.getNumChunksFromMsg() - 1)) {
            compressedPayload.release();
            increaseAvailablePermits(cnx);
            return null;
        }

        compressedPayload.release();
        compressedPayload = chunkedMsgCtx.chunkedMsgBuffer;
        // TODO: 11/6/23 会对msg进行解压缩
        ByteBuf uncompressedPayload = uncompressPayloadIfNeeded(messageId, msgMetadata, compressedPayload, cnx, false);
        compressedPayload.release();
        return uncompressedPayload;
    }

    /**
     * Notify waiting asyncReceive request with the received message.
     *
     * @param message
     */
    void notifyPendingReceivedCallback(final Message<T> message, Exception exception) {
        if (pendingReceives.isEmpty()) {
            return;
        }

        // fetch receivedCallback from queue
        final CompletableFuture<Message<T>> receivedFuture = nextPendingReceive();
        if (receivedFuture == null) {
            return;
        }

        if (exception != null) {
            internalPinnedExecutor.execute(() -> receivedFuture.completeExceptionally(exception));
            return;
        }

        // TODO: 11/6/23 msg 为空处理异常
        if (message == null) {
            IllegalStateException e = new IllegalStateException("received message can't be null");
            internalPinnedExecutor.execute(() -> receivedFuture.completeExceptionally(e));
            return;
        }

        // TODO: 11/6/23 如果队列大小设置为0，将立即调用拦截器和完成接收回调， 默认为1000
        if (conf.getReceiverQueueSize() == 0) {
            // call interceptor and complete received callback
            trackMessage(message);
            interceptAndComplete(message, receivedFuture);
            return;
        }

        // TODO: 11/6/23 记录应用处理了一条消息的事件。并尝试发送一个 Flow 命令通知 broker 可以推送消息了
        // increase permits for available message-queue
        messageProcessed(message);
        // call interceptor and complete received callback
        interceptAndComplete(message, receivedFuture);
    }

    private void interceptAndComplete(final Message<T> message, final CompletableFuture<Message<T>> receivedFuture) {
        // TODO: 11/6/23 调用拦截器
        // call proper interceptor
        final Message<T> interceptMessage = beforeConsume(message);
        // TODO: 11/6/23 设置完成标志
        // return message to receivedCallback
        completePendingReceive(receivedFuture, interceptMessage);
    }

    void receiveIndividualMessagesFromBatch(BrokerEntryMetadata brokerEntryMetadata, MessageMetadata msgMetadata,
                                            int redeliveryCount, List<Long> ackSet, ByteBuf uncompressedPayload,
                                            MessageIdData messageId, ClientCnx cnx, long consumerEpoch) {
        int batchSize = msgMetadata.getNumMessagesInBatch();

        // TODO: 11/6/23 创建一个 MessageIdImpl 实例
        // create ack tracker for entry aka batch
        MessageIdImpl batchMessage = new MessageIdImpl(messageId.getLedgerId(), messageId.getEntryId(),
                getPartitionIndex());
        List<MessageImpl<T>> possibleToDeadLetter = null;
        if (deadLetterPolicy != null && redeliveryCount >= deadLetterPolicy.getMaxRedeliverCount()) {
            possibleToDeadLetter = new ArrayList<>();
        }

        BatchMessageAcker acker = BatchMessageAcker.newAcker(batchSize);
        BitSetRecyclable ackBitSet = null;
        if (ackSet != null && ackSet.size() > 0) {
            ackBitSet = BitSetRecyclable.valueOf(SafeCollectionUtils.longListToArray(ackSet));
        }

        SingleMessageMetadata singleMessageMetadata = new SingleMessageMetadata();
        int skippedMessages = 0;
        try {
            for (int i = 0; i < batchSize; ++i) {
                // TODO: 11/6/23 构建 MessageImpl 实例
                final MessageImpl<T> message = newSingleMessage(i, batchSize, brokerEntryMetadata, msgMetadata,
                        singleMessageMetadata, uncompressedPayload, batchMessage, schema, true,
                        ackBitSet, acker, redeliveryCount, consumerEpoch);
                if (message == null) {
                    skippedMessages++;
                    continue;
                }
                // TODO: 11/6/23 如果死信队列不为空，则把消息放入里面
                if (possibleToDeadLetter != null) {
                    possibleToDeadLetter.add(message);
                }
                executeNotifyCallback(message);
            }
            if (ackBitSet != null) {
                ackBitSet.recycle();
            }
        } catch (IllegalStateException e) {
            // TODO: 11/6/23 不能从batch里面获取msg移除处理
            log.warn("[{}] [{}] unable to obtain message in batch", subscription, consumerName, e);
            discardCorruptedMessage(messageId, cnx, ValidationError.BatchDeSerializeError);
        }

        if (possibleToDeadLetter != null && possibleSendToDeadLetterTopicMessages != null) {
            possibleSendToDeadLetterTopicMessages.put(batchMessage, possibleToDeadLetter);
        }

        if (log.isDebugEnabled()) {
            log.debug("[{}] [{}] enqueued messages in batch. queue size - {}, available queue size - {}", subscription,
                    consumerName, incomingMessages.size(), incomingMessages.remainingCapacity());
        }

        if (skippedMessages > 0) {
            increaseAvailablePermits(cnx, skippedMessages);
        }
    }

    private boolean isPriorEntryIndex(long idx) {
        return resetIncludeHead ? idx < startMessageId.getEntryId() : idx <= startMessageId.getEntryId();
    }

    private boolean isPriorBatchIndex(long idx) {
        return resetIncludeHead ? idx < startMessageId.getBatchIndex() : idx <= startMessageId.getBatchIndex();
    }

    private boolean isSameEntry(MessageIdImpl messageId) {
        return startMessageId != null
                && messageId.getLedgerId() == startMessageId.getLedgerId()
                && messageId.getEntryId() == startMessageId.getEntryId();
    }

    /**
     * Record the event that one message has been processed by the application.
     *
     * Periodically, it sends a Flow command to notify the broker that it can push more messages
     */
    @Override
    protected synchronized void messageProcessed(Message<?> msg) {
        // TODO: 11/3/23 消费者客户端信息
        ClientCnx currentCnx = cnx();
        // TODO: 11/3/23 消息携带的信息
        ClientCnx msgCnx = ((MessageImpl<?>) msg).getCnx();
        // TODO: 11/3/23 更新 lastDequeuedMessageId 为该msg的Id
        lastDequeuedMessageId = msg.getMessageId();

        if (msgCnx != currentCnx) {
            // The processed message did belong to the old queue that was cleared after reconnection.
        } else {
            if (listener == null && !parentConsumerHasListener) {
                // TODO: 11/3/23 增加可用许可 +1
                increaseAvailablePermits(currentCnx);
            }
            // TODO: 11/3/23 记录消息的状态 ,主要记录msgNum，msgSize
            stats.updateNumMsgsReceived(msg);

            // TODO: 11/3/23 把消息放入跟踪未确认消息记录器
            trackMessage(msg);
        }
        // TODO: 11/3/23 因为这个消息被处理，应该把incoming msg size 减去 当前msg的数量 
        decreaseIncomingMessageSize(msg);
    }

    protected void trackMessage(Message<?> msg) {
        if (msg != null) {
            trackMessage(msg.getMessageId(), msg.getRedeliveryCount());
        }
    }

    protected void trackMessage(MessageId messageId) {
            trackMessage(messageId, 0);
    }

    protected void trackMessage(MessageId messageId, int redeliveryCount) {
        // TODO: 11/3/23  ackTimeout默认应该是30s，
        if (conf.getAckTimeoutMillis() > 0 && messageId instanceof MessageIdImpl) {
            MessageIdImpl id = (MessageIdImpl) messageId;
            // TODO: 11/3/23  对于批量消息实现，不需要每个消息都放入跟踪器
            if (id instanceof BatchMessageIdImpl) {
                // do not add each item in batch message into tracker
                id = new MessageIdImpl(id.getLedgerId(), id.getEntryId(), getPartitionIndex());
            }
            if (hasParentConsumer) {
                //TODO: check parent consumer here
                // we should no longer track this message, TopicsConsumer will take care from now onwards
                unAckedMessageTracker.remove(id);
            } else {
                // TODO: 11/3/23 没有ACK的消息放入 消息跟踪器
                unAckedMessageTracker.add(id, redeliveryCount);
            }
        }
    }

    void increaseAvailablePermits(MessageImpl<?> msg) {
        ClientCnx currentCnx = cnx();
        ClientCnx msgCnx = msg.getCnx();
        if (msgCnx == currentCnx) {
            increaseAvailablePermits(currentCnx);
        }
    }

    void increaseAvailablePermits(ClientCnx currentCnx) {
        increaseAvailablePermits(currentCnx, 1);
    }

    protected void increaseAvailablePermits(ClientCnx currentCnx, int delta) {
        // TODO: 11/2/23 delta 
        int available = AVAILABLE_PERMITS_UPDATER.addAndGet(this, delta);

        // TODO: 11/2/23  receiverQueueRefillThreshold=500 默认。
        while (available >= receiverQueueRefillThreshold && !paused) {
            // TODO: 11/2/23 如果可用许可里面就有 available值，则把 AVAILABLE_PERMITS_UPDATER 设置为0
            if (AVAILABLE_PERMITS_UPDATER.compareAndSet(this, available, 0)) {
                // TODO: 11/2/23 向broker发起拉数据的请求， 发送流控命令到 broker
                sendFlowPermitsToBroker(currentCnx, available);
                break;
            } else {
                // TODO: 11/2/23 全部拿完可用许可
                available = AVAILABLE_PERMITS_UPDATER.get(this);
            }
        }
    }

    public void increaseAvailablePermits(int delta) {
        increaseAvailablePermits(cnx(), delta);
    }

    @Override
    public void pause() {
        paused = true;
    }

    @Override
    public void resume() {
        if (paused) {
            paused = false;
            increaseAvailablePermits(cnx(), 0);
        }
    }

    @Override
    public long getLastDisconnectedTimestamp() {
        return connectionHandler.lastConnectionClosedTimestamp;
    }

    // TODO: 11/6/23 如果需要，则解密加密的消息
    private ByteBuf decryptPayloadIfNeeded(MessageIdData messageId, int redeliveryCount, MessageMetadata msgMetadata,
                                           ByteBuf payload, ClientCnx currentCnx) {

        if (msgMetadata.getEncryptionKeysCount() == 0) {
            return payload.retain();
        }

        // TODO: 11/6/23 如果密钥读取器没有配置，按照系统配置进行处理
        // If KeyReader is not configured throw exception based on config param
        if (conf.getCryptoKeyReader() == null) {
            switch (conf.getCryptoFailureAction()) {
                case CONSUME:
                    log.warn("[{}][{}][{}] CryptoKeyReader interface is not implemented. Consuming encrypted message.",
                            topic, subscription, consumerName);
                    return payload.retain();
                case DISCARD:
                    log.warn(
                            "[{}][{}][{}] Skipping decryption since CryptoKeyReader interface is not implemented and"
                                    + " config is set to discard",
                            topic, subscription, consumerName);
                    discardMessage(messageId, currentCnx, ValidationError.DecryptionError);
                    return null;
                case FAIL:
                    MessageId m = new MessageIdImpl(messageId.getLedgerId(), messageId.getEntryId(), partitionIndex);
                    log.error(
                            "[{}][{}][{}][{}] Message delivery failed since CryptoKeyReader interface is not"
                                    + " implemented to consume encrypted message",
                            topic, subscription, consumerName, m);
                    unAckedMessageTracker.add(m, redeliveryCount);
                    return null;
            }
        }


        int maxDecryptedSize = msgCrypto.getMaxOutputSize(payload.readableBytes());
        ByteBuf decryptedData = PulsarByteBufAllocator.DEFAULT.buffer(maxDecryptedSize);
        ByteBuffer nioDecryptedData = decryptedData.nioBuffer(0, maxDecryptedSize);
        // TODO: 11/6/23 解密消息 
        if (msgCrypto.decrypt(() -> msgMetadata, payload.nioBuffer(), nioDecryptedData, conf.getCryptoKeyReader())) {
            decryptedData.writerIndex(nioDecryptedData.limit());
            // TODO: 11/6/23 如果解密成功，直接返回
            return decryptedData;
        }

        decryptedData.release();

        // TODO: 11/6/23 解密失败时，系统根据配置处理
        switch (conf.getCryptoFailureAction()) {
            case CONSUME: // 注意，即使配置（CryptoFailureAction）为消费选项，批量消息也会解密失败，这里往下传。
                // Note, batch message will fail to consume even if config is set to consume
                log.warn("[{}][{}][{}][{}] Decryption failed. Consuming encrypted message since config is set to"
                                + " consume.",
                        topic, subscription, consumerName, messageId);
                return payload.retain();
            case DISCARD://CryptoFailureAction配置为丢弃，如果解密失败，（意味着不会给上层应用），系统将自动确认，跳过这个消息
                log.warn("[{}][{}][{}][{}] Discarding message since decryption failed and config is set to discard",
                        topic, subscription, consumerName, messageId);
                discardMessage(messageId, currentCnx, ValidationError.DecryptionError);
                return null;
            case FAIL://CryptoFailureAction配置为失败选项，不能解密消息，消息投递失败。
                MessageId m = new MessageIdImpl(messageId.getLedgerId(), messageId.getEntryId(), partitionIndex);
                log.error(
                        "[{}][{}][{}][{}] Message delivery failed since unable to decrypt incoming message",
                        topic, subscription, consumerName, m);
                unAckedMessageTracker.add(m, redeliveryCount);
                return null;
        }
        return null;
    }

    private ByteBuf uncompressPayloadIfNeeded(MessageIdData messageId, MessageMetadata msgMetadata, ByteBuf payload,
            ClientCnx currentCnx, boolean checkMaxMessageSize) {
        // TODO: 11/6/23 获取压缩类型
        CompressionType compressionType = msgMetadata.getCompression();
        // TODO: 11/6/23 根据压缩类型，生成对应的压缩器实例
        CompressionCodec codec = CompressionCodecProvider.getCompressionCodec(compressionType);
        int uncompressedSize = msgMetadata.getUncompressedSize();
        int payloadSize = payload.readableBytes();
        // TODO: 11/6/23 如果超过了最大的传输大小 5MB， 则报错
        if (checkMaxMessageSize && payloadSize > ClientCnx.getMaxMessageSize()) {
            // payload size is itself corrupted since it cannot be bigger than the MaxMessageSize
            log.error("[{}][{}] Got corrupted payload message size {} at {}", topic, subscription, payloadSize,
                    messageId);
            // TODO: 11/6/23 丢弃此消息，并尝试通知 broker 推送消息
            discardCorruptedMessage(messageId, currentCnx, ValidationError.UncompressedSizeCorruption);
            return null;
        }
        try {
            // TODO: 11/6/23 对message进行解压缩
            ByteBuf uncompressedPayload = codec.decode(payload, uncompressedSize);
            return uncompressedPayload;
        } catch (IOException e) {
            // TODO: 11/6/23 解压缩报错
            log.error("[{}][{}] Failed to decompress message with {} at {}: {}", topic, subscription, compressionType,
                    messageId, e.getMessage(), e);
            discardCorruptedMessage(messageId, currentCnx, ValidationError.DecompressionError);
            return null;
        }
    }

    // TODO: 11/6/23 验证校验和
    private boolean verifyChecksum(ByteBuf headersAndPayload, MessageIdData messageId) {

        // TODO: 11/6/23 判定是否有校验和
        if (hasChecksum(headersAndPayload)) {
            // TODO: 11/6/23 获取原始校验和
            int checksum = Commands.readChecksum(headersAndPayload);
            // TODO: 11/6/23 重新计算校验和
            int computedChecksum = Crc32cIntChecksum.computeChecksum(headersAndPayload);
            // TODO: 11/6/23 如果不匹配，则校验不通过
            if (checksum != computedChecksum) {
                log.error(
                        "[{}][{}] Checksum mismatch for message at {}:{}. Received checksum: 0x{},"
                                + " Computed checksum: 0x{}",
                        topic, subscription, messageId.getLedgerId(), messageId.getEntryId(),
                        Long.toHexString(checksum), Integer.toHexString(computedChecksum));
                return false;
            }
        }

        return true;
    }

    private void discardCorruptedMessage(MessageIdImpl messageId, ClientCnx currentCnx,
                                         ValidationError validationError) {
        log.error("[{}][{}] Discarding corrupted message at {}:{}", topic, subscription, messageId.getLedgerId(),
                messageId.getEntryId());
        ByteBuf cmd = Commands.newAck(consumerId, messageId.getLedgerId(), messageId.getEntryId(), null,
                AckType.Individual, validationError, Collections.emptyMap(), -1);
        currentCnx.ctx().writeAndFlush(cmd, currentCnx.ctx().voidPromise());
        increaseAvailablePermits(currentCnx);
        stats.incrementNumReceiveFailed();
    }

    private void discardCorruptedMessage(MessageIdData messageId, ClientCnx currentCnx,
            ValidationError validationError) {
        // TODO: 11/6/23 消息校验失败，则丢弃这个消息，并通知 broker 重发
        log.error("[{}][{}] Discarding corrupted message at {}:{}", topic, subscription, messageId.getLedgerId(),
                messageId.getEntryId());
        discardMessage(messageId, currentCnx, validationError);
    }

    private void discardMessage(MessageIdData messageId, ClientCnx currentCnx, ValidationError validationError) {
        // TODO: 11/6/23 消息校验失败，则丢弃这个消息，并通知 broker 重发
        ByteBuf cmd = Commands.newAck(consumerId, messageId.getLedgerId(), messageId.getEntryId(), null,
                AckType.Individual, validationError, Collections.emptyMap(), -1);
        currentCnx.ctx().writeAndFlush(cmd, currentCnx.ctx().voidPromise());
        // TODO: 11/6/23  增加 permit，通知broker重发
        increaseAvailablePermits(currentCnx);
        // TODO: 11/6/23 增加接收失败数目
        stats.incrementNumReceiveFailed();
    }

    @Override
    String getHandlerName() {
        return subscription;
    }

    @Override
    public boolean isConnected() {
        return getClientCnx() != null && (getState() == State.Ready);
    }

    public boolean isConnected(ClientCnx cnx) {
        return cnx != null && (getState() == State.Ready);
    }

    int getPartitionIndex() {
        return partitionIndex;
    }

    @Override
    public int getAvailablePermits() {
        return AVAILABLE_PERMITS_UPDATER.get(this);
    }

    @Override
    public int numMessagesInQueue() {
        return incomingMessages.size();
    }

    @Override
    public void redeliverUnacknowledgedMessages() {
        // First : synchronized in order to handle consumer reconnect produce race condition, when broker receive
        // redeliverUnacknowledgedMessages and consumer have not be created and
        // then receive reconnect epoch change the broker is smaller than the client epoch, this will cause client epoch
        // smaller than broker epoch forever. client will not receive message anymore.
        // Second : we should synchronized `ClientCnx cnx = cnx()` to
        // prevent use old cnx to send redeliverUnacknowledgedMessages to a old broker
        synchronized (ConsumerImpl.this) {
            ClientCnx cnx = cnx();
            // V1 don't support redeliverUnacknowledgedMessages
            if (cnx != null && cnx.getRemoteEndpointProtocolVersion() < ProtocolVersion.v2.getValue()) {
                if ((getState() == State.Connecting)) {
                    log.warn("[{}] Client Connection needs to be established "
                            + "for redelivery of unacknowledged messages", this);
                } else {
                    log.warn("[{}] Reconnecting the client to redeliver the messages.", this);
                    cnx.ctx().close();
                }

                return;
            }

            // clear local message
            int currentSize = 0;
            currentSize = incomingMessages.size();
            clearIncomingMessages();
            unAckedMessageTracker.clear();

            // we should increase epoch every time, because MultiTopicsConsumerImpl also increase it,
            // we need to keep both epochs the same
            if (conf.getSubscriptionType() == SubscriptionType.Failover
                    || conf.getSubscriptionType() == SubscriptionType.Exclusive) {
                CONSUMER_EPOCH.incrementAndGet(this);
            }
            // is channel is connected, we should send redeliver command to broker
            if (cnx != null && isConnected(cnx)) {
                cnx.ctx().writeAndFlush(Commands.newRedeliverUnacknowledgedMessages(
                        consumerId, CONSUMER_EPOCH.get(this)), cnx.ctx().voidPromise());
                if (currentSize > 0) {
                    increaseAvailablePermits(cnx, currentSize);
                }
                if (log.isDebugEnabled()) {
                    log.debug("[{}] [{}] [{}] Redeliver unacked messages and send {} permits", subscription, topic,
                            consumerName, currentSize);
                }
            } else {
                log.warn("[{}] Send redeliver messages command but the client is reconnect or close, "
                        + "so don't need to send redeliver command to broker", this);
            }
        }
    }

    public int clearIncomingMessagesAndGetMessageNumber() {
        int messagesNumber = incomingMessages.size();
        incomingMessages.forEach(Message::release);
        clearIncomingMessages();
        unAckedMessageTracker.clear();
        return messagesNumber;
    }

    // TODO: 11/6/23 通知 broker 重新投递消息：主要两件事 处理过期消息，处理死信消息
    @Override
    public void redeliverUnacknowledgedMessages(Set<MessageId> messageIds) {
        if (messageIds.isEmpty()) {
            return;
        }

        checkArgument(messageIds.stream().findFirst().get() instanceof MessageIdImpl);

        if (conf.getSubscriptionType() != SubscriptionType.Shared
                && conf.getSubscriptionType() != SubscriptionType.Key_Shared) {
            // TODO: 11/6/23 如果订阅类型不是共享类型，则不重发单个消息
            // We cannot redeliver single messages if subscription type is not Shared
            redeliverUnacknowledgedMessages();
            return;
        }
        ClientCnx cnx = cnx();
        // TODO: 11/6/23 判定是否已连接和版本是否支持 
        if (isConnected() && cnx.getRemoteEndpointProtocolVersion() >= ProtocolVersion.v2.getValue()) {
            // TODO: 11/6/23 从 incomingMessages 中移除过期的msg
            int messagesFromQueue = removeExpiredMessagesFromQueue(messageIds);
            Iterable<List<MessageIdImpl>> batches = Iterables.partition(
                messageIds.stream()
                    .map(messageId -> (MessageIdImpl) messageId)
                    .collect(Collectors.toSet()), MAX_REDELIVER_UNACKNOWLEDGED); //限制当前次处理的未确认消息数
            batches.forEach(ids -> {
                // TODO: 11/6/23 尝试把有一些msg发送到死信topic里面
                getRedeliveryMessageIdData(ids).thenAccept(messageIdData -> {
                    if (!messageIdData.isEmpty()) {
                        // TODO: 11/6/23  这里就是发命令通知 broker 重发本次未确认消息
                        ByteBuf cmd = Commands.newRedeliverUnacknowledgedMessages(consumerId, messageIdData);
                        cnx.ctx().writeAndFlush(cmd, cnx.ctx().voidPromise());
                    }
                });
            });
            if (messagesFromQueue > 0) {
                increaseAvailablePermits(cnx, messagesFromQueue);
            }
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] [{}] Redeliver unacked messages and increase {} permits", subscription, topic,
                        consumerName, messagesFromQueue);
            }
            return;
        }
        if (cnx == null || (getState() == State.Connecting)) {
            log.warn("[{}] Client Connection needs to be established for redelivery of unacknowledged messages", this);
        } else {
            log.warn("[{}] Reconnecting the client to redeliver the messages.", this);
            cnx.ctx().close();
        }
    }

    @Override
    protected void completeOpBatchReceive(OpBatchReceive<T> op) {
        notifyPendingBatchReceivedCallBack(op);
    }

    private CompletableFuture<List<MessageIdData>> getRedeliveryMessageIdData(List<MessageIdImpl> messageIds) {
        if (messageIds == null || messageIds.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }
        List<CompletableFuture<MessageIdData>> futures = messageIds.stream().map(messageId -> {
            // TODO: 11/6/23 把消息发到死信队列 
            CompletableFuture<Boolean> future = processPossibleToDLQ(messageId);
            return future.thenApply(sendToDLQ -> {
                if (!sendToDLQ) {
                    return new MessageIdData()
                            .setPartition(messageId.getPartitionIndex())
                            .setLedgerId(messageId.getLedgerId())
                            .setEntryId(messageId.getEntryId());
                }
                return null;
            });
        }).collect(Collectors.toList());
        // TODO: 11/6/23 把发送到死信topic失败的msg移除，成功的发送到死信topic的msg，就不需要从broker端从新发送过来。
        return FutureUtil.waitForAll(futures).thenApply(v ->
                futures.stream().map(CompletableFuture::join).filter(Objects::nonNull).collect(Collectors.toList()));
    }

    private CompletableFuture<Boolean> processPossibleToDLQ(MessageIdImpl messageId) {
        List<MessageImpl<T>> deadLetterMessages = null;
        // TODO: 11/6/23  死信消息容器 不为null
        if (possibleSendToDeadLetterTopicMessages != null) {
            if (messageId instanceof BatchMessageIdImpl) {
                messageId = new MessageIdImpl(messageId.getLedgerId(), messageId.getEntryId(),
                        getPartitionIndex());
            }
            // TODO: 11/6/23 从死信消息容器 中获取 对应的消息 
            deadLetterMessages = possibleSendToDeadLetterTopicMessages.get(messageId);
        }
        CompletableFuture<Boolean> result = new CompletableFuture<>();
        // TODO: 11/6/23 在死信消息容器 中找到消息 
        if (deadLetterMessages != null) {
            // TODO: 11/6/23 初始化producer
            initDeadLetterProducerIfNeeded();
            List<MessageImpl<T>> finalDeadLetterMessages = deadLetterMessages;
            MessageIdImpl finalMessageId = messageId;
            deadLetterProducer.thenAcceptAsync(producerDLQ -> {
                for (MessageImpl<T> message : finalDeadLetterMessages) {
                    // TODO: 11/6/23 msgId
                    String originMessageIdStr = getOriginMessageIdStr(message);
                    // TODO: 11/6/23 topic name
                    String originTopicNameStr = getOriginTopicNameStr(message);
                    producerDLQ.newMessage(Schema.AUTO_PRODUCE_BYTES(message.getReaderSchema().get()))
                            .value(message.getData())
                            .properties(getPropertiesMap(message, originMessageIdStr, originTopicNameStr))
                            .sendAsync()// 发送数据到死信topic
                            .thenAccept(messageIdInDLQ -> {
                                // TODO: 11/6/23 发送成功，从 死信消息容器 中移除 msgID
                                possibleSendToDeadLetterTopicMessages.remove(finalMessageId);
                                // TODO: 11/6/23 向broker端发送ACK，即该消息不会再次发送到客户端
                                acknowledgeAsync(finalMessageId).whenComplete((v, ex) -> {
                                    if (ex != null) {
                                        log.warn("[{}] [{}] [{}] Failed to acknowledge the message {} of the original"
                                                        + " topic but send to the DLQ successfully.",
                                                topicName, subscription, consumerName, finalMessageId, ex);
                                    } else {
                                        result.complete(true);
                                    }
                                });
                            }).exceptionally(ex -> {
                                log.warn("[{}] [{}] [{}] Failed to send DLQ message to {} for message id {}",
                                        topicName, subscription, consumerName, finalMessageId, ex);
                                result.complete(false);
                                return null;
                    });
                }
            }).exceptionally(ex -> {
                log.error("Dead letter producer exception with topic: {}", deadLetterPolicy.getDeadLetterTopic(), ex);
                deadLetterProducer = null;
                result.complete(false);
                return null;
            });
        } else {
            result.complete(false);
        }
        return result;
    }

    private void initDeadLetterProducerIfNeeded() {
        if (deadLetterProducer == null) {
            createProducerLock.writeLock().lock();
            try {
                // TODO: 11/6/23 初始化死信topic的producer，前面加锁，避免多线程同时创建多个producer
                if (deadLetterProducer == null) {
                    deadLetterProducer =
                            ((ProducerBuilderImpl<byte[]>) client.newProducer(Schema.AUTO_PRODUCE_BYTES(schema)))
                                    .initialSubscriptionName(this.deadLetterPolicy.getInitialSubscriptionName())
                                    .topic(this.deadLetterPolicy.getDeadLetterTopic())
                                    .blockIfQueueFull(false)
                                    .createAsync();
                }
            } finally {
                createProducerLock.writeLock().unlock();
            }
        }
    }

    @Override
    public void seek(MessageId messageId) throws PulsarClientException {
        try {
            seekAsync(messageId).get();
        } catch (Exception e) {
            throw PulsarClientException.unwrap(e);
        }
    }

    @Override
    public void seek(long timestamp) throws PulsarClientException {
        try {
            seekAsync(timestamp).get();
        } catch (Exception e) {
            throw PulsarClientException.unwrap(e);
        }
    }

    @Override
    public void seek(Function<String, Object> function) throws PulsarClientException {
        try {
            seekAsync(function).get();
        } catch (Exception e) {
            throw PulsarClientException.unwrap(e);
        }
    }

    @Override
    public CompletableFuture<Void> seekAsync(Function<String, Object> function) {
        if (function == null) {
            return FutureUtil.failedFuture(new PulsarClientException("Function must be set"));
        }
        Object seekPosition = function.apply(topic);
        if (seekPosition == null) {
            return CompletableFuture.completedFuture(null);
        }
        if (seekPosition instanceof MessageId) {
            return seekAsync((MessageId) seekPosition);
        } else if (seekPosition.getClass().getTypeName()
                .equals(Long.class.getTypeName())) {
            return seekAsync((long) seekPosition);
        }
        return FutureUtil.failedFuture(
                new PulsarClientException("Only support seek by messageId or timestamp"));
    }

    private Optional<CompletableFuture<Void>> seekAsyncCheckState(String seekBy) {
        if (getState() == State.Closing || getState() == State.Closed) {
            return Optional.of(FutureUtil
                    .failedFuture(new PulsarClientException.AlreadyClosedException(
                            String.format("The consumer %s was already closed when seeking the subscription %s of the"
                                    + " topic %s to %s", consumerName, subscription, topicName.toString(), seekBy))));
        }

        if (!isConnected()) {
            return Optional.of(FutureUtil.failedFuture(new PulsarClientException(
                    String.format("The client is not connected to the broker when seeking the subscription %s of the "
                            + "topic %s to %s", subscription, topicName.toString(), seekBy))));
        }

        return Optional.empty();
    }

    private CompletableFuture<Void> seekAsyncInternal(long requestId, ByteBuf seek, MessageId seekId, String seekBy) {
        final CompletableFuture<Void> seekFuture = new CompletableFuture<>();
        ClientCnx cnx = cnx();

        BatchMessageIdImpl originSeekMessageId = seekMessageId;
        seekMessageId = new BatchMessageIdImpl((MessageIdImpl) seekId);
        duringSeek.set(true);
        log.info("[{}][{}] Seeking subscription to {}", topic, subscription, seekBy);

        cnx.sendRequestWithId(seek, requestId).thenRun(() -> {
            log.info("[{}][{}] Successfully reset subscription to {}", topic, subscription, seekBy);
            acknowledgmentsGroupingTracker.flushAndClean();

            lastDequeuedMessageId = MessageId.earliest;

            clearIncomingMessages();
            seekFuture.complete(null);
        }).exceptionally(e -> {
            // re-set duringSeek and seekMessageId if seek failed
            seekMessageId = originSeekMessageId;
            duringSeek.set(false);
            log.error("[{}][{}] Failed to reset subscription: {}", topic, subscription, e.getCause().getMessage());

            seekFuture.completeExceptionally(
                PulsarClientException.wrap(e.getCause(),
                    String.format("Failed to seek the subscription %s of the topic %s to %s",
                        subscription, topicName.toString(), seekBy)));
            return null;
        });
        return seekFuture;
    }

    @Override
    public CompletableFuture<Void> seekAsync(long timestamp) {
        String seekBy = String.format("the timestamp %d", timestamp);
        return seekAsyncCheckState(seekBy).orElseGet(() -> {
            long requestId = client.newRequestId();
            return seekAsyncInternal(requestId, Commands.newSeek(consumerId, requestId, timestamp),
                MessageId.earliest, seekBy);
        });
    }

    @Override
    public CompletableFuture<Void> seekAsync(MessageId messageId) {
        String seekBy = String.format("the message %s", messageId.toString());
        return seekAsyncCheckState(seekBy).orElseGet(() -> {
            long requestId = client.newRequestId();
            ByteBuf seek = null;
            if (messageId instanceof BatchMessageIdImpl) {
                BatchMessageIdImpl msgId = (BatchMessageIdImpl) messageId;
                // Initialize ack set
                BitSetRecyclable ackSet = BitSetRecyclable.create();
                ackSet.set(0, msgId.getBatchSize());
                ackSet.clear(0, Math.max(msgId.getBatchIndex(), 0));
                long[] ackSetArr = ackSet.toLongArray();
                ackSet.recycle();

                seek = Commands.newSeek(consumerId, requestId, msgId.getLedgerId(), msgId.getEntryId(), ackSetArr);
            } else if (messageId instanceof ChunkMessageIdImpl) {
                ChunkMessageIdImpl msgId = (ChunkMessageIdImpl) messageId;
                seek = Commands.newSeek(consumerId, requestId, msgId.getFirstChunkMessageId().getLedgerId(),
                        msgId.getFirstChunkMessageId().getEntryId(), new long[0]);
            } else {
                MessageIdImpl msgId = (MessageIdImpl) messageId;
                seek = Commands.newSeek(consumerId, requestId, msgId.getLedgerId(), msgId.getEntryId(), new long[0]);
            }
            return seekAsyncInternal(requestId, seek, messageId, seekBy);
        });
    }

    public boolean hasMessageAvailable() throws PulsarClientException {
        try {
            return hasMessageAvailableAsync().get();
        } catch (Exception e) {
            throw PulsarClientException.unwrap(e);
        }
    }

    public CompletableFuture<Boolean> hasMessageAvailableAsync() {
        final CompletableFuture<Boolean> booleanFuture = new CompletableFuture<>();

        // we haven't read yet. use startMessageId for comparison
        if (lastDequeuedMessageId == MessageId.earliest) {
            // if we are starting from latest, we should seek to the actual last message first.
            // allow the last one to be read when read head inclusively.
            if (MessageId.latest.equals(startMessageId)) {

                CompletableFuture<GetLastMessageIdResponse> future = internalGetLastMessageIdAsync();
                // if the consumer is configured to read inclusive then we need to seek to the last message
                if (resetIncludeHead) {
                    future = future.thenCompose((lastMessageIdResponse) ->
                            seekAsync(lastMessageIdResponse.lastMessageId)
                                    .thenApply((ignore) -> lastMessageIdResponse));
                }

                future.thenAccept(response -> {
                    MessageIdImpl lastMessageId = MessageIdImpl.convertToMessageIdImpl(response.lastMessageId);
                    MessageIdImpl markDeletePosition = MessageIdImpl
                            .convertToMessageIdImpl(response.markDeletePosition);

                    if (markDeletePosition != null && !(markDeletePosition.getEntryId() < 0
                            && markDeletePosition.getLedgerId() > lastMessageId.getLedgerId())) {
                        // we only care about comparing ledger ids and entry ids as mark delete position doesn't have
                        // other ids such as batch index
                        int result = ComparisonChain.start()
                                .compare(markDeletePosition.getLedgerId(), lastMessageId.getLedgerId())
                                .compare(markDeletePosition.getEntryId(), lastMessageId.getEntryId())
                                .result();
                        if (lastMessageId.getEntryId() < 0) {
                            completehasMessageAvailableWithValue(booleanFuture, false);
                        } else {
                            completehasMessageAvailableWithValue(booleanFuture,
                                    resetIncludeHead ? result <= 0 : result < 0);
                        }
                    } else if (lastMessageId == null || lastMessageId.getEntryId() < 0) {
                        completehasMessageAvailableWithValue(booleanFuture, false);
                    } else {
                        completehasMessageAvailableWithValue(booleanFuture, resetIncludeHead);
                    }
                }).exceptionally(ex -> {
                    log.error("[{}][{}] Failed getLastMessageId command", topic, subscription, ex);
                    booleanFuture.completeExceptionally(ex.getCause());
                    return null;
                });

                return booleanFuture;
            }

            if (hasMoreMessages(lastMessageIdInBroker, startMessageId, resetIncludeHead)) {
                completehasMessageAvailableWithValue(booleanFuture, true);
                return booleanFuture;
            }

            getLastMessageIdAsync().thenAccept(messageId -> {
                lastMessageIdInBroker = messageId;
                if (hasMoreMessages(lastMessageIdInBroker, startMessageId, resetIncludeHead)) {
                    completehasMessageAvailableWithValue(booleanFuture, true);
                } else {
                    completehasMessageAvailableWithValue(booleanFuture, false);
                }
            }).exceptionally(e -> {
                log.error("[{}][{}] Failed getLastMessageId command", topic, subscription);
                booleanFuture.completeExceptionally(e.getCause());
                return null;
            });

        } else {
            // read before, use lastDequeueMessage for comparison
            if (hasMoreMessages(lastMessageIdInBroker, lastDequeuedMessageId, false)) {
                completehasMessageAvailableWithValue(booleanFuture, true);
                return booleanFuture;
            }

            getLastMessageIdAsync().thenAccept(messageId -> {
                lastMessageIdInBroker = messageId;
                if (hasMoreMessages(lastMessageIdInBroker, lastDequeuedMessageId, false)) {
                    completehasMessageAvailableWithValue(booleanFuture, true);
                } else {
                    completehasMessageAvailableWithValue(booleanFuture, false);
                }
            }).exceptionally(e -> {
                log.error("[{}][{}] Failed getLastMessageId command", topic, subscription);
                booleanFuture.completeExceptionally(e.getCause());
                return null;
            });
        }

        return booleanFuture;
    }

    private void completehasMessageAvailableWithValue(CompletableFuture<Boolean> future, boolean value) {
        internalPinnedExecutor.execute(() -> {
            future.complete(value);
        });
    }

    private boolean hasMoreMessages(MessageId lastMessageIdInBroker, MessageId messageId, boolean inclusive) {
        if (inclusive && lastMessageIdInBroker.compareTo(messageId) >= 0
                && ((MessageIdImpl) lastMessageIdInBroker).getEntryId() != -1) {
            return true;
        }

        if (!inclusive && lastMessageIdInBroker.compareTo(messageId) > 0
                && ((MessageIdImpl) lastMessageIdInBroker).getEntryId() != -1) {
            return true;
        }

        return false;
    }

    private static final class GetLastMessageIdResponse {
        final MessageId lastMessageId;
        final MessageId markDeletePosition;

        GetLastMessageIdResponse(MessageId lastMessageId, MessageId markDeletePosition) {
            this.lastMessageId = lastMessageId;
            this.markDeletePosition = markDeletePosition;
        }
    }

    @Override
    public CompletableFuture<MessageId> getLastMessageIdAsync() {
        return internalGetLastMessageIdAsync().thenApply(r -> r.lastMessageId);
    }

    public CompletableFuture<GetLastMessageIdResponse> internalGetLastMessageIdAsync() {
        if (getState() == State.Closing || getState() == State.Closed) {
            return FutureUtil
                .failedFuture(new PulsarClientException.AlreadyClosedException(
                    String.format("The consumer %s was already closed when the subscription %s of the topic %s "
                            + "getting the last message id", consumerName, subscription, topicName.toString())));
                }

        AtomicLong opTimeoutMs = new AtomicLong(client.getConfiguration().getOperationTimeoutMs());
        Backoff backoff = new BackoffBuilder()
                .setInitialTime(100, TimeUnit.MILLISECONDS)
                .setMax(opTimeoutMs.get() * 2, TimeUnit.MILLISECONDS)
                .setMandatoryStop(0, TimeUnit.MILLISECONDS)
                .create();

        CompletableFuture<GetLastMessageIdResponse> getLastMessageIdFuture = new CompletableFuture<>();

        internalGetLastMessageIdAsync(backoff, opTimeoutMs, getLastMessageIdFuture);
        return getLastMessageIdFuture;
    }

    private void internalGetLastMessageIdAsync(final Backoff backoff,
                                               final AtomicLong remainingTime,
                                               CompletableFuture<GetLastMessageIdResponse> future) {
        ClientCnx cnx = cnx();
        if (isConnected() && cnx != null) {
            if (!Commands.peerSupportsGetLastMessageId(cnx.getRemoteEndpointProtocolVersion())) {
                future.completeExceptionally(
                    new PulsarClientException.NotSupportedException(
                        String.format("The command `GetLastMessageId` is not supported for the protocol version %d. "
                                        + "The consumer is %s, topic %s, subscription %s",
                                cnx.getRemoteEndpointProtocolVersion(),
                                consumerName, topicName.toString(), subscription)));
                return;
            }

            long requestId = client.newRequestId();
            ByteBuf getLastIdCmd = Commands.newGetLastMessageId(consumerId, requestId);
            log.info("[{}][{}] Get topic last message Id", topic, subscription);

            cnx.sendGetLastMessageId(getLastIdCmd, requestId).thenAccept(cmd -> {
                MessageIdData lastMessageId = cmd.getLastMessageId();
                MessageIdImpl markDeletePosition = null;
                if (cmd.hasConsumerMarkDeletePosition()) {
                    markDeletePosition = new MessageIdImpl(cmd.getConsumerMarkDeletePosition().getLedgerId(),
                            cmd.getConsumerMarkDeletePosition().getEntryId(), -1);
                }
                log.info("[{}][{}] Successfully getLastMessageId {}:{}",
                    topic, subscription, lastMessageId.getLedgerId(), lastMessageId.getEntryId());

                MessageId lastMsgId = lastMessageId.getBatchIndex() <= 0
                        ? new MessageIdImpl(lastMessageId.getLedgerId(),
                                lastMessageId.getEntryId(), lastMessageId.getPartition())
                        : new BatchMessageIdImpl(lastMessageId.getLedgerId(), lastMessageId.getEntryId(),
                                lastMessageId.getPartition(), lastMessageId.getBatchIndex());

                future.complete(new GetLastMessageIdResponse(lastMsgId, markDeletePosition));
            }).exceptionally(e -> {
                log.error("[{}][{}] Failed getLastMessageId command", topic, subscription);
                future.completeExceptionally(
                    PulsarClientException.wrap(e.getCause(),
                        String.format("The subscription %s of the topic %s gets the last message id was failed",
                            subscription, topicName.toString())));
                return null;
            });
        } else {
            long nextDelay = Math.min(backoff.next(), remainingTime.get());
            if (nextDelay <= 0) {
                future.completeExceptionally(
                    new PulsarClientException.TimeoutException(
                        String.format("The subscription %s of the topic %s could not get the last message id "
                                + "withing configured timeout", subscription, topicName.toString())));
                return;
            }

            internalPinnedExecutor.schedule(() -> {
                log.warn("[{}] [{}] Could not get connection while getLastMessageId -- Will try again in {} ms",
                        topic, getHandlerName(), nextDelay);
                remainingTime.addAndGet(-nextDelay);
                internalGetLastMessageIdAsync(backoff, remainingTime, future);
            }, nextDelay, TimeUnit.MILLISECONDS);
        }
    }

    private MessageIdImpl getMessageIdImpl(Message<?> msg) {
        MessageIdImpl messageId = (MessageIdImpl) msg.getMessageId();
        if (messageId instanceof BatchMessageIdImpl) {
            // messageIds contain MessageIdImpl, not BatchMessageIdImpl
            messageId = new MessageIdImpl(messageId.getLedgerId(), messageId.getEntryId(), getPartitionIndex());
        }
        return messageId;
    }


    private boolean isMessageUndecryptable(MessageMetadata msgMetadata) {
        return (msgMetadata.getEncryptionKeysCount() > 0 && conf.getCryptoKeyReader() == null
                && conf.getCryptoFailureAction() == ConsumerCryptoFailureAction.CONSUME);
    }

    /**
     * Create EncryptionContext if message payload is encrypted.
     *
     * @param msgMetadata
     * @return {@link Optional}<{@link EncryptionContext}>
     */
    private Optional<EncryptionContext> createEncryptionContext(MessageMetadata msgMetadata) {

        EncryptionContext encryptionCtx = null;
        if (msgMetadata.getEncryptionKeysCount() > 0) {
            encryptionCtx = new EncryptionContext();
            Map<String, EncryptionKey> keys = msgMetadata.getEncryptionKeysList().stream()
                    .collect(
                            Collectors.toMap(EncryptionKeys::getKey,
                                    e -> new EncryptionKey(e.getValue(),
                                            e.getMetadatasList().stream().collect(
                                                    Collectors.toMap(KeyValue::getKey, KeyValue::getValue)))));
            byte[] encParam = msgMetadata.getEncryptionParam();
            Optional<Integer> batchSize = Optional
                    .ofNullable(msgMetadata.hasNumMessagesInBatch() ? msgMetadata.getNumMessagesInBatch() : null);
            encryptionCtx.setKeys(keys);
            encryptionCtx.setParam(encParam);
            if (msgMetadata.hasEncryptionAlgo()) {
                encryptionCtx.setAlgorithm(msgMetadata.getEncryptionAlgo());
            }
            encryptionCtx
                    .setCompressionType(CompressionCodecProvider.convertFromWireProtocol(msgMetadata.getCompression()));
            encryptionCtx.setUncompressedMessageSize(msgMetadata.getUncompressedSize());
            encryptionCtx.setBatchSize(batchSize);
        }
        return Optional.ofNullable(encryptionCtx);
    }

    private int removeExpiredMessagesFromQueue(Set<MessageId> messageIds) {
        int messagesFromQueue = 0;
        Message<T> peek = incomingMessages.peek();
        if (peek != null) {
            MessageIdImpl messageId = getMessageIdImpl(peek);
            if (!messageIds.contains(messageId)) {
                // first message is not expired, then no message is expired in queue.
                return 0;
            }

            // try not to remove elements that are added while we remove
            Message<T> message = incomingMessages.poll();
            while (message != null) {
                decreaseIncomingMessageSize(message);
                messagesFromQueue++;
                MessageIdImpl id = getMessageIdImpl(message);
                if (!messageIds.contains(id)) {
                    messageIds.add(id);
                    break;
                }
                message.release();
                message = incomingMessages.poll();
            }
        }
        return messagesFromQueue;
    }

    @Override
    public ConsumerStatsRecorder getStats() {
        return stats;
    }

    void setTerminated() {
        log.info("[{}] [{}] [{}] Consumer has reached the end of topic", subscription, topic, consumerName);
        hasReachedEndOfTopic = true;
        if (listener != null) {
            // Propagate notification to listener
            listener.reachedEndOfTopic(this);
        }
    }

    @Override
    public boolean hasReachedEndOfTopic() {
        return hasReachedEndOfTopic;
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, subscription, consumerName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConsumerImpl)) {
            return false;
        }
        ConsumerImpl<?> consumer = (ConsumerImpl<?>) o;
        return consumerId == consumer.consumerId;
    }

    // wrapper for connection methods
    ClientCnx cnx() {
        return this.connectionHandler.cnx();
    }

    void resetBackoff() {
        this.connectionHandler.resetBackoff();
    }

    void connectionClosed(ClientCnx cnx) {
        this.connectionHandler.connectionClosed(cnx);
    }

    public ClientCnx getClientCnx() {
        return this.connectionHandler.cnx();
    }

    void setClientCnx(ClientCnx clientCnx) {
        if (clientCnx != null) {
            this.connectionHandler.setClientCnx(clientCnx);
            clientCnx.registerConsumer(consumerId, this);
            if (conf.isAckReceiptEnabled()
                    && !Commands.peerSupportsAckReceipt(clientCnx.getRemoteEndpointProtocolVersion())) {
                log.warn("Server don't support ack for receipt! "
                        + "ProtoVersion >=17 support! nowVersion : {}", clientCnx.getRemoteEndpointProtocolVersion());
            }
        }
        ClientCnx previousClientCnx = clientCnxUsedForConsumerRegistration.getAndSet(clientCnx);
        if (previousClientCnx != null && previousClientCnx != clientCnx) {
            previousClientCnx.removeConsumer(consumerId);
        }
    }

    void deregisterFromClientCnx() {
        setClientCnx(null);
    }

    void reconnectLater(Throwable exception) {
        this.connectionHandler.reconnectLater(exception);
    }

    void grabCnx() {
        // TODO: 11/2/23 调用 connectionHandler 里面的grabCnx方法，和broker建立连接,
        //  这里和producer侧于broker建立连接相同
        this.connectionHandler.grabCnx();
    }

    public String getTopicNameWithoutPartition() {
        return topicNameWithoutPartition;
    }

    static class ChunkedMessageCtx {

        protected int totalChunks = -1;
        protected ByteBuf chunkedMsgBuffer;
        protected int lastChunkedMessageId = -1;
        protected MessageIdImpl[] chunkedMessageIds;
        protected long receivedTime = 0;

        static ChunkedMessageCtx get(int numChunksFromMsg, ByteBuf chunkedMsgBuffer) {
            ChunkedMessageCtx ctx = RECYCLER.get();
            ctx.totalChunks = numChunksFromMsg;
            ctx.chunkedMsgBuffer = chunkedMsgBuffer;
            ctx.chunkedMessageIds = new MessageIdImpl[numChunksFromMsg];
            ctx.receivedTime = System.currentTimeMillis();
            return ctx;
        }

        private final Handle<ChunkedMessageCtx> recyclerHandle;

        private ChunkedMessageCtx(Handle<ChunkedMessageCtx> recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
        }

        private static final Recycler<ChunkedMessageCtx> RECYCLER = new Recycler<ChunkedMessageCtx>() {
            protected ChunkedMessageCtx newObject(Recycler.Handle<ChunkedMessageCtx> handle) {
                return new ChunkedMessageCtx(handle);
            }
        };

        public void recycle() {
            this.totalChunks = -1;
            this.chunkedMsgBuffer = null;
            this.lastChunkedMessageId = -1;
            recyclerHandle.recycle(this);
        }
    }

    private void removeOldestPendingChunkedMessage() {
        ChunkedMessageCtx chunkedMsgCtx = null;
        String firstPendingMsgUuid = null;
        while (chunkedMsgCtx == null && !pendingChunkedMessageUuidQueue.isEmpty()) {
            // TODO: 11/6/23 获取并移除头部
            // remove oldest pending chunked-message group and free memory
            firstPendingMsgUuid = pendingChunkedMessageUuidQueue.poll();
            chunkedMsgCtx = StringUtils.isNotBlank(firstPendingMsgUuid) ? chunkedMessagesMap.get(firstPendingMsgUuid)
                    : null;
        }
        // TODO: 11/6/23 移除头部消息块 
        removeChunkMessage(firstPendingMsgUuid, chunkedMsgCtx, this.autoAckOldestChunkedMessageOnQueueFull);
    }

    // TODO: 11/6/23 移除过期没有完成从批量消息
    protected void removeExpireIncompleteChunkedMessages() {
        if (expireTimeOfIncompleteChunkedMessageMillis <= 0) {
            return;
        }
        ChunkedMessageCtx chunkedMsgCtx = null;
        String messageUUID;
        while ((messageUUID = pendingChunkedMessageUuidQueue.peek()) != null) {
            chunkedMsgCtx = StringUtils.isNotBlank(messageUUID) ? chunkedMessagesMap.get(messageUUID) : null;
            if (chunkedMsgCtx != null && System
                    .currentTimeMillis() > (chunkedMsgCtx.receivedTime + expireTimeOfIncompleteChunkedMessageMillis)) {
                // TODO: 11/6/23  从 pendingChunkedMessageUuidQueue里面移除过期的消息UUID
                pendingChunkedMessageUuidQueue.remove(messageUUID);
                removeChunkMessage(messageUUID, chunkedMsgCtx, true);
            } else {
                return;
            }
        }
    }

    private void removeChunkMessage(String msgUUID, ChunkedMessageCtx chunkedMsgCtx, boolean autoAck) {
        if (chunkedMsgCtx == null) {
            return;
        }
        // TODO: 11/6/23 移除 chunkedMessagesMap 里面的msgUUID
        // clean up pending chuncked-Message
        chunkedMessagesMap.remove(msgUUID);
        if (chunkedMsgCtx.chunkedMessageIds != null) {
            for (MessageIdImpl msgId : chunkedMsgCtx.chunkedMessageIds) {
                if (msgId == null) {
                    continue;
                }
                if (autoAck) {
                    log.info("Removing chunk message-id {}", msgId);
                    doAcknowledge(msgId, AckType.Individual, Collections.emptyMap(), null);
                } else {
                    trackMessage(msgId);
                }
            }
        }
        if (chunkedMsgCtx.chunkedMsgBuffer != null) {
            chunkedMsgCtx.chunkedMsgBuffer.release();
        }
        chunkedMsgCtx.recycle();
        pendingChunkedMessageCount--;
    }

    private CompletableFuture<Void> doTransactionAcknowledgeForResponse(MessageId messageId, AckType ackType,
                                                                        ValidationError validationError,
                                                                        Map<String, Long> properties, TxnID txnID) {
        BitSetRecyclable bitSetRecyclable = null;
        long ledgerId;
        long entryId;
        ByteBuf cmd;
        long requestId = client.newRequestId();
        if (messageId instanceof BatchMessageIdImpl) {
            BatchMessageIdImpl batchMessageId = (BatchMessageIdImpl) messageId;
            bitSetRecyclable = BitSetRecyclable.create();
            ledgerId = batchMessageId.getLedgerId();
            entryId = batchMessageId.getEntryId();
            if (ackType == AckType.Cumulative) {
                batchMessageId.ackCumulative();
                bitSetRecyclable.set(0, batchMessageId.getBatchSize());
                bitSetRecyclable.clear(0, batchMessageId.getBatchIndex() + 1);
            } else {
                bitSetRecyclable.set(0, batchMessageId.getBatchSize());
                bitSetRecyclable.clear(batchMessageId.getBatchIndex());
            }
            cmd = Commands.newAck(consumerId, ledgerId, entryId, bitSetRecyclable, ackType, validationError, properties,
                    txnID.getLeastSigBits(), txnID.getMostSigBits(), requestId, batchMessageId.getBatchSize());
            bitSetRecyclable.recycle();
        } else {
            MessageIdImpl singleMessage = (MessageIdImpl) messageId;
            ledgerId = singleMessage.getLedgerId();
            entryId = singleMessage.getEntryId();
            cmd = Commands.newAck(consumerId, ledgerId, entryId, bitSetRecyclable, ackType,
                    validationError, properties, txnID.getLeastSigBits(), txnID.getMostSigBits(), requestId);
        }

        if (ackType == AckType.Cumulative) {
            unAckedMessageTracker.removeMessagesTill(messageId);
        } else {
            unAckedMessageTracker.remove(messageId);
        }
        return cnx().newAckForReceipt(cmd, requestId);
    }

    public Map<MessageIdImpl, List<MessageImpl<T>>> getPossibleSendToDeadLetterTopicMessages() {
        return possibleSendToDeadLetterTopicMessages;
    }

    private static final Logger log = LoggerFactory.getLogger(ConsumerImpl.class);

}
