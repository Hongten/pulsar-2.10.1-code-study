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
import static com.google.common.base.Preconditions.checkState;
import static com.scurrilous.circe.checksum.Crc32cIntChecksum.computeChecksum;
import static com.scurrilous.circe.checksum.Crc32cIntChecksum.resumeChecksum;
import static java.lang.String.format;
import static org.apache.pulsar.client.impl.MessageImpl.SchemaState.Broken;
import static org.apache.pulsar.client.impl.MessageImpl.SchemaState.None;
import static org.apache.pulsar.client.impl.ProducerBase.MultiSchemaMode.Auto;
import static org.apache.pulsar.client.impl.ProducerBase.MultiSchemaMode.Enabled;
import static org.apache.pulsar.common.protocol.Commands.hasChecksum;
import static org.apache.pulsar.common.protocol.Commands.readChecksum;
import static org.apache.pulsar.common.util.Runnables.catchingAndLoggingThrowables;
import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import io.netty.util.concurrent.ScheduledFuture;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.Consumer;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.BatcherBuilder;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageCrypto;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.ProducerCryptoFailureAction;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientException.CryptoException;
import org.apache.pulsar.client.api.PulsarClientException.TimeoutException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.client.impl.crypto.MessageCryptoBc;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.client.impl.transaction.TransactionImpl;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.api.proto.ProtocolVersion;
import org.apache.pulsar.common.compression.CompressionCodec;
import org.apache.pulsar.common.compression.CompressionCodecProvider;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.protocol.ByteBufPair;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Commands.ChecksumType;
import org.apache.pulsar.common.protocol.schema.SchemaHash;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.common.util.DateFormatter;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.RelativeTimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerImpl<T> extends ProducerBase<T> implements TimerTask, ConnectionHandler.Connection {

    // TODO: 10/18/23  生产者ID，用于标识单个连接
    // Producer id, used to identify a producer within a single connection
    protected final long producerId;

    // TODO: 10/18/23 消息ID生成器 
    // Variable is used through the atomic updater
    private volatile long msgIdGenerator;

    // TODO: 10/18/23 待处理消息队列
    private final OpSendMsgQueue pendingMessages;
    // TODO: 10/18/23 信号量，用于控制发送消息频率
    private final Optional<Semaphore> semaphore;
    // TODO: 10/18/23 发送超时定时器
    private volatile Timeout sendTimeout = null;
    private final long lookupDeadline;

    @SuppressWarnings("rawtypes")
    private static final AtomicLongFieldUpdater<ProducerImpl> PRODUCER_DEADLINE_UPDATER = AtomicLongFieldUpdater
            .newUpdater(ProducerImpl.class, "producerDeadline");
    @SuppressWarnings("unused")
    private volatile long producerDeadline = 0; // gets set on first successful connection

    // TODO: 10/18/23 批量消息容器
    private final BatchMessageContainerBase batchMessageContainer;
    // TODO: 10/18/23 最新消息发送（状态）标记
    private CompletableFuture<MessageId> lastSendFuture = CompletableFuture.completedFuture(null);
    private LastSendFutureWrapper lastSendFutureWrapper = LastSendFutureWrapper.create(lastSendFuture);

    // TODO: 10/18/23 全局唯一生产者名称
    // Globally unique producer name
    private String producerName;
    private final boolean userProvidedProducerName;

    // TODO: 10/18/23 连接ID
    private String connectionId;
    private String connectedSince;
    // TODO: 10/18/23 分区索引（ID）
    private final int partitionIndex;
    // TODO: 10/18/23 生产者状态记录器 

    private final ProducerStatsRecorder stats;

    // TODO: 10/18/23 消息压缩类型
    private final CompressionCodec compressor;

    static final AtomicLongFieldUpdater<ProducerImpl> LAST_SEQ_ID_PUBLISHED_UPDATER = AtomicLongFieldUpdater
            .newUpdater(ProducerImpl.class, "lastSequenceIdPublished");
    // TODO: 10/18/23 最后一个发送消息ID
    private volatile long lastSequenceIdPublished;

    static final AtomicLongFieldUpdater<ProducerImpl> LAST_SEQ_ID_PUSHED_UPDATER = AtomicLongFieldUpdater
            .newUpdater(ProducerImpl.class, "lastSequenceIdPushed");
    protected volatile long lastSequenceIdPushed;
    private volatile boolean isLastSequenceIdPotentialDuplicated;

    // TODO: 10/18/23 消息加密器
    private final MessageCrypto msgCrypto;

    // TODO: 10/18/23 KEY生成任务
    private ScheduledFuture<?> keyGeneratorTask = null;

    // TODO: 10/18/23 元数据容器
    private final Map<String, String> metadata;

    private Optional<byte[]> schemaVersion = Optional.empty();

    // TODO: 10/18/23 连接句柄
    private final ConnectionHandler connectionHandler;

    private ScheduledFuture<?> batchTimerTask;

    private Optional<Long> topicEpoch = Optional.empty();
    private final List<Throwable> previousExceptions = new CopyOnWriteArrayList<Throwable>();

    private boolean errorState;

    // TODO: 10/18/23 原子字段更新器，消息ID
    @SuppressWarnings("rawtypes")
    private static final AtomicLongFieldUpdater<ProducerImpl> msgIdGeneratorUpdater = AtomicLongFieldUpdater
            .newUpdater(ProducerImpl.class, "msgIdGenerator");

    // TODO: 2/24/23 创建 ProducerImpl 实例
    public ProducerImpl(PulsarClientImpl client, String topic, ProducerConfigurationData conf,
                        CompletableFuture<Producer<T>> producerCreatedFuture, int partitionIndex, Schema<T> schema,
                        ProducerInterceptors interceptors, Optional<String> overrideProducerName) {
        // TODO: 10/18/23 这里producerCreatedFuture，用于实现异步创建
        super(client, topic, conf, producerCreatedFuture, schema, interceptors);
        // TODO: 2/24/23 创建producerId
        this.producerId = client.newProducerId();
        // TODO: 2/24/23 producer Name 默认为null，用户提供了以后，会被覆盖
        this.producerName = conf.getProducerName();
        this.userProvidedProducerName = StringUtils.isNotBlank(producerName);
        // TODO: 2/24/23 partition的序号 ，如果是一个partition的topic，index=-1
        this.partitionIndex = partitionIndex;
        // TODO: 2/24/23  OpSendMsgQueue
        this.pendingMessages = createPendingMessagesQueue();
        if (conf.getMaxPendingMessages() > 0) {
            this.semaphore = Optional.of(new Semaphore(conf.getMaxPendingMessages(), true));
        } else {
            this.semaphore = Optional.empty();
        }
        overrideProducerName.ifPresent(key -> this.producerName = key);

        // TODO: 10/18/23 默认压缩类型为NONE 
        this.compressor = CompressionCodecProvider.getCompressionCodec(conf.getCompressionType());

        // TODO: 2/24/23 初始化 sequenceId，如果Producer 有配置 initialSequenceId，
        //  则将 lastSequenceIdPublished 和 lastSequenceIdPushed 都置为 initialSequenceId ，
        //  将msgIdGenerator 置为 initialSequenceId+1 ；
        //  如果没有配置，则 lastSequenceIdPublished 和 lastSequenceIdPushed 都置为-1，msgIdGenerator 置为0；
        //  lastSequenceIdPushed 表示已经 send 到 broker 的消息的 sequenceId，
        //  lastSequenceIdPublished 表示已经 publish 成功的 message 的 sequenceId
        if (conf.getInitialSequenceId() != null) {
            long initialSequenceId = conf.getInitialSequenceId();
            this.lastSequenceIdPublished = initialSequenceId;
            this.lastSequenceIdPushed = initialSequenceId;
            this.msgIdGenerator = initialSequenceId + 1L;
        } else {
            this.lastSequenceIdPublished = -1L;
            this.lastSequenceIdPushed = -1L;
            this.msgIdGenerator = 0L;
        }

        // TODO: 10/18/23 消息加密
        if (conf.isEncryptionEnabled()) {
            String logCtx = "[" + topic + "] [" + producerName + "] [" + producerId + "]";

            if (conf.getMessageCrypto() != null) {
                this.msgCrypto = conf.getMessageCrypto();
            } else {
                // default to use MessageCryptoBc;
                MessageCrypto msgCryptoBc;
                try {
                    msgCryptoBc = new MessageCryptoBc(logCtx, true);
                } catch (Exception e) {
                    log.error("MessageCryptoBc may not included in the jar in Producer. e:", e);
                    msgCryptoBc = null;
                }
                this.msgCrypto = msgCryptoBc;
            }
        } else {
            this.msgCrypto = null;
        }

        // TODO: 10/18/23 如果开启消息加密，则加载加密方法和密钥加载器
        if (this.msgCrypto != null) {
            // Regenerate data key cipher at fixed interval
            keyGeneratorTask = client.eventLoopGroup().scheduleWithFixedDelay(catchingAndLoggingThrowables(() -> {
                try {
                    msgCrypto.addPublicKeyCipher(conf.getEncryptionKeys(), conf.getCryptoKeyReader());
                } catch (CryptoException e) {
                    if (!producerCreatedFuture.isDone()) {
                        log.warn("[{}] [{}] [{}] Failed to add public key cipher.", topic, producerName, producerId);
                        producerCreatedFuture.completeExceptionally(
                                PulsarClientException.wrap(e,
                                        String.format("The producer %s of the topic %s "
                                                        + "adds the public key cipher was failed",
                                                producerName, topic)));
                    }
                }
            }), 0L, 4L, TimeUnit.HOURS);
        }

        // TODO: 2/24/23 默认30s，如果设置发消息超时时间，则初始化消息发送超时任务
        if (conf.getSendTimeoutMs() > 0) {
            sendTimeout = client.timer().newTimeout(this, conf.getSendTimeoutMs(), TimeUnit.MILLISECONDS);
        }

        // TODO: 2/24/23 默认30s
        this.lookupDeadline = System.currentTimeMillis() + client.getConfiguration().getLookupTimeoutMs();
        if (conf.isBatchingEnabled()) {
            BatcherBuilder containerBuilder = conf.getBatcherBuilder();
            if (containerBuilder == null) {
                containerBuilder = BatcherBuilder.DEFAULT;
            }
            this.batchMessageContainer = (BatchMessageContainerBase) containerBuilder.build();
            this.batchMessageContainer.setProducer(this);
        } else {
            this.batchMessageContainer = null;
        }
        // TODO: 2/24/23 多久打印一下数据，比如消费者的消费速度、字节数、消费者总共接收了多少消息等，默认60s
        if (client.getConfiguration().getStatsIntervalSeconds() > 0) {
            stats = new ProducerStatsRecorderImpl(client, conf, this);
        } else {
            stats = ProducerStatsDisabled.INSTANCE;
        }

        if (conf.getProperties().isEmpty()) {
            metadata = Collections.emptyMap();
        } else {

            metadata = Collections.unmodifiableMap(new HashMap<>(conf.getProperties()));
        }

        // TODO: 2/24/23 创建ConnectionHandler ，由于ProducerImpl实现了ConnectionHandler.Connection，
        //  并且继承了HandlerState，因此this本身就有HandlerState，Connection功能
        this.connectionHandler = new ConnectionHandler(this,
            new BackoffBuilder()
                .setInitialTime(client.getConfiguration().getInitialBackoffIntervalNanos(), TimeUnit.NANOSECONDS)
                .setMax(client.getConfiguration().getMaxBackoffIntervalNanos(), TimeUnit.NANOSECONDS)
                .setMandatoryStop(Math.max(100, conf.getSendTimeoutMs() - 100), TimeUnit.MILLISECONDS)
                .create(),
            this);

        // TODO: 2/24/23 然后调用ConnectionHandler.grabCnx 查找topic所在的broker地址，然后建立到broker的连接.
        //  建立好连接后，会向broker端发送PRODUCER请求，broker端接收到请求后，会获取topic信息，并且创建Broker端的producer，
        //  然后把Broker端的producer注册到topic里面，然后返回给客户端PRODUCER_SUCCESS响应。
        //  客户端接收到了PRODUCER_SUCCESS响应后，会做下面事情
        /**
         * - Producer会根据sequenceId来初始化lastSequenceIdPublished以及msgIdGenerator，逻辑是如果msgIdGenerator为0并且producer没有指定InitialSequenceId，则将lastSequenceIdPublished置为sequenceId，msgIdGenerator置为sequenceId+1
         * - 如果开启了Batch，则初始化一个定时任务，根据配置的batchingMaxPublishDelayMicros时间来定时发送消息，发送是会将BatchContainer中的所有message遍历封装到send请求中发送到Broker，请求中会携带第一条message的sequenceId和最后一条mesage的sequenceId
         * - 重新发送pendingMessages中的消息
         */
        grabCnx();
        // TODO: 2/24/23 producer的初始化工作完成
    }

    protected void semaphoreRelease(final int releaseCountRequest) {
        if (semaphore.isPresent()) {
            if (!errorState) {
                final int availableReleasePermits =
                        conf.getMaxPendingMessages() - this.semaphore.get().availablePermits();
                if (availableReleasePermits - releaseCountRequest < 0) {
                    log.error("Semaphore permit release count request greater then availableReleasePermits"
                                    + " : availableReleasePermits={}, releaseCountRequest={}",
                            availableReleasePermits, releaseCountRequest);
                    errorState = true;
                }
            }
            semaphore.get().release(releaseCountRequest);
        }
    }

    protected OpSendMsgQueue createPendingMessagesQueue() {
        return new OpSendMsgQueue();
    }

    public ConnectionHandler getConnectionHandler() {
        return connectionHandler;
    }

    private boolean isBatchMessagingEnabled() {
        return conf.isBatchingEnabled();
    }

    private boolean isMultiSchemaEnabled(boolean autoEnable) {
        if (multiSchemaMode != Auto) {
            return multiSchemaMode == Enabled;
        }
        if (autoEnable) {
            multiSchemaMode = Enabled;
            return true;
        }
        return false;
    }

    @Override
    public long getLastSequenceId() {
        return lastSequenceIdPublished;
    }

    @Override
    CompletableFuture<MessageId> internalSendAsync(Message<?> message) {
        CompletableFuture<MessageId> future = new CompletableFuture<>();

        // TODO: 10/18/23 执行拦截器调用
        MessageImpl<?> interceptorMessage = (MessageImpl) beforeSend(message);
        // Retain the buffer used by interceptors callback to get message. Buffer will release after complete
        // interceptors.
        interceptorMessage.getDataBuffer().retain();
        if (interceptors != null) {
            interceptorMessage.getProperties();
        }
        sendAsync(interceptorMessage, new SendCallback() {
            SendCallback nextCallback = null;
            MessageImpl<?> nextMsg = null;
            long createdAt = System.nanoTime();

            @Override
            public CompletableFuture<MessageId> getFuture() {
                return future;
            }

            @Override
            public SendCallback getNextSendCallback() {
                return nextCallback;
            }

            @Override
            public MessageImpl<?> getNextMessage() {
                return nextMsg;
            }

            // TODO: 10/18/23 发送完成回调函数
            @Override
            public void sendComplete(Exception e) {
                try {
                    // TODO: 10/18/23 异常处理
                    if (e != null) {
                        stats.incrementSendFailed();
                        onSendAcknowledgement(interceptorMessage, null, e);
                        future.completeExceptionally(e);
                    } else {
                        onSendAcknowledgement(interceptorMessage, interceptorMessage.getMessageId(), null);
                        future.complete(interceptorMessage.getMessageId());
                        stats.incrementNumAcksReceived(System.nanoTime() - createdAt);
                    }
                } finally {
                    interceptorMessage.getDataBuffer().release();
                }

                while (nextCallback != null) {
                    SendCallback sendCallback = nextCallback;
                    MessageImpl<?> msg = nextMsg;
                    // Retain the buffer used by interceptors callback to get message. Buffer will release after
                    // complete interceptors.
                    try {
                        msg.getDataBuffer().retain();
                        if (e != null) {
                            stats.incrementSendFailed();
                            onSendAcknowledgement(msg, null, e);
                            sendCallback.getFuture().completeExceptionally(e);
                        } else {
                            // TODO: 10/18/23 触发消息发送成功，broker 应答成功时拦截器调用
                            onSendAcknowledgement(msg, msg.getMessageId(), null);
                            sendCallback.getFuture().complete(msg.getMessageId());
                            stats.incrementNumAcksReceived(System.nanoTime() - createdAt);
                        }
                        nextMsg = nextCallback.getNextMessage();
                        nextCallback = nextCallback.getNextSendCallback();
                    } finally {
                        msg.getDataBuffer().release();
                    }
                }
            }

            @Override
            public void addCallback(MessageImpl<?> msg, SendCallback scb) {
                nextMsg = msg;
                nextCallback = scb;
            }
        });
        return future;
    }

    @Override
    CompletableFuture<MessageId> internalSendWithTxnAsync(Message<?> message, Transaction txn) {
        if (txn == null) {
            return internalSendAsync(message);
        } else {
            CompletableFuture<MessageId> completableFuture = new CompletableFuture<>();
            if (!((TransactionImpl) txn).checkIfOpen(completableFuture)) {
               return completableFuture;
            }
            return ((TransactionImpl) txn).registerProducedTopic(topic)
                        .thenCompose(ignored -> internalSendAsync(message));
        }
    }

    /**
     * Compress the payload if compression is configured.
     * @param payload
     * @return a new payload
     */
    private ByteBuf applyCompression(ByteBuf payload) {
        ByteBuf compressedPayload = compressor.encode(payload);
        // TODO: 10/21/23 释放原消息体 
        payload.release();
        return compressedPayload;
    }

    public void sendAsync(Message<?> message, SendCallback callback) {
        // TODO: 10/18/23 检查是否MessageImpl实例
        checkArgument(message instanceof MessageImpl);

        // TODO: 10/18/23 检查生产者状态是否正常 
        if (!isValidProducerState(callback, message.getSequenceId())) {
            return;
        }

        MessageImpl<?> msg = (MessageImpl<?>) message;
        MessageMetadata msgMetadata = msg.getMessageBuilder();
        ByteBuf payload = msg.getDataBuffer();
        int uncompressedSize = payload.readableBytes();

        // TODO: 10/18/23 如果启用队列满则阻塞标志，则请求信号量（阻塞）
        //  否则，试着请求信号量，如果信号量没有获取成功，则立即返回，并抛异常
        if (!canEnqueueRequest(callback, message.getSequenceId(), uncompressedSize)) {
            return;
        }

        // TODO: 10/21/23 如果启用压缩，则压缩，否则用相同的一块缓冲区
        // If compression is enabled, we are compressing, otherwise it will simply use the same buffer
        ByteBuf compressedPayload = payload;
        boolean compressed = false;
        // TODO: 10/21/23 如果批量消息没有启用，（这里表示没启用批量消息 )
        // Batch will be compressed when closed
        // If a message has a delayed delivery time, we'll always send it individually
        if (!isBatchMessagingEnabled() || msgMetadata.hasDeliverAtTime()) {
            // TODO: 10/21/23 进行压缩，然后释放原消息体
            compressedPayload = applyCompression(payload);
            compressed = true;

            // validate msg-size (For batching this will be check at the batch completion size)
            int compressedSize = compressedPayload.readableBytes();
            // TODO: 10/21/23 如果压缩后的大小都大于了最大的消息size，并且没有配置分块发送 
            if (compressedSize > ClientCnx.getMaxMessageSize() && !this.conf.isChunkingEnabled()) {
                // TODO: 10/21/23 释放压缩消息体 
                compressedPayload.release();
                String compressedStr = (!isBatchMessagingEnabled() && conf.getCompressionType() != CompressionType.NONE)
                                           ? "Compressed"
                                           : "";
                // TODO: 10/21/23 设置回调异常，表示消息已经超大
                PulsarClientException.InvalidMessageException invalidMessageException =
                        new PulsarClientException.InvalidMessageException(
                                format("The producer %s of the topic %s sends a %s message with %d bytes that exceeds"
                                                + " %d bytes",
                        producerName, topic, compressedStr, compressedSize, ClientCnx.getMaxMessageSize()));
                completeCallbackAndReleaseSemaphore(uncompressedSize, callback, invalidMessageException);
                return;
            }
        }

        // TODO: 10/21/23 不能重用相同的消息（TODO）
        if (!msg.isReplicated() && msgMetadata.hasProducerName()) {
            PulsarClientException.InvalidMessageException invalidMessageException =
                new PulsarClientException.InvalidMessageException(
                    format("The producer %s of the topic %s can not reuse the same message", producerName, topic),
                        msg.getSequenceId());
            completeCallbackAndReleaseSemaphore(uncompressedSize, callback, invalidMessageException);
            compressedPayload.release();
            return;
        }

        // TODO: 10/21/23 设置Schema版本 
        if (!populateMessageSchema(msg, callback)) {
            compressedPayload.release();
            return;
        }

        // TODO: 10/21/23 客户端默认开启了batch 发送数据，即假设客户端发送的消息大小为10M，那么会取压缩后的消息大小和传输大小（5M)进行计算
        // TODO: 10/21/23 出需要发送多少块数据 
        // send in chunks
        int totalChunks = canAddToBatch(msg) ? 1
                : Math.max(1, compressedPayload.readableBytes()) / ClientCnx.getMaxMessageSize()
                        + (Math.max(1, compressedPayload.readableBytes()) % ClientCnx.getMaxMessageSize() == 0 ? 0 : 1);
        // chunked message also sent individually so, try to acquire send-permits
        for (int i = 0; i < (totalChunks - 1); i++) {
            if (!canEnqueueRequest(callback, message.getSequenceId(), 0 /* The memory was already reserved */)) {
                client.getMemoryLimitController().releaseMemory(uncompressedSize);
                semaphoreRelease(i + 1);
                return;
            }
        }

        try {
            // TODO: 10/21/23 同步代码块（用对象监视器），确保消息有序发送，这里与后面的消息应答处理相呼应
            synchronized (this) {
                int readStartIndex = 0;
                long sequenceId;
                // TODO: 10/21/23 是否设置了序列ID
                if (!msgMetadata.hasSequenceId()) {
                    // TODO: 10/21/23 没有设置，则生成一个序列ID
                    sequenceId = msgIdGeneratorUpdater.getAndIncrement(this);
                    // TODO: 10/21/23 把生产好的序列ID放回到msgMetadata中，方便后续使用
                    msgMetadata.setSequenceId(sequenceId);
                } else {
                    sequenceId = msgMetadata.getSequenceId();
                }
                // TODO: 10/21/23 是否进行分块发送
                String uuid = totalChunks > 1 ? String.format("%s-%d", producerName, sequenceId) : null;
                ChunkedMessageCtx chunkedMessageCtx = totalChunks > 1 ? ChunkedMessageCtx.get(totalChunks) : null;
                byte[] schemaVersion = totalChunks > 1 && msg.getMessageBuilder().hasSchemaVersion()
                        ? msg.getMessageBuilder().getSchemaVersion() : null;
                byte[] orderingKey = totalChunks > 1 && msg.getMessageBuilder().hasOrderingKey()
                        ? msg.getMessageBuilder().getOrderingKey() : null;
                for (int chunkId = 0; chunkId < totalChunks; chunkId++) {
                    // Need to reset the schemaVersion, because the schemaVersion is based on a ByteBuf object in
                    // `MessageMetadata`, if we want to re-serialize the `SEND` command using a same `MessageMetadata`,
                    // we need to reset the ByteBuf of the schemaVersion in `MessageMetadata`, I think we need to
                    // reset `ByteBuf` objects in `MessageMetadata` after call the method `MessageMetadata#writeTo()`.
                    if (chunkId > 0) {
                        if (schemaVersion != null) {
                            msg.getMessageBuilder().setSchemaVersion(schemaVersion);
                        }
                        if (orderingKey != null) {
                            msg.getMessageBuilder().setOrderingKey(orderingKey);
                        }
                    }
                    // TODO: 10/21/23 序列化和发送消息
                    serializeAndSendMessage(msg, payload, sequenceId, uuid, chunkId, totalChunks,
                            readStartIndex, ClientCnx.getMaxMessageSize(), compressedPayload, compressed,
                            compressedPayload.readableBytes(), uncompressedSize, callback, chunkedMessageCtx);
                    // TODO: 10/21/23 更新read 起始index
                    readStartIndex = ((chunkId + 1) * ClientCnx.getMaxMessageSize());
                }
            }
        } catch (PulsarClientException e) {
            e.setSequenceId(msg.getSequenceId());
            completeCallbackAndReleaseSemaphore(uncompressedSize, callback, e);
        } catch (Throwable t) {
            completeCallbackAndReleaseSemaphore(uncompressedSize, callback,
                    new PulsarClientException(t, msg.getSequenceId()));
        }
    }

    @Override
    public int getNumOfPartitions() {
        return 0;
    }

    private void serializeAndSendMessage(MessageImpl<?> msg,
                                         ByteBuf payload,
                                         long sequenceId,
                                         String uuid,
                                         int chunkId,
                                         int totalChunks,
                                         int readStartIndex,
                                         int chunkMaxSizeInBytes,
                                         ByteBuf compressedPayload,
                                         boolean compressed,
                                         int compressedPayloadSize,
                                         int uncompressedSize,
                                         SendCallback callback,
                                         ChunkedMessageCtx chunkedMessageCtx) throws IOException {
        ByteBuf chunkPayload = compressedPayload;
        MessageMetadata msgMetadata = msg.getMessageBuilder();
        if (totalChunks > 1 && TopicName.get(topic).isPersistent()) {
            // TODO: 10/21/23 如果是块发送，则每次发送5M的数据
            chunkPayload = compressedPayload.slice(readStartIndex,
                    Math.min(chunkMaxSizeInBytes, chunkPayload.readableBytes() - readStartIndex));
            // don't retain last chunk payload and builder as it will be not needed for next chunk-iteration and it will
            // be released once this chunk-message is sent
            if (chunkId != totalChunks - 1) {
                chunkPayload.retain();
            }
            if (uuid != null) {
                msgMetadata.setUuid(uuid);
            }
            msgMetadata.setChunkId(chunkId)
                .setNumChunksFromMsg(totalChunks)
                .setTotalChunkMsgSize(compressedPayloadSize);
        }
        // TODO: 10/21/23 如果没有设置发布时间
        if (!msgMetadata.hasPublishTime()) {
            msgMetadata.setPublishTime(client.getClientClock().millis());

            checkArgument(!msgMetadata.hasProducerName());

            msgMetadata.setProducerName(producerName);

            // TODO: 10/21/23 设置压缩类型
            if (conf.getCompressionType() != CompressionType.NONE) {
                // TODO: 10/21/23 把客户端压缩类型转换为服务端的压缩类型
                msgMetadata
                        .setCompression(CompressionCodecProvider.convertToWireProtocol(conf.getCompressionType()));
            }
            // TODO: 10/21/23 设置没有压缩的消息大小
            msgMetadata.setUncompressedSize(uncompressedSize);
        }

        // TODO: 10/21/23 块小于等于1
        if (canAddToBatch(msg) && totalChunks <= 1) {
            // TODO: 10/23/23 是否把消息加入到现有的batch里面
            if (canAddToCurrentBatch(msg)) {
                // should trigger complete the batch message, new message will add to a new batch and new batch
                // sequence id use the new message, so that broker can handle the message duplication
                if (sequenceId <= lastSequenceIdPushed) {
                    isLastSequenceIdPotentialDuplicated = true;
                    if (sequenceId <= lastSequenceIdPublished) {
                        log.warn("Message with sequence id {} is definitely a duplicate", sequenceId);
                    } else {
                        log.info("Message with sequence id {} might be a duplicate but cannot be determined at this"
                                + " time.", sequenceId);
                    }
                    // TODO: 10/23/23 发送消息
                    doBatchSendAndAdd(msg, callback, payload);
                } else {
                    // Should flush the last potential duplicated since can't combine potential duplicated messages
                    // and non-duplicated messages into a batch.
                    if (isLastSequenceIdPotentialDuplicated) {
                        doBatchSendAndAdd(msg, callback, payload);
                    } else {
                        // TODO: 10/23/23 则把当前消息放入容器（重点）
                        // handle boundary cases where message being added would exceed
                        // batch size and/or max message size
                        boolean isBatchFull = batchMessageContainer.add(msg, callback);
                        lastSendFuture = callback.getFuture();
                        payload.release();
                        if (isBatchFull) {
                            batchMessageAndSend();
                        }
                    }
                    isLastSequenceIdPotentialDuplicated = false;
                }
            } else {
                // TODO: 10/23/23 如果不可以加入到当前批次，那么就单独发送该消息
                doBatchSendAndAdd(msg, callback, payload);
            }
        } else {
            // TODO: 10/23/23 分块发送消息
            // in this case compression has not been applied by the caller
            // but we have to compress the payload if compression is configured
            if (!compressed) {
                // TODO: 10/23/23 压缩消息
                chunkPayload = applyCompression(chunkPayload);
            }
            // TODO: 10/23/23 加密消息
            ByteBuf encryptedPayload = encryptMessage(msgMetadata, chunkPayload);

            // TODO: 10/23/23 记录这次发送了多少条数据
            // When publishing during replication, we need to set the correct number of message in batch
            // This is only used in tracking the publish rate stats
            int numMessages = msg.getMessageBuilder().hasNumMessagesInBatch()
                    ? msg.getMessageBuilder().getNumMessagesInBatch()
                    : 1;
            final OpSendMsg op;
            if (msg.getSchemaState() == MessageImpl.SchemaState.Ready) {
                // TODO: 10/23/23 发送消息命令
                ByteBufPair cmd = sendMessage(producerId, sequenceId, numMessages, msgMetadata, encryptedPayload);
                op = OpSendMsg.create(msg, cmd, sequenceId, callback);
            } else {
                op = OpSendMsg.create(msg, null, sequenceId, callback);
                final MessageMetadata finalMsgMetadata = msgMetadata;
                op.rePopulate = () -> {
                    op.cmd = sendMessage(producerId, sequenceId, numMessages, finalMsgMetadata, encryptedPayload);
                };
            }
            op.setNumMessagesInBatch(numMessages);
            op.setBatchSizeByte(encryptedPayload.readableBytes());
            if (totalChunks > 1) {
                op.totalChunks = totalChunks;
                op.chunkId = chunkId;
            }
            op.chunkedMessageCtx = chunkedMessageCtx;
            lastSendFuture = callback.getFuture();
            // TODO: 10/23/23 处理发送消息操作
            processOpSendMsg(op);
        }
    }

    private boolean populateMessageSchema(MessageImpl msg, SendCallback callback) {
        MessageMetadata msgMetadataBuilder = msg.getMessageBuilder();
        if (msg.getSchemaInternal() == schema) {
            schemaVersion.ifPresent(v -> msgMetadataBuilder.setSchemaVersion(v));
            msg.setSchemaState(MessageImpl.SchemaState.Ready);
            return true;
        }
        if (!isMultiSchemaEnabled(true)) {
            PulsarClientException.InvalidMessageException e = new PulsarClientException.InvalidMessageException(
                    format("The producer %s of the topic %s is disabled the `MultiSchema`", producerName, topic)
                    , msg.getSequenceId());
            completeCallbackAndReleaseSemaphore(msg.getUncompressedSize(), callback, e);
            return false;
        }
        SchemaHash schemaHash = SchemaHash.of(msg.getSchemaInternal());
        byte[] schemaVersion = schemaCache.get(schemaHash);
        if (schemaVersion != null) {
            msgMetadataBuilder.setSchemaVersion(schemaVersion);
            msg.setSchemaState(MessageImpl.SchemaState.Ready);
        }
        return true;
    }

    private boolean rePopulateMessageSchema(MessageImpl msg) {
        SchemaHash schemaHash = SchemaHash.of(msg.getSchemaInternal());
        byte[] schemaVersion = schemaCache.get(schemaHash);
        if (schemaVersion == null) {
            return false;
        }
        msg.getMessageBuilder().setSchemaVersion(schemaVersion);
        msg.setSchemaState(MessageImpl.SchemaState.Ready);
        return true;
    }

    private void tryRegisterSchema(ClientCnx cnx, MessageImpl msg, SendCallback callback, long expectedCnxEpoch) {
        if (!changeToRegisteringSchemaState()) {
            return;
        }
        SchemaInfo schemaInfo = msg.hasReplicateFrom() ? msg.getSchemaInfoForReplicator() : msg.getSchemaInfo();
        schemaInfo = Optional.ofNullable(schemaInfo)
                                        .filter(si -> si.getType().getValue() > 0)
                                        .orElse(Schema.BYTES.getSchemaInfo());
        getOrCreateSchemaAsync(cnx, schemaInfo).handle((v, ex) -> {
            if (ex != null) {
                Throwable t = FutureUtil.unwrapCompletionException(ex);
                log.warn("[{}] [{}] GetOrCreateSchema error", topic, producerName, t);
                if (t instanceof PulsarClientException.IncompatibleSchemaException) {
                    msg.setSchemaState(MessageImpl.SchemaState.Broken);
                    callback.sendComplete((PulsarClientException.IncompatibleSchemaException) t);
                }
            } else {
                log.info("[{}] [{}] GetOrCreateSchema succeed", topic, producerName);
                // In broker, if schema version is an empty byte array, it means the topic doesn't have schema. In this
                // case, we should not cache the schema version so that the schema version of the message metadata will
                // be null, instead of an empty array.
                if (v.length != 0) {
                    SchemaHash schemaHash = SchemaHash.of(msg.getSchemaInternal());
                    schemaCache.putIfAbsent(schemaHash, v);
                    msg.getMessageBuilder().setSchemaVersion(v);
                }
                msg.setSchemaState(MessageImpl.SchemaState.Ready);
            }
            cnx.ctx().channel().eventLoop().execute(() -> {
                synchronized (ProducerImpl.this) {
                    recoverProcessOpSendMsgFrom(cnx, msg, expectedCnxEpoch);
                }
            });
            return null;
        });
    }

    private CompletableFuture<byte[]> getOrCreateSchemaAsync(ClientCnx cnx, SchemaInfo schemaInfo) {
        if (!Commands.peerSupportsGetOrCreateSchema(cnx.getRemoteEndpointProtocolVersion())) {
            return FutureUtil.failedFuture(
                new PulsarClientException.NotSupportedException(
                    format("The command `GetOrCreateSchema` is not supported for the protocol version %d. "
                            + "The producer is %s, topic is %s",
                            cnx.getRemoteEndpointProtocolVersion(), producerName, topic)));
        }
        long requestId = client.newRequestId();
        ByteBuf request = Commands.newGetOrCreateSchema(requestId, topic, schemaInfo);
        log.info("[{}] [{}] GetOrCreateSchema request", topic, producerName);
        return cnx.sendGetOrCreateSchema(request, requestId);
    }

    protected ByteBuf encryptMessage(MessageMetadata msgMetadata, ByteBuf compressedPayload)
            throws PulsarClientException {

        if (!conf.isEncryptionEnabled() || msgCrypto == null) {
            return compressedPayload;
        }

        try {
            int maxSize = msgCrypto.getMaxOutputSize(compressedPayload.readableBytes());
            ByteBuf encryptedPayload = PulsarByteBufAllocator.DEFAULT.buffer(maxSize);
            ByteBuffer targetBuffer = encryptedPayload.nioBuffer(0, maxSize);

            msgCrypto.encrypt(conf.getEncryptionKeys(), conf.getCryptoKeyReader(), () -> msgMetadata,
                    compressedPayload.nioBuffer(), targetBuffer);

            encryptedPayload.writerIndex(targetBuffer.remaining());
            compressedPayload.release();
            return encryptedPayload;
        } catch (PulsarClientException e) {
            // Unless config is set to explicitly publish un-encrypted message upon failure, fail the request
            if (conf.getCryptoFailureAction() == ProducerCryptoFailureAction.SEND) {
                log.warn("[{}] [{}] Failed to encrypt message {}. Proceeding with publishing unencrypted message",
                        topic, producerName, e.getMessage());
                return compressedPayload;
            }
            throw e;
        }
    }

    protected ByteBufPair sendMessage(long producerId, long sequenceId, int numMessages, MessageMetadata msgMetadata,
            ByteBuf compressedPayload) {
        // TODO: 10/23/23 发送消息命令
        return Commands.newSend(producerId, sequenceId, numMessages, getChecksumType(), msgMetadata, compressedPayload);
    }

    protected ByteBufPair sendMessage(long producerId, long lowestSequenceId, long highestSequenceId, int numMessages,
                                      MessageMetadata msgMetadata, ByteBuf compressedPayload) {
        // TODO: 10/23/23 发送消息命令
        return Commands.newSend(producerId, lowestSequenceId, highestSequenceId, numMessages, getChecksumType(),
                msgMetadata, compressedPayload);
    }

    protected ChecksumType getChecksumType() {
        if (connectionHandler.cnx() == null
                || connectionHandler.cnx().getRemoteEndpointProtocolVersion() >= brokerChecksumSupportedVersion()) {
            return ChecksumType.Crc32c;
        } else {
            return ChecksumType.None;
        }
    }

    private boolean canAddToBatch(MessageImpl<?> msg) {
        // TODO: 10/21/23 客户端默认开启了batch 发送数据
        return msg.getSchemaState() == MessageImpl.SchemaState.Ready
                && isBatchMessagingEnabled() && !msg.getMessageBuilder().hasDeliverAtTime();
    }

    private boolean canAddToCurrentBatch(MessageImpl<?> msg) {
        // TODO: 10/23/23 检查批量消息容器是否还有空间
        return batchMessageContainer.haveEnoughSpace(msg)
               && (!isMultiSchemaEnabled(false) || batchMessageContainer.hasSameSchema(msg))
                && batchMessageContainer.hasSameTxn(msg);
    }

    // TODO: 10/23/23 触发批量发送，并把当前消息放入新的批量消息容器
    private void doBatchSendAndAdd(MessageImpl<?> msg, SendCallback callback, ByteBuf payload) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] [{}] Closing out batch to accommodate large message with size {}", topic, producerName,
                    msg.getUncompressedSize());
        }
        try {
            batchMessageAndSend();
            batchMessageContainer.add(msg, callback);
            lastSendFuture = callback.getFuture();
        } finally {
            payload.release();
        }
    }

    private boolean isValidProducerState(SendCallback callback, long sequenceId) {
        switch (getState()) {
        case Ready:
            // OK
        case Connecting:
            // We are OK to queue the messages on the client, it will be sent to the broker once we get the connection
        case RegisteringSchema:
            // registering schema
            return true;
        case Closing:
        case Closed:
            callback.sendComplete(
                    new PulsarClientException.AlreadyClosedException("Producer already closed", sequenceId));
            return false;
        case ProducerFenced:
            callback.sendComplete(new PulsarClientException.ProducerFencedException("Producer was fenced"));
            return false;
        case Terminated:
            callback.sendComplete(
                    new PulsarClientException.TopicTerminatedException("Topic was terminated", sequenceId));
            return false;
        case Failed:
        case Uninitialized:
        default:
            callback.sendComplete(new PulsarClientException.NotConnectedException(sequenceId));
            return false;
        }
    }

    private boolean canEnqueueRequest(SendCallback callback, long sequenceId, int payloadSize) {
        try {
            if (conf.isBlockIfQueueFull()) {
                if (semaphore.isPresent()) {
                    semaphore.get().acquire();
                }
                client.getMemoryLimitController().reserveMemory(payloadSize);
            } else {
                if (!semaphore.map(Semaphore::tryAcquire).orElse(true)) {
                    callback.sendComplete(new PulsarClientException.ProducerQueueIsFullError(
                            "Producer send queue is full", sequenceId));
                    return false;
                }

                if (!client.getMemoryLimitController().tryReserveMemory(payloadSize)) {
                    semaphore.ifPresent(Semaphore::release);
                    callback.sendComplete(new PulsarClientException.MemoryBufferIsFullError(
                            "Client memory buffer is full", sequenceId));
                    return false;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            callback.sendComplete(new PulsarClientException(e, sequenceId));
            return false;
        }

        return true;
    }

    // TODO: 10/23/23 实现了Runnable
    private static final class WriteInEventLoopCallback implements Runnable {
        // TODO: 10/23/23 生产者
        private ProducerImpl<?> producer;
        // TODO: 10/23/23 发送命名
        private ByteBufPair cmd;
        private long sequenceId;
        private ClientCnx cnx;
        private OpSendMsg op;

        static WriteInEventLoopCallback create(ProducerImpl<?> producer, ClientCnx cnx, OpSendMsg op) {
            WriteInEventLoopCallback c = RECYCLER.get();
            c.producer = producer;
            c.cnx = cnx;
            c.sequenceId = op.sequenceId;
            c.cmd = op.cmd;
            c.op = op;
            return c;
        }

        @Override
        public void run() {
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] Sending message cnx {}, sequenceId {}", producer.topic, producer.producerName, cnx,
                        sequenceId);
            }

            try {
                // TODO: 10/23/23 WriteInEventLoopCallback 类中核心代码，仅仅是通过 channel 发送消息
                cnx.ctx().writeAndFlush(cmd, cnx.ctx().voidPromise());
                op.updateSentTimestamp();
            } finally {
                recycle();
            }
        }

        private void recycle() {
            producer = null;
            cnx = null;
            cmd = null;
            sequenceId = -1;
            op = null;
            recyclerHandle.recycle(this);
        }

        private final Handle<WriteInEventLoopCallback> recyclerHandle;

        private WriteInEventLoopCallback(Handle<WriteInEventLoopCallback> recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
        }

        private static final Recycler<WriteInEventLoopCallback> RECYCLER = new Recycler<WriteInEventLoopCallback>() {
            @Override
            protected WriteInEventLoopCallback newObject(Handle<WriteInEventLoopCallback> handle) {
                return new WriteInEventLoopCallback(handle);
            }
        };
    }

    private static final class LastSendFutureWrapper {
        private final CompletableFuture<MessageId> lastSendFuture;
        private static final int FALSE = 0;
        private static final int TRUE = 1;
        private static final AtomicIntegerFieldUpdater<LastSendFutureWrapper> THROW_ONCE_UPDATER =
                AtomicIntegerFieldUpdater.newUpdater(LastSendFutureWrapper.class, "throwOnce");
        private volatile int throwOnce = FALSE;

        private LastSendFutureWrapper(CompletableFuture<MessageId> lastSendFuture) {
            this.lastSendFuture = lastSendFuture;
        }
        static LastSendFutureWrapper create(CompletableFuture<MessageId> lastSendFuture) {
            return new LastSendFutureWrapper(lastSendFuture);
        }
        public CompletableFuture<Void> handleOnce() {
            return lastSendFuture.handle((ignore, t) -> {
                if (t != null && THROW_ONCE_UPDATER.compareAndSet(this, FALSE, TRUE)) {
                    throw FutureUtil.wrapToCompletionException(t);
                }
                return null;
            });
        }
    }


    // TODO: 10/23/23 producer关闭
    @Override
    public CompletableFuture<Void> closeAsync() {
        // TODO: 10/23/23 获取和更新 Producer 状态,如果状态是已关闭，直接返回已关闭，否则直接返回正在关闭
        final State currentState = getAndUpdateState(state -> {
            if (state == State.Closed) {
                return state;
            }
            return State.Closing;
        });

        // TODO: 10/23/23 如果当前状态是已关闭或正在关闭，则直接返回关闭成功
        if (currentState == State.Closed || currentState == State.Closing) {
            return CompletableFuture.completedFuture(null);
        }

        closeProducerTasks();

        // TODO: 10/23/23 Producer 已关闭，设置关闭状态，并且把正处理消息全部设置为 producer 已关闭异常
        ClientCnx cnx = cnx();
        if (cnx == null || currentState != State.Ready) {
            log.info("[{}] [{}] Closed Producer (not connected)", topic, producerName);
            closeAndClearPendingMessages();
            return CompletableFuture.completedFuture(null);
        }

        // TODO: 10/23/23 生成 producer 关闭命令，向 broker 注销自己 
        long requestId = client.newRequestId();
        ByteBuf cmd = Commands.newCloseProducer(producerId, requestId);

        CompletableFuture<Void> closeFuture = new CompletableFuture<>();
        // TODO: 10/23/23 发送producer关闭命令给broker 
        cnx.sendRequestWithId(cmd, requestId).handle((v, exception) -> {
            cnx.removeProducer(producerId);
            // TODO: 10/23/23 要么 已经成功从 broker 接收到关闭 producer 的应答命令，
            //  要么 此时与broker连接已经被破坏，无论什么情况， producer 将关闭（设置状态为 关闭，并且把正处理的消息都释放，
            //  其队列将被清理，其他的资源也被释放）
            if (exception == null || !cnx.ctx().channel().isActive()) {
                // Either we've received the success response for the close producer command from the broker, or the
                // connection did break in the meantime. In any case, the producer is gone.
                log.info("[{}] [{}] Closed Producer", topic, producerName);
                closeAndClearPendingMessages();
                closeFuture.complete(null);
            } else {
                closeFuture.completeExceptionally(exception);
            }

            return null;
        });

        return closeFuture;
    }

    private synchronized void closeAndClearPendingMessages() {
        setState(State.Closed);
        client.cleanupProducer(this);
        PulsarClientException ex = new PulsarClientException.AlreadyClosedException(
                format("The producer %s of the topic %s was already closed when closing the producers",
                        producerName, topic));
        // Use null for cnx to ensure that the pending messages are failed immediately
        failPendingMessages(null, ex);
    }

    @Override
    public boolean isConnected() {
        return getCnxIfReady() != null;
    }

    /**
     * Hook method for testing. By returning null, it's possible to prevent messages
     * being delivered to the broker.
     *
     * @return cnx if OpSend messages should be written to open connection. Caller must
     * verify that the returned cnx is not null before using reference.
     */
    protected ClientCnx getCnxIfReady() {
        if (getState() == State.Ready) {
            return connectionHandler.cnx();
        } else {
            return null;
        }
    }

    @Override
    public long getLastDisconnectedTimestamp() {
        return connectionHandler.lastConnectionClosedTimestamp;
    }

    public boolean isWritable() {
        ClientCnx cnx = connectionHandler.cnx();
        return cnx != null && cnx.channel().isWritable();
    }

    public void terminated(ClientCnx cnx) {
        State previousState = getAndUpdateState(state -> (state == State.Closed ? State.Closed : State.Terminated));
        if (previousState != State.Terminated && previousState != State.Closed) {
            log.info("[{}] [{}] The topic has been terminated", topic, producerName);
            setClientCnx(null);
            synchronized (this) {
                failPendingMessages(cnx,
                        new PulsarClientException.TopicTerminatedException(
                                format("The topic %s that the producer %s produces to has been terminated",
                                        topic, producerName)));
            }
        }
    }

    // TODO: 10/23/23 客户端实际处理 broker 的应答
    void ackReceived(ClientCnx cnx, long sequenceId, long highestSequenceId, long ledgerId, long entryId) {
        OpSendMsg op = null;
        // TODO: 10/23/23 同步代码块（对象监视器）
        synchronized (this) {
            // TODO: 10/23/23 从正处理消息队列查看一消息发送指令
            op = pendingMessages.peek();
            // TODO: 10/23/23 如果为空，则意味着已超时处理，返回
            if (op == null) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] [{}] Got ack for timed out msg {} - {}",
                            topic, producerName, sequenceId, highestSequenceId);
                }
                return;
            }

            // TODO: 10/23/23 这种情况（不知什么情况会出现），应该强制关闭连接，消息应该在新连接里重新传输
            if (sequenceId > op.sequenceId) {
                log.warn("[{}] [{}] Got ack for msg. expecting: {} - {} - got: {} - {} - queue-size: {}",
                        topic, producerName, op.sequenceId, op.highestSequenceId, sequenceId, highestSequenceId,
                        pendingMessages.messagesCount());
                // Force connection closing so that messages can be re-transmitted in a new connection
                cnx.channel().close();
                return;
            } else if (sequenceId < op.sequenceId) {
                // TODO: 10/23/23 不管这种应答，因为这消息已经被超时处理 
                // Ignoring the ack since it's referring to a message that has already timed out.
                if (log.isDebugEnabled()) {
                    log.debug("[{}] [{}] Got ack for timed out msg. expecting: {} - {} - got: {} - {}",
                            topic, producerName, op.sequenceId, op.highestSequenceId, sequenceId, highestSequenceId);
                }
                return;
            } else {
                // TODO: 10/23/23 消息已被正常处理 
                // Add check `sequenceId >= highestSequenceId` for backward compatibility.
                if (sequenceId >= highestSequenceId || highestSequenceId == op.highestSequenceId) {
                    // Message was persisted correctly
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] [{}] Received ack for msg {} ", topic, producerName, sequenceId);
                    }
                    pendingMessages.remove();
                    releaseSemaphoreForSendOp(op);
                } else {
                    log.warn("[{}] [{}] Got ack for batch msg error. expecting: {} - {} - got: {} - {} - queue-size: {}"
                                    + "",
                            topic, producerName, op.sequenceId, op.highestSequenceId, sequenceId, highestSequenceId,
                            pendingMessages.messagesCount());
                    // Force connection closing so that messages can be re-transmitted in a new connection
                    cnx.channel().close();
                    return;
                }
            }
        }

        OpSendMsg finalOp = op;
        LAST_SEQ_ID_PUBLISHED_UPDATER.getAndUpdate(this, last -> Math.max(last, getHighestSequenceId(finalOp)));
        // TODO: 10/23/23  生成消息ID
        op.setMessageId(ledgerId, entryId, partitionIndex);
        if (op.totalChunks > 1) {
            if (op.chunkId == 0) {
                op.chunkedMessageCtx.firstChunkMessageId = new MessageIdImpl(ledgerId, entryId, partitionIndex);
            } else if (op.chunkId == op.totalChunks - 1) {
                op.chunkedMessageCtx.lastChunkMessageId = new MessageIdImpl(ledgerId, entryId, partitionIndex);
                op.setMessageId(op.chunkedMessageCtx.getChunkMessageId());
            }
        }


        // if message is chunked then call callback only on last chunk
        if (op.totalChunks <= 1 || (op.chunkId == op.totalChunks - 1)) {
            try {
                // Need to protect ourselves from any exception being thrown in the future handler from the
                // application
                op.sendComplete(null);
            } catch (Throwable t) {
                log.warn("[{}] [{}] Got exception while completing the callback for msg {}:", topic,
                        producerName, sequenceId, t);
            }
        }
        ReferenceCountUtil.safeRelease(op.cmd);
        op.recycle();
    }

    private long getHighestSequenceId(OpSendMsg op) {
        return Math.max(op.highestSequenceId, op.sequenceId);
    }

    private void releaseSemaphoreForSendOp(OpSendMsg op) {

        semaphoreRelease(isBatchMessagingEnabled() ? op.numMessagesInBatch : 1);

        client.getMemoryLimitController().releaseMemory(op.uncompressedSize);
    }

    private void completeCallbackAndReleaseSemaphore(long payloadSize, SendCallback callback, Exception exception) {
        semaphore.ifPresent(Semaphore::release);
        client.getMemoryLimitController().releaseMemory(payloadSize);
        callback.sendComplete(exception);
    }

    /**
     * Checks message checksum to retry if message was corrupted while sending to broker. Recomputes checksum of the
     * message header-payload again.
     * <ul>
     * <li><b>if matches with existing checksum</b>: it means message was corrupt while sending to broker. So, resend
     * message</li>
     * <li><b>if doesn't match with existing checksum</b>: it means message is already corrupt and can't retry again.
     * So, fail send-message by failing callback</li>
     * </ul>
     *
     * @param cnx
     * @param sequenceId
     */
    protected synchronized void recoverChecksumError(ClientCnx cnx, long sequenceId) {
        OpSendMsg op = pendingMessages.peek();
        if (op == null) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] Got send failure for timed out msg {}", topic, producerName, sequenceId);
            }
        } else {
            long expectedSequenceId = getHighestSequenceId(op);
            if (sequenceId == expectedSequenceId) {
                boolean corrupted = !verifyLocalBufferIsNotCorrupted(op);
                if (corrupted) {
                    // remove message from pendingMessages queue and fail callback
                    pendingMessages.remove();
                    releaseSemaphoreForSendOp(op);
                    try {
                        op.sendComplete(
                            new PulsarClientException.ChecksumException(
                                format("The checksum of the message which is produced by producer %s to the topic "
                                        + "%s is corrupted", producerName, topic)));
                    } catch (Throwable t) {
                        log.warn("[{}] [{}] Got exception while completing the callback for msg {}:", topic,
                                producerName, sequenceId, t);
                    }
                    ReferenceCountUtil.safeRelease(op.cmd);
                    op.recycle();
                    return;
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] [{}] Message is not corrupted, retry send-message with sequenceId {}", topic,
                                producerName, sequenceId);
                    }
                }

            } else {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] [{}] Corrupt message is already timed out {}", topic, producerName, sequenceId);
                }
            }
        }
        // as msg is not corrupted : let producer resend pending-messages again including checksum failed message
        resendMessages(cnx, this.connectionHandler.getEpoch());
    }

    protected synchronized void recoverNotAllowedError(long sequenceId, String errorMsg) {
        OpSendMsg op = pendingMessages.peek();
        if (op != null && sequenceId == getHighestSequenceId(op)) {
            pendingMessages.remove();
            releaseSemaphoreForSendOp(op);
            try {
                op.sendComplete(
                        new PulsarClientException.NotAllowedException(errorMsg));
            } catch (Throwable t) {
                log.warn("[{}] [{}] Got exception while completing the callback for msg {}:", topic,
                        producerName, sequenceId, t);
            }
            ReferenceCountUtil.safeRelease(op.cmd);
            op.recycle();
        }
    }

    /**
     * Computes checksum again and verifies it against existing checksum. If checksum doesn't match it means that
     * message is corrupt.
     *
     * @param op
     * @return returns true only if message is not modified and computed-checksum is same as previous checksum else
     *         return false that means that message is corrupted. Returns true if checksum is not present.
     */
    protected boolean verifyLocalBufferIsNotCorrupted(OpSendMsg op) {
        ByteBufPair msg = op.cmd;

        if (msg != null) {
            ByteBuf headerFrame = msg.getFirst();
            headerFrame.markReaderIndex();
            try {
                // skip bytes up to checksum index
                headerFrame.skipBytes(4); // skip [total-size]
                int cmdSize = (int) headerFrame.readUnsignedInt();
                headerFrame.skipBytes(cmdSize);
                // verify if checksum present
                if (hasChecksum(headerFrame)) {
                    int checksum = readChecksum(headerFrame);
                    // msg.readerIndex is already at header-payload index, Recompute checksum for headers-payload
                    int metadataChecksum = computeChecksum(headerFrame);
                    long computedChecksum = resumeChecksum(metadataChecksum, msg.getSecond());
                    return checksum == computedChecksum;
                } else {
                    log.warn("[{}] [{}] checksum is not present into message with id {}", topic, producerName,
                            op.sequenceId);
                }
            } finally {
                headerFrame.resetReaderIndex();
            }
            return true;
        } else {
            log.warn("[{}] Failed while casting empty ByteBufPair, ", producerName);
            return false;
        }
    }

    static class ChunkedMessageCtx extends AbstractReferenceCounted {
        protected MessageIdImpl firstChunkMessageId;
        protected MessageIdImpl lastChunkMessageId;

        public ChunkMessageIdImpl getChunkMessageId() {
            return new ChunkMessageIdImpl(firstChunkMessageId, lastChunkMessageId);
        }

        private static final Recycler<ProducerImpl.ChunkedMessageCtx> RECYCLER =
                new Recycler<ProducerImpl.ChunkedMessageCtx>() {
                    protected ProducerImpl.ChunkedMessageCtx newObject(
                            Recycler.Handle<ProducerImpl.ChunkedMessageCtx> handle) {
                        return new ProducerImpl.ChunkedMessageCtx(handle);
                    }
                };

        public static ChunkedMessageCtx get(int totalChunks) {
            ChunkedMessageCtx chunkedMessageCtx = RECYCLER.get();
            chunkedMessageCtx.setRefCnt(totalChunks);
            return chunkedMessageCtx;
        }

        private final Handle<ProducerImpl.ChunkedMessageCtx> recyclerHandle;

        private ChunkedMessageCtx(Handle<ChunkedMessageCtx> recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
        }

        @Override
        protected void deallocate() {
            this.firstChunkMessageId = null;
            this.lastChunkMessageId = null;
            recyclerHandle.recycle(this);
        }

        @Override
        public ReferenceCounted touch(Object hint) {
            return this;
        }
    }

    protected static final class OpSendMsg {
        MessageImpl<?> msg;
        List<MessageImpl<?>> msgs;
        ByteBufPair cmd;
        SendCallback callback;
        Runnable rePopulate;
        ChunkedMessageCtx chunkedMessageCtx;
        long uncompressedSize;
        long sequenceId;
        long createdAt;
        long firstSentAt;
        long lastSentAt;
        int retryCount;
        long batchSizeByte = 0;
        int numMessagesInBatch = 1;
        long highestSequenceId;
        int totalChunks = 0;
        int chunkId = -1;

        static OpSendMsg create(MessageImpl<?> msg, ByteBufPair cmd, long sequenceId, SendCallback callback) {
            OpSendMsg op = RECYCLER.get();
            op.msg = msg;
            op.cmd = cmd;
            op.callback = callback;
            op.sequenceId = sequenceId;
            op.createdAt = System.nanoTime();
            op.uncompressedSize = msg.getUncompressedSize();
            return op;
        }

        static OpSendMsg create(List<MessageImpl<?>> msgs, ByteBufPair cmd, long sequenceId, SendCallback callback) {
            OpSendMsg op = RECYCLER.get();
            op.msgs = msgs;
            op.cmd = cmd;
            op.callback = callback;
            op.sequenceId = sequenceId;
            op.createdAt = System.nanoTime();
            op.uncompressedSize = 0;
            for (int i = 0; i < msgs.size(); i++) {
                op.uncompressedSize += msgs.get(i).getUncompressedSize();
            }
            return op;
        }

        static OpSendMsg create(List<MessageImpl<?>> msgs, ByteBufPair cmd, long lowestSequenceId,
                                long highestSequenceId,  SendCallback callback) {
            OpSendMsg op = RECYCLER.get();
            op.msgs = msgs;
            op.cmd = cmd;
            op.callback = callback;
            op.sequenceId = lowestSequenceId;
            op.highestSequenceId = highestSequenceId;
            op.createdAt = System.nanoTime();
            op.uncompressedSize = 0;
            for (int i = 0; i < msgs.size(); i++) {
                op.uncompressedSize += msgs.get(i).getUncompressedSize();
            }
            return op;
        }

        void updateSentTimestamp() {
            this.lastSentAt = System.nanoTime();
            if (this.firstSentAt == -1L) {
                this.firstSentAt = this.lastSentAt;
            }
            ++this.retryCount;
        }

        void sendComplete(final Exception e) {
            SendCallback callback = this.callback;
            if (null != callback) {
                Exception finalEx = e;
                if (finalEx != null && finalEx instanceof TimeoutException) {
                    TimeoutException te = (TimeoutException) e;
                    long sequenceId = te.getSequenceId();
                    long ns = System.nanoTime();
                    String errMsg = String.format(
                        "%s : createdAt %s seconds ago, firstSentAt %s seconds ago, lastSentAt %s seconds ago, "
                                + "retryCount %s",
                        te.getMessage(),
                        RelativeTimeUtil.nsToSeconds(ns - this.createdAt),
                        RelativeTimeUtil.nsToSeconds(this.firstSentAt <= 0
                                ? ns - this.lastSentAt
                                : ns - this.firstSentAt),
                        RelativeTimeUtil.nsToSeconds(ns - this.lastSentAt),
                        retryCount
                    );

                    finalEx = new TimeoutException(errMsg, sequenceId);
                }

                callback.sendComplete(finalEx);
            }
        }

        void recycle() {
            msg = null;
            msgs = null;
            cmd = null;
            callback = null;
            rePopulate = null;
            sequenceId = -1L;
            createdAt = -1L;
            firstSentAt = -1L;
            lastSentAt = -1L;
            highestSequenceId = -1L;
            totalChunks = 0;
            chunkId = -1;
            uncompressedSize = 0;
            retryCount = 0;
            batchSizeByte = 0;
            numMessagesInBatch = 1;
            ReferenceCountUtil.safeRelease(chunkedMessageCtx);
            chunkedMessageCtx = null;
            recyclerHandle.recycle(this);
        }

        void setNumMessagesInBatch(int numMessagesInBatch) {
            this.numMessagesInBatch = numMessagesInBatch;
        }

        void setBatchSizeByte(long batchSizeByte) {
            this.batchSizeByte = batchSizeByte;
        }

        void setMessageId(long ledgerId, long entryId, int partitionIndex) {
            if (msg != null) {
                msg.setMessageId(new MessageIdImpl(ledgerId, entryId, partitionIndex));
            } else {
                for (int batchIndex = 0; batchIndex < msgs.size(); batchIndex++) {
                    msgs.get(batchIndex)
                            .setMessageId(new BatchMessageIdImpl(ledgerId, entryId, partitionIndex, batchIndex));
                }
            }
        }

        void setMessageId(ChunkMessageIdImpl chunkMessageId) {
            if (msg != null) {
                msg.setMessageId(chunkMessageId);
            }
        }

        private OpSendMsg(Handle<OpSendMsg> recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
        }

        private final Handle<OpSendMsg> recyclerHandle;
        private static final Recycler<OpSendMsg> RECYCLER = new Recycler<OpSendMsg>() {
            @Override
            protected OpSendMsg newObject(Handle<OpSendMsg> handle) {
                return new OpSendMsg(handle);
            }
        };
    }


    /**
     * Queue implementation that is used as the pending messages queue.
     *
     * This implementation postpones adding of new OpSendMsg entries that happen
     * while the forEach call is in progress. This is needed for preventing
     * ConcurrentModificationExceptions that would occur when the forEach action
     * calls the add method via a callback in user code.
     *
     * This queue is not thread safe.
     */
    protected static class OpSendMsgQueue implements Iterable<OpSendMsg> {
        private final Queue<OpSendMsg> delegate = new ArrayDeque<>();
        private int forEachDepth = 0;
        private List<OpSendMsg> postponedOpSendMgs;
        private final AtomicInteger messagesCount = new AtomicInteger(0);

        @Override
        public void forEach(Consumer<? super OpSendMsg> action) {
            try {
                // track any forEach call that is in progress in the current call stack
                // so that adding a new item while iterating doesn't cause ConcurrentModificationException
                forEachDepth++;
                delegate.forEach(action);
            } finally {
                forEachDepth--;
                // if this is the top-most forEach call and there are postponed items, add them
                if (forEachDepth == 0 && postponedOpSendMgs != null && !postponedOpSendMgs.isEmpty()) {
                    delegate.addAll(postponedOpSendMgs);
                    postponedOpSendMgs.clear();
                }
            }
        }

        public boolean add(OpSendMsg o) {
            // postpone adding to the queue while forEach iteration is in progress
            messagesCount.addAndGet(o.numMessagesInBatch);
            if (forEachDepth > 0) {
                if (postponedOpSendMgs == null) {
                    postponedOpSendMgs = new ArrayList<>();
                }
                return postponedOpSendMgs.add(o);
            } else {
                return delegate.add(o);
            }
        }

        public void clear() {
            delegate.clear();
            messagesCount.set(0);
        }

        public void remove() {
            OpSendMsg op = delegate.remove();
            if (op != null) {
                messagesCount.addAndGet(-op.numMessagesInBatch);
            }
        }

        public OpSendMsg peek() {
            return delegate.peek();
        }

        public int messagesCount() {
            return messagesCount.get();
        }

        @Override
        public Iterator<OpSendMsg> iterator() {
            return delegate.iterator();
        }
    }

    @Override
    public void connectionOpened(final ClientCnx cnx) {
        previousExceptions.clear();

        final long epoch;
        synchronized (this) {
            // Because the state could have been updated while retrieving the connection, we set it back to connecting,
            // as long as the change from current state to connecting is a valid state change.
            if (!changeToConnecting()) {
                return;
            }
            // TODO: 10/18/23 在 CliectCnx 注册生产者之前设置 ClientCnx 引用，是为了创建生产者之前释放 cnx 对象，
            //  生成一个新的 cnx 对象
            // We set the cnx reference before registering the producer on the cnx, so if the cnx breaks before creating
            // the producer, it will try to grab a new cnx. We also increment and get the epoch value for the producer.
            epoch = connectionHandler.switchClientCnx(cnx);
        }
        // TODO: 2/24/23 注册当前的producerImpl到 客户端缓存 producers
        cnx.registerProducer(producerId, this);

        log.info("[{}] [{}] Creating producer on cnx {}", topic, producerName, cnx.ctx().channel());

        long requestId = client.newRequestId();

        PRODUCER_DEADLINE_UPDATER
            .compareAndSet(this, 0, System.currentTimeMillis() + client.getConfiguration().getOperationTimeoutMs());

        // TODO: 2/24/23 topic schema信息
        SchemaInfo schemaInfo = null;
        if (schema != null) {
            if (schema.getSchemaInfo() != null) {
                if (schema.getSchemaInfo().getType() == SchemaType.JSON) {
                    // TODO: 10/18/23 为了向后兼容目的，JSONSchema最初基于JSON模式标准为pojo生成了一个模式，
                    //  但现在已经对每个模式进行了标准化（处理）以生成基于Avro的模式
                    // for backwards compatibility purposes
                    // JSONSchema originally generated a schema for pojo based of of the JSON schema standard
                    // but now we have standardized on every schema to generate an Avro based schema
                    if (Commands.peerSupportJsonSchemaAvroFormat(cnx.getRemoteEndpointProtocolVersion())) {
                        schemaInfo = schema.getSchemaInfo();
                    } else if (schema instanceof JSONSchema){
                        JSONSchema jsonSchema = (JSONSchema) schema;
                        schemaInfo = jsonSchema.getBackwardsCompatibleJsonSchemaInfo();
                    } else {
                        schemaInfo = schema.getSchemaInfo();
                    }
                } else if (schema.getSchemaInfo().getType() == SchemaType.BYTES
                        || schema.getSchemaInfo().getType() == SchemaType.NONE) {
                    // don't set schema info for Schema.BYTES
                    schemaInfo = null;
                } else {
                    schemaInfo = schema.getSchemaInfo();
                }
            }
        }

        // TODO: 10/18/23 向broker注册生产者
        cnx.sendRequestWithId(
                // TODO: 2/24/23  发送 PRODUCER 请求到broker。broker接收到 PRODUCER 请求之后，会进行以下操作
                /**
                 * - 获取Topic信息
                 * - 检查 Topic 是否超出了BacklogQuota的限制，如果超过则返回异常，异常根据BacklogQuota.RetentionPolicy的不同而不同，
                 * 如果policy是producer_request_hold，
                 * client接收到异常之后，会阻塞Producer的创建；如果是producer_exception则会抛出异常
                 * - 创建服务端的Producer对象，并将Producer注册到Topic中
                 * - 返回 PRODUCER_SUCCESS响应给Producer，会携带消息去重中的最大sequenceId
                 */
                Commands.newProducer(topic, producerId, requestId, producerName, conf.isEncryptionEnabled(), metadata,
                        schemaInfo, epoch, userProvidedProducerName,
                        conf.getAccessMode(), topicEpoch, client.conf.isEnableTransaction(),
                        conf.getInitialSubscriptionName()),
                requestId).thenAccept(response -> {
            // TODO: 10/18/23 这里表示注册成功
            // TODO: 2/24/23 接收到PRODUCER_SUCCESS响应之后，Producer会根据sequenceId来初始化lastSequenceIdPublished以及msgIdGenerator，
            //  逻辑是如果msgIdGenerator为0并且producer没有指定InitialSequenceId，则将lastSequenceIdPublished置为sequenceId，msgIdGenerator置为sequenceId+1
                    String producerName = response.getProducerName();
                    long lastSequenceId = response.getLastSequenceId();
                    schemaVersion = Optional.ofNullable(response.getSchemaVersion());
                    schemaVersion.ifPresent(v -> schemaCache.put(SchemaHash.of(schema), v));

            // TODO: 10/18/23 重新连接到 broker ，并且 清空发送的消息。重新发送正处理的消息，设置 cnx 对象，有新消息将立即发送。 
                    // We are now reconnected to broker and clear to send messages. Re-send all pending messages and
                    // set the cnx pointer so that new messages will be sent immediately
                    synchronized (ProducerImpl.this) {
                        if (getState() == State.Closing || getState() == State.Closed) {
                            // TODO: 10/18/23 当正在重连的时候，生产者将被关闭，关闭连接确保 broker 释放生产者相关资源
                            // Producer was closed while reconnecting, close the connection to make sure the broker
                            // drops the producer on its side
                            cnx.removeProducer(producerId);
                            cnx.channel().close();
                            return;
                        }
                        // TODO: 10/18/23 重置定时重连器
                        resetBackoff();

                        log.info("[{}] [{}] Created producer on cnx {}", topic, producerName, cnx.ctx().channel());
                        connectionId = cnx.ctx().channel().toString();
                        connectedSince = DateFormatter.now();
                        if (conf.getAccessMode() != ProducerAccessMode.Shared && !topicEpoch.isPresent()) {
                            log.info("[{}] [{}] Producer epoch is {}", topic, producerName, response.getTopicEpoch());
                        }
                        topicEpoch = response.getTopicEpoch();

                        if (this.producerName == null) {
                            this.producerName = producerName;
                        }

                        if (this.msgIdGenerator == 0 && conf.getInitialSequenceId() == null) {
                            // TODO: 10/18/23 仅更新序列ID生成器（如果尚未修改）。 这意味着我们只想在第一次建立连接时更新id生成器，
                            //  并忽略 broker 在后续生成器重新连接中发送的序列ID
                            // Only update sequence id generator if it wasn't already modified. That means we only want
                            // to update the id generator the first time the producer gets established, and ignore the
                            // sequence id sent by broker in subsequent producer reconnects
                            this.lastSequenceIdPublished = lastSequenceId;
                            this.msgIdGenerator = lastSequenceId + 1;
                        }

                        // TODO: 2/24/23  如果开启了Batch，则初始化一个定时任务，根据配置的batchingMaxPublishDelayMicros时间来定时发送消息，
                        //  发送是会将BatchContainer中的所有message遍历封装到send请求中发送到Broker，
                        //  请求中会携带第一条message的sequenceId和最后一条mesage的sequenceId
                        if (!producerCreatedFuture.isDone() && isBatchMessagingEnabled()) {
                            // schedule the first batch message task
                            batchTimerTask = cnx.ctx().executor()
                                    .scheduleWithFixedDelay(catchingAndLoggingThrowables(() -> {
                                        if (log.isTraceEnabled()) {
                                            log.trace(
                                                    "[{}] [{}] Batching the messages from the batch container from "
                                                            + "timer thread",
                                                    topic,
                                                    producerName);
                                        }
                                        // semaphore acquired when message was enqueued to container
                                        synchronized (ProducerImpl.this) {
                                            // If it's closing/closed we need to ignore the send batch timer and not
                                            // schedule next timeout.
                                            if (getState() == State.Closing || getState() == State.Closed) {
                                                return;
                                            }

                                            batchMessageAndSend();
                                        }
                                    }), 0, conf.getBatchingMaxPublishDelayMicros(), TimeUnit.MICROSECONDS);
                        }
                        // TODO: 2/24/23 重新发送pendingMessages中的消息
                        resendMessages(cnx, epoch);
                    }
                }).exceptionally((e) -> {
            // TODO: 10/18/23 注册生产者失败 
                    Throwable cause = e.getCause();
                    cnx.removeProducer(producerId);
                    if (getState() == State.Closing || getState() == State.Closed) {
                        // TODO: 10/18/23 当正在重连的时候，生产者将被关闭，关闭连接确保 broker 释放生产者相关资源 
                        // Producer was closed while reconnecting, close the connection to make sure the broker
                        // drops the producer on its side
                        cnx.channel().close();
                        return null;
                    }

                    if (cause instanceof TimeoutException) {
                        // Creating the producer has timed out. We need to ensure the broker closes the producer
                        // in case it was indeed created, otherwise it might prevent new create producer operation,
                        // since we are not necessarily closing the connection.
                        long closeRequestId = client.newRequestId();
                        ByteBuf cmd = Commands.newCloseProducer(producerId, closeRequestId);
                        cnx.sendRequestWithId(cmd, closeRequestId);
                    }

                    if (cause instanceof PulsarClientException.ProducerFencedException) {
                        if (log.isDebugEnabled()) {
                            log.debug("[{}] [{}] Failed to create producer: {}",
                                    topic, producerName, cause.getMessage());
                        }
                    } else {
                        log.error("[{}] [{}] Failed to create producer: {}", topic, producerName, cause.getMessage());
                    }
                    // Close the producer since topic does not exist.
                    if (cause instanceof PulsarClientException.TopicDoesNotExistException) {
                        closeAsync().whenComplete((v, ex) -> {
                            if (ex != null) {
                                log.error("Failed to close producer on TopicDoesNotExistException.", ex);
                            }
                            producerCreatedFuture.completeExceptionally(cause);
                        });
                        return null;
                    }
            // TODO: 10/18/23 生产者 Topic 配额超出异常，可能连接已到上限 
                    if (cause instanceof PulsarClientException.ProducerBlockedQuotaExceededException) {
                        synchronized (this) {
                            log.warn("[{}] [{}] Topic backlog quota exceeded. Throwing Exception on producer.", topic,
                                    producerName);

                            if (log.isDebugEnabled()) {
                                log.debug("[{}] [{}] Pending messages: {}", topic, producerName,
                                        pendingMessages.messagesCount());
                            }

                            PulsarClientException bqe = new PulsarClientException.ProducerBlockedQuotaExceededException(
                                format("The backlog quota of the topic %s that the producer %s produces to is exceeded",
                                    topic, producerName));
                            // TODO: 10/18/23 注册失败时，可能要把正处理的消息全部设置成异常失败
                            failPendingMessages(cnx(), bqe);
                        }
                        // TODO: 10/18/23 生产者 Topic 积压配额错误 
                    } else if (cause instanceof PulsarClientException.ProducerBlockedQuotaExceededError) {
                        log.warn("[{}] [{}] Producer is blocked on creation because backlog exceeded on topic.",
                                producerName, topic);
                    }

            // TODO: 10/18/23 topic已关闭异常 
                    if (cause instanceof PulsarClientException.TopicTerminatedException) {
                        setState(State.Terminated);
                        synchronized (this) {
                            failPendingMessages(cnx(), (PulsarClientException) cause);
                        }
                        producerCreatedFuture.completeExceptionally(cause);
                        closeProducerTasks();
                        client.cleanupProducer(this);
                    } else if (cause instanceof PulsarClientException.ProducerFencedException) {
                        setState(State.ProducerFenced);
                        synchronized (this) {
                            failPendingMessages(cnx(), (PulsarClientException) cause);
                        }
                        producerCreatedFuture.completeExceptionally(cause);
                        closeProducerTasks();
                        client.cleanupProducer(this);
                    } else if (producerCreatedFuture.isDone() || //
                               (cause instanceof PulsarClientException && PulsarClientException.isRetriableError(cause)
                                && System.currentTimeMillis() < PRODUCER_DEADLINE_UPDATER.get(ProducerImpl.this))) {
                        // Either we had already created the producer once (producerCreatedFuture.isDone()) or we are
                        // still within the initial timeout budget and we are dealing with a retriable error
                        reconnectLater(cause);
                    } else {
                        // TODO: 10/18/23  设置生产者创建失败
                        setState(State.Failed);
                        producerCreatedFuture.completeExceptionally(cause);
                        closeProducerTasks();
                        client.cleanupProducer(this);
                        Timeout timeout = sendTimeout;
                        if (timeout != null) {
                            timeout.cancel();
                            sendTimeout = null;
                        }
                    }

                    return null;
                });
    }

    @Override
    public void connectionFailed(PulsarClientException exception) {
        boolean nonRetriableError = !PulsarClientException.isRetriableError(exception);
        boolean timeout = System.currentTimeMillis() > lookupDeadline;
        if (nonRetriableError || timeout) {
            exception.setPreviousExceptions(previousExceptions);
            if (producerCreatedFuture.completeExceptionally(exception)) {
                if (nonRetriableError) {
                    log.info("[{}] Producer creation failed for producer {} with unretriableError = {}",
                            topic, producerId, exception);
                } else {
                    log.info("[{}] Producer creation failed for producer {} after producerTimeout", topic, producerId);
                }
                closeProducerTasks();
                setState(State.Failed);
                client.cleanupProducer(this);
            }
        } else {
            previousExceptions.add(exception);
        }
    }

    private void closeProducerTasks() {
        // TODO: 10/23/23 如果定时器不为空，则取消
        Timeout timeout = sendTimeout;
        if (timeout != null) {
            timeout.cancel();
            sendTimeout = null;
        }

        // TODO: 10/23/23 批量消息发送定时器不为空，则取消
        ScheduledFuture<?> batchTimerTask = this.batchTimerTask;
        if (batchTimerTask != null) {
            batchTimerTask.cancel(false);
            this.batchTimerTask = null;
        }

        // TODO: 10/23/23 取消数据加密key加载任务
        if (keyGeneratorTask != null && !keyGeneratorTask.isCancelled()) {
            keyGeneratorTask.cancel(false);
        }

        stats.cancelStatsTimeout();
    }

    // TODO: 2/24/23 重新发送pendingMessages中的消息
    private void resendMessages(ClientCnx cnx, long expectedEpoch) {
        cnx.ctx().channel().eventLoop().execute(() -> {
            synchronized (this) {
                if (getState() == State.Closing || getState() == State.Closed) {
                    // Producer was closed while reconnecting, close the connection to make sure the broker
                    // drops the producer on its side
                    cnx.channel().close();
                    return;
                }
                int messagesToResend = pendingMessages.messagesCount();
                if (messagesToResend == 0) {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] [{}] No pending messages to resend {}", topic, producerName, messagesToResend);
                    }
                    if (changeToReadyState()) {
                        producerCreatedFuture.complete(ProducerImpl.this);
                        return;
                    } else {
                        // Producer was closed while reconnecting, close the connection to make sure the broker
                        // drops the producer on its side
                        cnx.channel().close();
                        return;
                    }

                }

                log.info("[{}] [{}] Re-Sending {} messages to server", topic, producerName, messagesToResend);
                recoverProcessOpSendMsgFrom(cnx, null, expectedEpoch);
            }
        });
    }

    /**
     * Strips checksum from {@link OpSendMsg} command if present else ignore it.
     *
     * @param op
     */
    private void stripChecksum(OpSendMsg op) {
        ByteBufPair msg = op.cmd;
        if (msg != null) {
            int totalMsgBufSize = msg.readableBytes();
            ByteBuf headerFrame = msg.getFirst();
            headerFrame.markReaderIndex();
            try {
                headerFrame.skipBytes(4); // skip [total-size]
                int cmdSize = (int) headerFrame.readUnsignedInt();

                // verify if checksum present
                headerFrame.skipBytes(cmdSize);

                if (!hasChecksum(headerFrame)) {
                    return;
                }

                int headerSize = 4 + 4 + cmdSize; // [total-size] [cmd-length] [cmd-size]
                int checksumSize = 4 + 2; // [magic-number] [checksum-size]
                int checksumMark = (headerSize + checksumSize); // [header-size] [checksum-size]
                int metaPayloadSize = (totalMsgBufSize - checksumMark); // metadataPayload = totalSize - checksumMark
                int newTotalFrameSizeLength = 4 + cmdSize + metaPayloadSize; // new total-size without checksum
                headerFrame.resetReaderIndex();
                int headerFrameSize = headerFrame.readableBytes();

                headerFrame.setInt(0, newTotalFrameSizeLength); // rewrite new [total-size]
                ByteBuf metadata = headerFrame.slice(checksumMark, headerFrameSize - checksumMark); // sliced only
                                                                                                    // metadata
                headerFrame.writerIndex(headerSize); // set headerFrame write-index to overwrite metadata over checksum
                metadata.readBytes(headerFrame, metadata.readableBytes());
                headerFrame.capacity(headerFrameSize - checksumSize); // reduce capacity by removed checksum bytes
            } finally {
                headerFrame.resetReaderIndex();
            }
        } else {
            log.warn("[{}] Failed while casting null into ByteBufPair", producerName);
        }
    }

    public int brokerChecksumSupportedVersion() {
        return ProtocolVersion.v6.getValue();
    }

    @Override
    String getHandlerName() {
        return producerName;
    }

    // TODO: 10/23/23 ProducerImpl 内，检查超时请求，主要用于消息发送超时检查
    /**
     * Process sendTimeout events.
     */
    @Override
    public void run(Timeout timeout) throws Exception {
        if (timeout.isCancelled()) {
            return;
        }

        long timeToWaitMs;

        synchronized (this) {
            // TODO: 10/23/23 如果当前处于正在关闭或已关闭状态则忽略本次超时检查
            // If it's closing/closed we need to ignore this timeout and not schedule next timeout.
            if (getState() == State.Closing || getState() == State.Closed) {
                return;
            }

            // TODO: 10/23/23  取出第一个消息（如果第一个消息都超时了，意味着后面的消息肯定全部超时了）
            OpSendMsg firstMsg = pendingMessages.peek();
            if (firstMsg == null) {
                // TODO: 10/23/23 发送超时时间，默认30s ，如果没有正在处理的消息，则重置超时时间为配置值
                // If there are no pending messages, reset the timeout to the configured value.
                timeToWaitMs = conf.getSendTimeoutMs();
            } else {
                // TODO: 10/23/23 如果至少有一个消息，则计算超时时间点差值与当前时间
                // If there is at least one message, calculate the diff between the message timeout and the elapsed
                // time since first message was created.
                long diff = conf.getSendTimeoutMs()
                        - TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - firstMsg.createdAt);
                if (diff <= 0) {
                    // TODO: 10/23/23 如果差值小于或等于0，则意味着消息已（发送或应答）超时，
                    //  每个消息都设置回调为超时异常，并情况正处理消息队列
                    // The diff is less than or equal to zero, meaning that the message has been timed out.
                    // Set the callback to timeout on every message, then clear the pending queue.
                    log.info("[{}] [{}] Message send timed out. Failing {} messages", topic, producerName,
                            pendingMessages.messagesCount());

                    PulsarClientException te = new PulsarClientException.TimeoutException(
                        format("The producer %s can not send message to the topic %s within given timeout",
                            producerName, topic), firstMsg.sequenceId);
                    failPendingMessages(cnx(), te);
                    // TODO: 10/23/23 一旦正处理消息队列被清空，则重置超时时间为配置值
                    // Since the pending queue is cleared now, set timer to expire after configured value.
                    timeToWaitMs = conf.getSendTimeoutMs();
                } else {
                    // TODO: 10/23/23 如果差值大于0，则设置此时间为超时时间（意味着下次这时间间隔将再次检查，而不是配置值）
                    // The diff is greater than zero, set the timeout to the diff value
                    timeToWaitMs = diff;
                }
            }

            sendTimeout = client.timer().newTimeout(this, timeToWaitMs, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * todo 注册失败时，可能要把正处理的消息全部设置成异常失败
     * This fails and clears the pending messages with the given exception. This method should be called from within the
     * ProducerImpl object mutex.
     */
    private void failPendingMessages(ClientCnx cnx, PulsarClientException ex) {
        if (cnx == null) {
            final AtomicInteger releaseCount = new AtomicInteger();
            final boolean batchMessagingEnabled = isBatchMessagingEnabled();
            pendingMessages.forEach(op -> {
                releaseCount.addAndGet(batchMessagingEnabled ? op.numMessagesInBatch : 1);
                try {
                    // Need to protect ourselves from any exception being thrown in the future handler from the
                    // application
                    ex.setSequenceId(op.sequenceId);
                    // if message is chunked then call callback only on last chunk
                    if (op.totalChunks <= 1 || (op.chunkId == op.totalChunks - 1)) {
                        // Need to protect ourselves from any exception being thrown in the future handler from the
                        // application
                        op.sendComplete(ex);
                    }
                } catch (Throwable t) {
                    log.warn("[{}] [{}] Got exception while completing the callback for msg {}:", topic, producerName,
                            op.sequenceId, t);
                }
                client.getMemoryLimitController().releaseMemory(op.uncompressedSize);
                ReferenceCountUtil.safeRelease(op.cmd);
                op.recycle();
            });

            pendingMessages.clear();
            semaphoreRelease(releaseCount.get());
            if (batchMessagingEnabled) {
                // TODO: 10/18/23 如果批量消息发送启用，则也设置
                failPendingBatchMessages(ex);
            }

        } else {
            // TODO: 10/18/23 如果还有连接，那么应该在事件循环线程上执行回调和循环（调用），以避免任何竞争条件，因为我们也在这个线程上写消息
            // If we have a connection, we schedule the callback and recycle on the event loop thread to avoid any
            // race condition since we also write the message on the socket from this thread
            cnx.ctx().channel().eventLoop().execute(() -> {
                synchronized (ProducerImpl.this) {
                    failPendingMessages(null, ex);
                }
            });
        }
    }

    /**
     * todo 批量消息里面也设置为异常
     * fail any pending batch messages that were enqueued, however batch was not closed out.
     *
     */
    private void failPendingBatchMessages(PulsarClientException ex) {
        if (batchMessageContainer.isEmpty()) {
            return;
        }
        final int numMessagesInBatch = batchMessageContainer.getNumMessagesInBatch();
        batchMessageContainer.discard(ex);
        semaphoreRelease(numMessagesInBatch);
    }

    @Override
    public CompletableFuture<Void> flushAsync() {
        synchronized (ProducerImpl.this) {
            if (isBatchMessagingEnabled()) {
                batchMessageAndSend();
            }
            CompletableFuture<MessageId>  lastSendFuture = this.lastSendFuture;
            if (!(lastSendFuture == this.lastSendFutureWrapper.lastSendFuture)) {
                this.lastSendFutureWrapper = LastSendFutureWrapper.create(lastSendFuture);
            }
        }

        return this.lastSendFutureWrapper.handleOnce();
    }

    @Override
    protected void triggerFlush() {
        if (isBatchMessagingEnabled()) {
            synchronized (ProducerImpl.this) {
                batchMessageAndSend();
            }
        }
    }

    // TODO: 10/23/23 在入队列之前，必须申请到信号量（这里主要防止内存溢出，当然也一定程度也保护 broker）
    // must acquire semaphore before enqueuing
    private void batchMessageAndSend() {
        if (log.isTraceEnabled()) {
            log.trace("[{}] [{}] Batching the messages from the batch container with {} messages", topic, producerName,
                    batchMessageContainer.getNumMessagesInBatch());
        }
        // TODO: 10/23/23 如果批量消息中消息队列不为空
        if (!batchMessageContainer.isEmpty()) {
            try {
                List<OpSendMsg> opSendMsgs;
                // TODO: 10/23/23 是否为多批
                if (batchMessageContainer.isMultiBatches()) {
                    opSendMsgs = batchMessageContainer.createOpSendMsgs();
                } else {
                    opSendMsgs = Collections.singletonList(batchMessageContainer.createOpSendMsg());
                }
                // TODO: 10/23/23 清理批量消息容器，下一次使用
                batchMessageContainer.clear();
                for (OpSendMsg opSendMsg : opSendMsgs) {
                    // TODO: 10/23/23 放入待处理消息队列，用于检查应答是否超时
                    processOpSendMsg(opSendMsg);
                }
            } catch (PulsarClientException e) {
                semaphoreRelease(batchMessageContainer.getNumMessagesInBatch());
            } catch (Throwable t) {
                semaphoreRelease(batchMessageContainer.getNumMessagesInBatch());
                log.warn("[{}] [{}] error while create opSendMsg by batch message container", topic, producerName, t);
            }
        }
    }

    // TODO: 10/23/23 这里是处理发送消息的操作
    protected void processOpSendMsg(OpSendMsg op) {
        if (op == null) {
            return;
        }
        try {
            // TODO: 10/23/23 如果配置了批量发送
            if (op.msg != null && isBatchMessagingEnabled()) {
                // TODO: 10/23/23 进行批量发送操作
                batchMessageAndSend();
            }
            pendingMessages.add(op);
            if (op.msg != null) {
                LAST_SEQ_ID_PUSHED_UPDATER.getAndUpdate(this,
                        last -> Math.max(last, getHighestSequenceId(op)));
            }

            final ClientCnx cnx = getCnxIfReady();
            if (cnx != null) {
                if (op.msg != null && op.msg.getSchemaState() == None) {
                    tryRegisterSchema(cnx, op.msg, op.callback, this.connectionHandler.getEpoch());
                    return;
                }
                // TODO: 10/23/23 发送消息
                // If we do have a connection, the message is sent immediately, otherwise we'll try again once a new
                // connection is established
                // TODO: 10/23/23 如果还有正常的连接，消息将被立即发送，否则尝试建立新的连接
                op.cmd.retain();
                // TODO: 10/23/23 这里才是把消息放入通道，发送出去
                cnx.ctx().channel().eventLoop().execute(WriteInEventLoopCallback.create(this, cnx, op));
                // TODO: 10/23/23 状态更新
                stats.updateNumMsgsSent(op.numMessagesInBatch, op.batchSizeByte);
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] [{}] Connection is not ready -- sequenceId {}", topic, producerName,
                        op.sequenceId);
                }
            }
        } catch (Throwable t) {
            releaseSemaphoreForSendOp(op);
            log.warn("[{}] [{}] error while closing out batch -- {}", topic, producerName, t);
            op.sendComplete(new PulsarClientException(t, op.sequenceId));
        }
    }

    // Must acquire a lock on ProducerImpl.this before calling method.
    private void recoverProcessOpSendMsgFrom(ClientCnx cnx, MessageImpl from, long expectedEpoch) {
        if (expectedEpoch != this.connectionHandler.getEpoch() || cnx() == null) {
            // In this case, the cnx passed to this method is no longer the active connection. This method will get
            // called again once the new connection registers the producer with the broker.
            log.info("[{}][{}] Producer epoch mismatch or the current connection is null. Skip re-sending the "
                    + " {} pending messages since they will deliver using another connection.", topic, producerName,
                    pendingMessages.messagesCount());
            return;
        }
        final boolean stripChecksum = cnx.getRemoteEndpointProtocolVersion() < brokerChecksumSupportedVersion();
        Iterator<OpSendMsg> msgIterator = pendingMessages.iterator();
        OpSendMsg pendingRegisteringOp = null;
        while (msgIterator.hasNext()) {
            OpSendMsg op = msgIterator.next();
            if (from != null) {
                if (op.msg == from) {
                    from = null;
                } else {
                    continue;
                }
            }
            if (op.msg != null) {
                if (op.msg.getSchemaState() == None) {
                    if (!rePopulateMessageSchema(op.msg)) {
                        pendingRegisteringOp = op;
                        break;
                    }
                } else if (op.msg.getSchemaState() == Broken) {
                    op.recycle();
                    msgIterator.remove();
                    continue;
                }
            }
            if (op.cmd == null) {
                checkState(op.rePopulate != null);
                op.rePopulate.run();
            }
            if (stripChecksum) {
                stripChecksum(op);
            }
            op.cmd.retain();
            if (log.isDebugEnabled()) {
                log.debug("[{}] [{}] Re-Sending message in cnx {}, sequenceId {}", topic, producerName,
                          cnx.channel(), op.sequenceId);
            }
            cnx.ctx().write(op.cmd, cnx.ctx().voidPromise());
            op.updateSentTimestamp();
            stats.updateNumMsgsSent(op.numMessagesInBatch, op.batchSizeByte);
        }
        cnx.ctx().flush();
        if (!changeToReadyState()) {
            // Producer was closed while reconnecting, close the connection to make sure the broker
            // drops the producer on its side
            cnx.channel().close();
            return;
        }
        if (pendingRegisteringOp != null) {
            tryRegisterSchema(cnx, pendingRegisteringOp.msg, pendingRegisteringOp.callback, expectedEpoch);
        }
    }

    public long getDelayInMillis() {
        OpSendMsg firstMsg = pendingMessages.peek();
        if (firstMsg != null) {
            return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - firstMsg.createdAt);
        }
        return 0L;
    }

    public String getConnectionId() {
        return cnx() != null ? connectionId : null;
    }

    public String getConnectedSince() {
        return cnx() != null ? connectedSince : null;
    }

    public int getPendingQueueSize() {
        return pendingMessages.messagesCount();
    }

    @Override
    public ProducerStatsRecorder getStats() {
        return stats;
    }

    @Override
    public String getProducerName() {
        return producerName;
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
        this.connectionHandler.setClientCnx(clientCnx);
    }

    void reconnectLater(Throwable exception) {
        this.connectionHandler.reconnectLater(exception);
    }

    void grabCnx() {
        this.connectionHandler.grabCnx();
    }

    @VisibleForTesting
    Optional<Semaphore> getSemaphore() {
        return semaphore;
    }

    @VisibleForTesting
    boolean isErrorStat() {
        return errorState;
    }

    @VisibleForTesting
    CompletableFuture<Void> getOriginalLastSendFuture() {
        CompletableFuture<MessageId> lastSendFuture = this.lastSendFuture;
        return lastSendFuture.thenApply(ignore -> null);
    }

    private static final Logger log = LoggerFactory.getLogger(ProducerImpl.class);
}
