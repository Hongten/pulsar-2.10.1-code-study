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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientException.NotSupportedException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TopicMetadata;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.client.impl.transaction.TransactionImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: 10/23/23 分区producer实现 。分区 Producer 的实现基本上跟无分区 Producer 实现类似，
//  只是少继承了一部分类，而且它就是使用了无分区 Producer 作为实际与 broker 通信的主体，
//  所以实现较为简单，主要在于处理多 无分区 Producer 协作和Topic 分区变更逻辑。
public class PartitionedProducerImpl<T> extends ProducerBase<T> {

    private static final Logger log = LoggerFactory.getLogger(PartitionedProducerImpl.class);

    // TODO: 10/23/23  每个分区一个无分区 Producer,即每个partition都对应着一个producer
    private final ConcurrentOpenHashMap<Integer, ProducerImpl<T>> producers;
    // TODO: 10/23/23 消息路由策略
    private final MessageRouter routerPolicy;
    // TODO: 10/23/23 Producer 状态记录器
    private final ProducerStatsRecorderImpl stats;
    // TODO: 10/23/23 topic元数据
    private TopicMetadata topicMetadata;
    private final int firstPartitionIndex;
    private String overrideProducerName;

    // TODO: 10/23/23 用于超时检查以及订阅分区变更增长消息 
    // timeout related to auto check and subscribe partition increasement
    private volatile Timeout partitionsAutoUpdateTimeout = null;
    // TODO: 10/23/23 Topic 分区变更监听器 
    TopicsPartitionChangedListener topicsPartitionChangedListener;
    // TODO: 10/23/23 分区自动更新future
    CompletableFuture<Void> partitionsAutoUpdateFuture = null;

    public PartitionedProducerImpl(PulsarClientImpl client, String topic, ProducerConfigurationData conf,
                                   int numPartitions, CompletableFuture<Producer<T>> producerCreatedFuture,
                                   Schema<T> schema, ProducerInterceptors interceptors) {
        super(client, topic, conf, producerCreatedFuture, schema, interceptors);
        // TODO: 2/24/23 最终的producers会放入到这个map里面
        this.producers =
                ConcurrentOpenHashMap.<Integer, ProducerImpl<T>>newBuilder().build();
        // TODO: 10/23/23 topic元数据信息只存放了partition个数
        this.topicMetadata = new TopicMetadataImpl(numPartitions);
        // TODO: 10/23/23 默认为轮询策略
        this.routerPolicy = getMessageRouter();
        stats = client.getConfiguration().getStatsIntervalSeconds() > 0 ? new ProducerStatsRecorderImpl() : null;

        // TODO: 10/23/23 分区topic配置，所有分区的最大缓存数量，默认为0，即生成一条立刻发送
        // MaxPendingMessagesAcrossPartitions doesn't support partial partition such as SinglePartition correctly
        int maxPendingMessages = Math.min(conf.getMaxPendingMessages(),
                conf.getMaxPendingMessagesAcrossPartitions() / numPartitions);
        conf.setMaxPendingMessages(maxPendingMessages);

        final List<Integer> indexList;
        if (conf.isLazyStartPartitionedProducers()
                && conf.getAccessMode() == ProducerAccessMode.Shared) {
            // try to create producer at least one partition
            indexList = Collections.singletonList(routerPolicy
                    .choosePartition(((TypedMessageBuilderImpl<T>) newMessage()).getMessage(), topicMetadata));
        } else {
            // try to create producer for all partitions
            indexList = IntStream.range(0, topicMetadata.numPartitions()).boxed().collect(Collectors.toList());
        }

        firstPartitionIndex = indexList.get(0);
        // TODO: 2/24/23  indexList 的size和 partition的size一样。 根据每个partition创建一个ProducerImpl实例
        start(indexList);

        // TODO: 10/23/23 启动自动监控 Topic 分区增长任务
        // start track and auto subscribe partition increasement
        if (conf.isAutoUpdatePartitions()) {
            // TODO: 10/23/23 topic分区变更监听器创建
            topicsPartitionChangedListener = new TopicsPartitionChangedListener();
            partitionsAutoUpdateTimeout = client.timer()
                .newTimeout(partitionsAutoUpdateTimerTask,
                        conf.getAutoUpdatePartitionsIntervalSeconds(), TimeUnit.SECONDS);
        }
    }

    private MessageRouter getMessageRouter() {
        MessageRouter messageRouter;

        // TODO: 10/23/23 默认为null
        MessageRoutingMode messageRouteMode = conf.getMessageRoutingMode();

        switch (messageRouteMode) {
            case CustomPartition:
                messageRouter = Objects.requireNonNull(conf.getCustomMessageRouter());
                break;
            case SinglePartition:
                messageRouter = new SinglePartitionMessageRouterImpl(
                        ThreadLocalRandom.current().nextInt(topicMetadata.numPartitions()), conf.getHashingScheme());
                break;
            case RoundRobinPartition:
            default:
                // TODO: 10/23/23 默认为轮询策略
                messageRouter = new RoundRobinPartitionMessageRouterImpl(
                        conf.getHashingScheme(),
                        ThreadLocalRandom.current().nextInt(topicMetadata.numPartitions()),
                        conf.isBatchingEnabled(),
                        TimeUnit.MICROSECONDS.toMillis(conf.batchingPartitionSwitchFrequencyIntervalMicros()));
        }

        return messageRouter;
    }

    @Override
    public String getProducerName() {
        return producers.get(firstPartitionIndex).getProducerName();
    }

    @Override
    public long getLastSequenceId() {
        // Return the highest sequence id across all partitions. This will be correct,
        // since there is a single id generator across all partitions for the same producer
        return producers.values().stream().map(Producer::getLastSequenceId).mapToLong(Long::longValue).max().orElse(-1);
    }

    // TODO: 2/24/23 根据每个partition创建一个ProducerImpl实例
    private void start(List<Integer> indexList) {
        // TODO: 2/24/23 indexList size = partition size
        AtomicReference<Throwable> createFail = new AtomicReference<Throwable>();
        AtomicInteger completed = new AtomicInteger();

        final BiConsumer<Boolean, Throwable> afterCreatingProducer = (failFast, createException) -> {
            final Runnable closeRunnable = () -> {
                // TODO: 2/24/23 run()
                // TODO: 10/23/23 否则创建失败，返回创建异常，并且关闭已创建成功的无分区 Producer
                log.error("[{}] Could not create partitioned producer.", topic, createFail.get().getCause());
                closeAsync().handle((ok, closeException) -> {
                    producerCreatedFuture().completeExceptionally(createFail.get());
                    client.cleanupProducer(this);
                    return null;
                });
            };

            // TODO: 2/24/23 如果有异常，进行异常处理
            if (createException != null) {
                setState(State.Failed);
                createFail.compareAndSet(null, createException);
                if (failFast) {
                    closeRunnable.run();
                }
            }
            // we mark success if all the partitions are created
            // successfully, else we throw an exception
            // due to any
            // failure in one of the partitions and close the successfully
            // created partitions
            if (completed.incrementAndGet() == indexList.size()) {
                // TODO: 2/24/23 只有当全部producer创建成功，才算成功
                if (createFail.get() == null) {
                    // TODO: 10/23/23  如果这里为null，则意味着分区 Producer 创建成功，并设置State.Ready
                    setState(State.Ready);
                    log.info("[{}] Created partitioned producer", topic);
                    producerCreatedFuture().complete(PartitionedProducerImpl.this);
                } else {
                    closeRunnable.run();
                }
            }
        };

        // TODO: 10/23/23 先创建第一个producer，然后再创建剩下的producer。创建的producer是ProducerImpl实例 
        // TODO: 2/24/23 创建第一个producer
        final ProducerImpl<T> firstProducer = createProducer(indexList.get(0));
        firstProducer.producerCreatedFuture().handle((prod, createException) -> {
            afterCreatingProducer.accept(true, createException);
            if (createException != null) {
                throw new RuntimeException(createException);
            }
            overrideProducerName = firstProducer.getProducerName();
            return Optional.of(overrideProducerName);
        }).thenApply(name -> {
            // TODO: 2/24/23 创建剩余的partition size-1 的producer
            for (int i = 1; i < indexList.size(); i++) {
                createProducer(indexList.get(i), name).producerCreatedFuture().handle((prod, createException) -> {
                    afterCreatingProducer.accept(false, createException);
                    return null;
                });
            }
            return null;
        });
    }

    private ProducerImpl<T> createProducer(final int partitionIndex) {
        return createProducer(partitionIndex, Optional.empty());
    }

    private ProducerImpl<T> createProducer(final int partitionIndex, final Optional<String> overrideProducerName) {
        return producers.computeIfAbsent(partitionIndex, (idx) -> {
            // TODO: 2/24/23 fullname-partition-x
            String partitionName = TopicName.get(topic).getPartition(idx).toString();
            // TODO: 2/24/23 创建当个的ProducerImpl
            return client.newProducerImpl(partitionName, idx,
                    conf, schema, interceptors, new CompletableFuture<>(), overrideProducerName);
        });
    }

    @Override
    CompletableFuture<MessageId> internalSendAsync(Message<?> message) {
        return internalSendWithTxnAsync(message, null);
    }

    @Override
    CompletableFuture<MessageId> internalSendWithTxnAsync(Message<?> message, Transaction txn) {
        CompletableFuture<MessageId> completableFuture = new CompletableFuture<>();
        if (txn != null && !((TransactionImpl) txn).checkIfOpen(completableFuture)) {
            return completableFuture;
        }
        // TODO: 10/23/23 通过路由策略，选择消息发送到哪个分区
        int partition = routerPolicy.choosePartition(message, topicMetadata);
        checkArgument(partition >= 0 && partition < topicMetadata.numPartitions(),
                "Illegal partition index chosen by the message routing policy: " + partition);

        // TODO: 10/23/23 如果是延迟创建producer，那么测试就可以创建剩余的producer了
        if (conf.isLazyStartPartitionedProducers() && !producers.containsKey(partition)) {
            final ProducerImpl<T> newProducer = createProducer(partition, Optional.ofNullable(overrideProducerName));
            final State createState = newProducer.producerCreatedFuture().handle((prod, createException) -> {
                if (createException != null) {
                    log.error("[{}] Could not create internal producer. partitionIndex: {}", topic, partition,
                            createException);
                    try {
                        producers.remove(partition, newProducer);
                        newProducer.close();
                    } catch (PulsarClientException e) {
                        log.error("[{}] Could not close internal producer. partitionIndex: {}", topic, partition, e);
                    }
                    return State.Failed;
                }
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Created internal producer. partitionIndex: {}", topic, partition);
                }
                return State.Ready;
            }).join();
            if (createState == State.Failed) {
                return FutureUtil.failedFuture(new PulsarClientException.NotConnectedException());
            }
        }

        switch (getState()) {
            case Ready:
            case Connecting:
                break; // Ok
            case Closing:
            case Closed:
                return FutureUtil.failedFuture(
                        new PulsarClientException.AlreadyClosedException("Producer already closed"));
            case ProducerFenced:
                return FutureUtil.failedFuture(
                        new PulsarClientException.ProducerFencedException("Producer was fenced"));
            case Terminated:
                return FutureUtil.failedFuture(
                        new PulsarClientException.TopicTerminatedException("Topic was terminated"));
            case Failed:
            case Uninitialized:
                return FutureUtil.failedFuture(new PulsarClientException.NotConnectedException());
        }

        // TODO: 10/23/23 根据分区索引来获取 Producer ，然后发消息到此无分区 Producer 中，达到增加吞吐量目的
        // TODO: 10/23/23 至此，分区 Producer 发送消息流程已经明朗，它的绝大多数实现都在无分区 Producer 实现，主要改进在于，
        //  Topic 分成 N 个分区，每个分区使用无分区 Producer 与broker通信，这意味着，只要网络不是瓶颈的情况下，
        //  与无分区Topic相比，理论上会提供N倍吞吐量。
        return producers.get(partition).internalSendWithTxnAsync(message, txn);
    }

    @Override
    public CompletableFuture<Void> flushAsync() {
        return CompletableFuture.allOf(
                producers.values().stream().map(ProducerImpl::flushAsync).toArray(CompletableFuture[]::new));
    }

    @Override
    void triggerFlush() {
        producers.values().forEach(ProducerImpl::triggerFlush);
    }

    @Override
    public boolean isConnected() {
        // returns false if any of the partition is not connected
        return producers.values().stream().allMatch(ProducerImpl::isConnected);
    }

    @Override
    public long getLastDisconnectedTimestamp() {
        long lastDisconnectedTimestamp = 0;
        Optional<ProducerImpl<T>> p = producers.values().stream()
                .max(Comparator.comparingLong(ProducerImpl::getLastDisconnectedTimestamp));
        if (p.isPresent()) {
            lastDisconnectedTimestamp = p.get().getLastDisconnectedTimestamp();
        }
        return lastDisconnectedTimestamp;
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        if (getState() == State.Closing || getState() == State.Closed) {
            return CompletableFuture.completedFuture(null);
        }
        setState(State.Closing);

        if (partitionsAutoUpdateTimeout != null) {
            partitionsAutoUpdateTimeout.cancel();
            partitionsAutoUpdateTimeout = null;
        }

        AtomicReference<Throwable> closeFail = new AtomicReference<Throwable>();
        AtomicInteger completed = new AtomicInteger((int) producers.size());
        CompletableFuture<Void> closeFuture = new CompletableFuture<>();
        for (Producer<T> producer : producers.values()) {
            if (producer != null) {
                producer.closeAsync().handle((closed, ex) -> {
                    if (ex != null) {
                        closeFail.compareAndSet(null, ex);
                    }
                    if (completed.decrementAndGet() == 0) {
                        if (closeFail.get() == null) {
                            setState(State.Closed);
                            closeFuture.complete(null);
                            log.info("[{}] Closed Partitioned Producer", topic);
                            client.cleanupProducer(this);
                        } else {
                            setState(State.Failed);
                            closeFuture.completeExceptionally(closeFail.get());
                            log.error("[{}] Could not close Partitioned Producer", topic, closeFail.get().getCause());
                        }
                    }

                    return null;
                });
            }

        }

        return closeFuture;
    }

    @Override
    public synchronized ProducerStatsRecorderImpl getStats() {
        if (stats == null) {
            return null;
        }
        stats.reset();
        producers.values().forEach(p -> stats.updateCumulativeStats(p.getStats()));
        return stats;
    }

    public List<ProducerImpl<T>> getProducers() {
        return producers.values().stream()
                .sorted(Comparator.comparingInt(e -> TopicName.getPartitionIndex(e.getTopic())))
                .collect(Collectors.toList());
    }

    @Override
    String getHandlerName() {
        return "partition-producer";
    }

    // TODO: 10/23/23 实现分区变更监听器接口， 当Topic的分区数变更时，此监听器将被触发
    // This listener is triggered when topics partitions are updated.
    private class TopicsPartitionChangedListener implements PartitionsChangedListener {
        // TODO: 10/23/23 实现topic分区变更方法， 检查 Topic 分区的与过去的改变，增加新的 Topic 的分区（目前只支持新增分区）
        // Check partitions changes of passed in topics, and add new topic partitions.
        @Override
        public CompletableFuture<Void> onTopicsExtended(Collection<String> topicsExtended) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            // TODO: 10/23/23 预防性检查
            if (topicsExtended.isEmpty() || !topicsExtended.contains(topic)) {
                future.complete(null);
                return future;
            }

            client.getPartitionsForTopic(topic).thenCompose(list -> {
                // TODO: 10/23/23 之前的topic的partition数量，从topic的元数据信息中获取
                int oldPartitionNumber = topicMetadata.numPartitions();
                // TODO: 10/23/23 当前的topic的partition数量
                int currentPartitionNumber = list.size();

                if (log.isDebugEnabled()) {
                    log.debug("[{}] partitions number. old: {}, new: {}",
                            topic, oldPartitionNumber, currentPartitionNumber);
                }

                // TODO: 10/23/23 表示没有分区变化， 则结束本次
                if (oldPartitionNumber == currentPartitionNumber) {
                    // topic partition number not changed
                    future.complete(null);
                    return future;
                    // TODO: 10/23/23 表示扩分区
                } else if (oldPartitionNumber < currentPartitionNumber) {
                    // TODO: 10/23/23 延迟创建producer && 是shared模式
                    if (conf.isLazyStartPartitionedProducers() && conf.getAccessMode() == ProducerAccessMode.Shared) {
                        // TODO: 10/23/23 topic元数据信息更新，更新topic的partition 数量
                        topicMetadata = new TopicMetadataImpl(currentPartitionNumber);
                        future.complete(null);
                        // call interceptor with the metadata change
                        onPartitionsChange(topic, currentPartitionNumber);
                        return future;
                    } else {
                        List<CompletableFuture<Producer<T>>> futureList = list
                                .subList(oldPartitionNumber, currentPartitionNumber)
                                .stream()// TODO: 10/23/23 找出新增的分区
                                .map(partitionName -> {
                                    // TODO: 10/23/23 获取索引号
                                    int partitionIndex = TopicName.getPartitionIndex(partitionName);
                                    // TODO: 10/23/23 创建对应的producer
                                    return producers.computeIfAbsent(partitionIndex, (idx) -> new ProducerImpl<>(
                                            client, partitionName, conf, new CompletableFuture<>(),
                                            idx, schema, interceptors,
                                            Optional.ofNullable(overrideProducerName))).producerCreatedFuture();
                                }).collect(Collectors.toList());

                        // TODO: 10/23/23 等待所有的新的生产者创建成功
                        FutureUtil.waitForAll(futureList)
                                .thenAccept(finalFuture -> {
                                    if (log.isDebugEnabled()) {
                                        log.debug(
                                                "[{}] success create producers for extended partitions."
                                                        + " old: {}, new: {}",
                                                topic, oldPartitionNumber, currentPartitionNumber);
                                    }
                                    // TODO: 10/23/23 更新topic元数据信息
                                    topicMetadata = new TopicMetadataImpl(currentPartitionNumber);
                                    future.complete(null);
                                })
                                .exceptionally(ex -> {
                                    // TODO: 10/23/23 如果有异常发生，则关闭新创建的生产者，并移除相关资源，等待下一次检查的到来
                                    // error happened, remove
                                    log.warn("[{}] fail create producers for extended partitions. old: {}, new: {}",
                                            topic, oldPartitionNumber, currentPartitionNumber);
                                    IntStream.range(oldPartitionNumber, (int) producers.size())
                                            .forEach(i -> producers.remove(i).closeAsync());
                                    future.completeExceptionally(ex);
                                    return null;
                                });
                        // call interceptor with the metadata change
                        onPartitionsChange(topic, currentPartitionNumber);
                        return null;
                    }
                } else {
                    // TODO: 10/23/23 不支持分区缩容
                    log.error("[{}] not support shrink topic partitions. old: {}, new: {}",
                            topic, oldPartitionNumber, currentPartitionNumber);
                    future.completeExceptionally(new NotSupportedException("not support shrink topic partitions"));
                }
                return future;
            }).exceptionally(throwable -> {
                log.error("[{}] Auto getting partitions failed", topic, throwable);
                future.completeExceptionally(throwable);
                return null;
            });

            return future;
        }
    }

    // TODO: 10/23/23 topic分区自动变更检查器
    private TimerTask partitionsAutoUpdateTimerTask = new TimerTask() {
        @Override
        public void run(Timeout timeout) throws Exception {
            try {
                if (timeout.isCancelled() || getState() != State.Ready) {
                    return;
                }

                if (log.isDebugEnabled()) {
                    log.debug("[{}] run partitionsAutoUpdateTimerTask for partitioned producer", topic);
                }

                // if last auto update not completed yet, do nothing.
                if (partitionsAutoUpdateFuture == null || partitionsAutoUpdateFuture.isDone()) {
                    // TODO: 10/23/23 topic分区变更监听器
                    partitionsAutoUpdateFuture =
                            topicsPartitionChangedListener.onTopicsExtended(ImmutableList.of(topic));
                }
            } catch (Throwable th) {
                log.warn("Encountered error in partition auto update timer task for partition producer."
                        + " Another task will be scheduled.", th);
            } finally {
                // TODO: 10/23/23 设置下一个周期的检查
                // schedule the next re-check task
                partitionsAutoUpdateTimeout = client.timer()
                        .newTimeout(partitionsAutoUpdateTimerTask,
                                conf.getAutoUpdatePartitionsIntervalSeconds(), TimeUnit.SECONDS);
            }
        }
    };

    @VisibleForTesting
    public CompletableFuture<Void> getPartitionsAutoUpdateFuture() {
        return partitionsAutoUpdateFuture;
    }

    @VisibleForTesting
    public Timeout getPartitionsAutoUpdateTimeout() {
        return partitionsAutoUpdateTimeout;
    }

    @VisibleForTesting
    public CompletableFuture<Void> getOriginalLastSendFuture() {
        return CompletableFuture.allOf(
                producers.values().stream().map(ProducerImpl::getOriginalLastSendFuture)
                        .toArray(CompletableFuture[]::new));
    }

    @Override
    public int getNumOfPartitions() {
        return topicMetadata.numPartitions();
    }

}
