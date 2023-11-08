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
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import io.netty.util.concurrent.FastThreadLocal;
import java.io.Closeable;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: 11/2/23 未确认消息跟踪器， 每个消息接收后首先都要放入此容器里，然后其监控是否消费超时，
//  如果超时，则移除并重新通知 broker 推送过来，
//  而且还会跟踪消息未消费次数，如果超过次数，则会放入死信队列，然后发送系统设置的死信Topic ，
//  另外此消息会被系统确认“消费”（意味着不再推送重新消费了，要消息只能到死信 Topic 里取）。

public class UnAckedMessageTracker implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(UnAckedMessageTracker.class);

    // TODO: 11/6/23 消息ID-消息集（为了更好的针对消息ID索引）
    protected final HashMap<MessageId, HashSet<MessageId>> messageIdPartitionMap;
    // TODO: 11/6/23 双向列表 用于时间分区
    protected final ArrayDeque<HashSet<MessageId>> timePartitions;

    // TODO: 11/6/23 读写锁
    protected final Lock readLock;
    protected final Lock writeLock;

    // TODO: 11/6/23 UnAckedMessageTracker 的空实现，用于非持久化消息组件
    public static final UnAckedMessageTrackerDisabled UNACKED_MESSAGE_TRACKER_DISABLED =
            new UnAckedMessageTrackerDisabled();
    // TODO: 11/6/23 确认超时时间
    protected final long ackTimeoutMillis;
    // TODO: 11/6/23 滴答时间（或者叫单位时间）
    protected final long tickDurationInMs;

    private static class UnAckedMessageTrackerDisabled extends UnAckedMessageTracker {
        @Override
        public void clear() {
        }

        @Override
        long size() {
            return 0;
        }

        @Override
        public boolean add(MessageId m) {
            return true;
        }

        @Override
        public boolean add(MessageId messageId, int redeliveryCount) {
            return true;
        }

        @Override
        public boolean remove(MessageId m) {
            return true;
        }

        @Override
        public int removeMessagesTill(MessageId msgId) {
            return 0;
        }

        @Override
        public void close() {
        }
    }

    protected Timeout timeout;

    public UnAckedMessageTracker() {
        readLock = null;
        writeLock = null;
        timePartitions = null;
        messageIdPartitionMap = null;
        this.ackTimeoutMillis = 0;
        this.tickDurationInMs = 0;
    }

    protected static final FastThreadLocal<HashSet<MessageId>> TL_MESSAGE_IDS_SET =
            new FastThreadLocal<HashSet<MessageId>>() {
        @Override
        protected HashSet<MessageId> initialValue() throws Exception {
            return new HashSet<>();
        }
    };

    public UnAckedMessageTracker(PulsarClientImpl client, ConsumerBase<?> consumerBase,
                                 ConsumerConfigurationData<?> conf) {
        this.ackTimeoutMillis = conf.getAckTimeoutMillis();
        this.tickDurationInMs = Math.min(conf.getTickDurationMillis(), conf.getAckTimeoutMillis());
        checkArgument(tickDurationInMs > 0 && ackTimeoutMillis >= tickDurationInMs);
        // TODO: 11/6/23 读写锁
        ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
        this.readLock = readWriteLock.readLock();
        this.writeLock = readWriteLock.writeLock();
        if (conf.getAckTimeoutRedeliveryBackoff() == null) {
            this.messageIdPartitionMap = new HashMap<>();
            this.timePartitions = new ArrayDeque<>();

            // TODO: 11/6/23 设置一个时间分区，假设确认超时时间为11000ms，滴答时间为2000ms，此时计算结果为5 
            int blankPartitions = (int) Math.ceil((double) this.ackTimeoutMillis / this.tickDurationInMs);
            // TODO: 11/6/23 创建6个时间分区容器 
            for (int i = 0; i < blankPartitions + 1; i++) {
                timePartitions.add(new HashSet<>(16, 1));
            }
            // TODO: 11/6/23  新增一个定时器任务，tickDurationInMs 为轮询间隔，也就是这里会处理消息消费超时，
            //  并通知 broker  进行重新投递 ，这里 UnAckedMessageTracker 最核心的处理
            timeout = client.timer().newTimeout(new TimerTask() {
                @Override
                public void run(Timeout t) throws Exception {
                    Set<MessageId> messageIds = TL_MESSAGE_IDS_SET.get();
                    messageIds.clear();

                    writeLock.lock();
                    try {
                        // TODO: 11/6/23 获取队列头部 ，如果容器内有消息，此时就认为过期了，把过期消息移除，并保留下来，
                        //  为接下来通知 broker 重发这些消息打下基础
                        HashSet<MessageId> headPartition = timePartitions.removeFirst();
                        if (!headPartition.isEmpty()) {
                            log.info("[{}] {} messages will be re-delivered", consumerBase, headPartition.size());
                            // TODO: 11/6/23
                            headPartition.forEach(messageId -> {
                                addChunkedMessageIdsAndRemoveFromSequenceMap(messageId, messageIds, consumerBase);
                                // TODO: 11/6/23 过期的msgID 集合
                                messageIds.add(messageId);
                                messageIdPartitionMap.remove(messageId);
                            });
                        }

                        headPartition.clear();
                        timePartitions.addLast(headPartition);
                    } finally {
                        writeLock.unlock();
                        // TODO: 11/6/23 如果有过期消息，则通知 broker 重新投递消息
                        if (messageIds.size() > 0) {
                            consumerBase.onAckTimeoutSend(messageIds);
                            consumerBase.redeliverUnacknowledgedMessages(messageIds);
                        }
                        // TODO: 11/6/23 准备下一次任务定时
                        timeout = client.timer().newTimeout(this, tickDurationInMs, TimeUnit.MILLISECONDS);
                    }
                }
            }, this.tickDurationInMs, TimeUnit.MILLISECONDS);
        } else {
            this.messageIdPartitionMap = null;
            this.timePartitions = null;
        }
    }

    public static void addChunkedMessageIdsAndRemoveFromSequenceMap(MessageId messageId, Set<MessageId> messageIds,
                                                                    ConsumerBase<?> consumerBase) {
        if (messageId instanceof MessageIdImpl) {
            MessageIdImpl[] chunkedMsgIds = consumerBase.unAckedChunkedMessageIdSequenceMap
                    .get((MessageIdImpl) messageId);
            if (chunkedMsgIds != null && chunkedMsgIds.length > 0) {
                Collections.addAll(messageIds, chunkedMsgIds);
            }
            consumerBase.unAckedChunkedMessageIdSequenceMap.remove((MessageIdImpl) messageId);
        }
    }

    // TODO: 11/6/23 清空容器
    public void clear() {
        writeLock.lock();
        try {
            messageIdPartitionMap.clear();
            timePartitions.forEach(tp -> tp.clear());
        } finally {
            writeLock.unlock();
        }
    }

    // TODO: 11/6/23 添加消息
    public boolean add(MessageId messageId) {
        writeLock.lock();
        try {
            // TODO: 11/6/23 从尾部获取一个哈希set，并把它放入message map，再把消息放入哈希set
            HashSet<MessageId> partition = timePartitions.peekLast();
            HashSet<MessageId> previousPartition = messageIdPartitionMap.putIfAbsent(messageId, partition);
            if (previousPartition == null) {
                return partition.add(messageId);
            } else {
                return false;
            }
        } finally {
            writeLock.unlock();
        }
    }

    public boolean add(MessageId messageId, int redeliveryCount) {
        return add(messageId);
    }

    // TODO: 11/6/23 是否为空
    boolean isEmpty() {
        readLock.lock();
        try {
            return messageIdPartitionMap.isEmpty();
        } finally {
            readLock.unlock();
        }
    }

    // TODO: 11/6/23 移除某消息ID
    public boolean remove(MessageId messageId) {
        writeLock.lock();
        try {
            boolean removed = false;
            HashSet<MessageId> exist = messageIdPartitionMap.remove(messageId);
            if (exist != null) {
                removed = exist.remove(messageId);
            }
            return removed;
        } finally {
            writeLock.unlock();
        }
    }

    long size() {
        readLock.lock();
        try {
            return messageIdPartitionMap.size();
        } finally {
            readLock.unlock();
        }
    }

    // TODO: 11/6/23 移除比MessageId小的所有消息
    public int removeMessagesTill(MessageId msgId) {
        writeLock.lock();
        try {
            int removed = 0;
            Iterator<Entry<MessageId, HashSet<MessageId>>> iterator = messageIdPartitionMap.entrySet().iterator();
            while (iterator.hasNext()) {
                Entry<MessageId, HashSet<MessageId>> entry = iterator.next();
                MessageId messageId = entry.getKey();
                // TODO: 11/6/23 比较方法
                if (messageId.compareTo(msgId) <= 0) {
                    entry.getValue().remove(messageId);
                    iterator.remove();
                    removed++;
                }
            }
            return removed;
        } finally {
            writeLock.unlock();
        }
    }

    private void stop() {
        writeLock.lock();
        try {
            if (timeout != null && !timeout.isCancelled()) {
                timeout.cancel();
                timeout = null;
            }
            this.clear();
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void close() {
        stop();
    }
}
