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

import java.io.IOException;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.api.proto.CompressionType;
import org.apache.pulsar.common.compression.CompressionCodec;
import org.apache.pulsar.common.compression.CompressionCodecProvider;

// TODO: 10/23/23  批次发送消息的容器
/**
 * Batch message container framework.
 */
@Slf4j
public abstract class AbstractBatchMessageContainer implements BatchMessageContainerBase {

    // TODO: 10/23/23 压缩类型
    protected CompressionType compressionType;
    // TODO: 10/23/23 压缩器
    protected CompressionCodec compressor;
    protected String topicName;
    protected String producerName;
    // TODO: 10/23/23 生产者
    protected ProducerImpl producer;

    // TODO: 10/23/23 一批里面最大的消息数量 ，默认为1000
    protected int maxNumMessagesInBatch;
    // TODO: 10/23/23 一批里面最大的消息大小，默认为128k
    protected int maxBytesInBatch;
    // TODO: 10/23/23 消息调试记录
    protected int numMessagesInBatch = 0;
    // TODO: 10/23/23 当前批次的大小
    protected long currentBatchSizeBytes = 0;

    protected long currentTxnidMostBits = -1L;
    protected long currentTxnidLeastBits = -1L;

    protected static final int INITIAL_BATCH_BUFFER_SIZE = 1024;

    // This will be the largest size for a batch sent from this particular producer. This is used as a baseline to
    // allocate a new buffer that can hold the entire batch without needing costly reallocations
    protected int maxBatchSize = INITIAL_BATCH_BUFFER_SIZE;

    @Override
    public boolean haveEnoughSpace(MessageImpl<?> msg) {
        int messageSize = msg.getDataBuffer().readableBytes();
        // TODO: 10/23/23  maxBytesInBatch=128k，即当前批大小 + 消息大小 <= 128k && 消息数 < 1000
        return (
            (maxBytesInBatch <= 0 && (messageSize + currentBatchSizeBytes) <= ClientCnx.getMaxMessageSize())
            || (maxBytesInBatch > 0 && (messageSize + currentBatchSizeBytes) <= maxBytesInBatch)
        ) && (maxNumMessagesInBatch <= 0 || numMessagesInBatch < maxNumMessagesInBatch);
    }

    protected boolean isBatchFull() {
        // TODO: 10/23/23  maxBytesInBatch=128k, 当前的批大小  >= 128k || 消息数 >= 1000
        return (maxBytesInBatch > 0 && currentBatchSizeBytes >= maxBytesInBatch)
            || (maxBytesInBatch <= 0 && currentBatchSizeBytes >= ClientCnx.getMaxMessageSize())
            || (maxNumMessagesInBatch > 0 && numMessagesInBatch >= maxNumMessagesInBatch);
    }

    @Override
    public int getNumMessagesInBatch() {
        return numMessagesInBatch;
    }

    @Override
    public long getCurrentBatchSize() {
        return currentBatchSizeBytes;
    }

    @Override
    public List<ProducerImpl.OpSendMsg> createOpSendMsgs() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ProducerImpl.OpSendMsg createOpSendMsg() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setProducer(ProducerImpl<?> producer) {
        this.producer = producer;
        this.topicName = producer.getTopic();
        this.producerName = producer.getProducerName();
        this.compressionType = CompressionCodecProvider
                .convertToWireProtocol(producer.getConfiguration().getCompressionType());
        this.compressor = CompressionCodecProvider.getCompressionCodec(compressionType);
        this.maxNumMessagesInBatch = producer.getConfiguration().getBatchingMaxMessages();
        this.maxBytesInBatch = producer.getConfiguration().getBatchingMaxBytes();
    }

    @Override
    public boolean hasSameTxn(MessageImpl<?> msg) {
        if (!msg.getMessageBuilder().hasTxnidMostBits() || !msg.getMessageBuilder().hasTxnidLeastBits()) {
            return true;
        }
        if (currentTxnidMostBits == -1 || currentTxnidLeastBits == -1) {
            currentTxnidMostBits = msg.getMessageBuilder().getTxnidMostBits();
            currentTxnidLeastBits = msg.getMessageBuilder().getTxnidLeastBits();
            return true;
        }
        return currentTxnidMostBits == msg.getMessageBuilder().getTxnidMostBits()
                && currentTxnidLeastBits == msg.getMessageBuilder().getTxnidLeastBits();
    }
}
