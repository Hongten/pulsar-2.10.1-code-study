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

import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.ProducerImpl.OpSendMsg;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.proto.CompressionType;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.ByteBufPair;
import org.apache.pulsar.common.protocol.Commands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: 10/23/23 默认的批量消息容器 
/**
 * Default batch message container.
 *
 * incoming single messages:
 * (k1, v1), (k2, v1), (k3, v1), (k1, v2), (k2, v2), (k3, v2), (k1, v3), (k2, v3), (k3, v3)
 *
 * batched into single batch message:
 * [(k1, v1), (k2, v1), (k3, v1), (k1, v2), (k2, v2), (k3, v2), (k1, v3), (k2, v3), (k3, v3)]
 */
class BatchMessageContainerImpl extends AbstractBatchMessageContainer {

    // TODO: 10/23/23 消息的metadata 
    private MessageMetadata messageMetadata = new MessageMetadata();
    // sequence id for this batch which will be persisted as a single entry by broker
    private long lowestSequenceId = -1L;
    private long highestSequenceId = -1L;
    // TODO: 10/23/23 消息的metadata和消息 字节 
    private ByteBuf batchedMessageMetadataAndPayload;
    // TODO: 10/23/23 消息集合 
    private List<MessageImpl<?>> messages = new ArrayList<>();
    protected SendCallback previousCallback = null;
    // keep track of callbacks for individual messages being published in a batch
    protected SendCallback firstCallback;

    // TODO: 10/23/23 用于批量消息时
    @Override
    public boolean add(MessageImpl<?> msg, SendCallback callback) {

        if (log.isDebugEnabled()) {
            log.debug("[{}] [{}] add message to batch, num messages in batch so far {}", topicName, producerName,
                    numMessagesInBatch);
        }

        // TODO: 10/23/23 容器内，第一条消息进来执行初始化操作
        if (++numMessagesInBatch == 1) {
            try {
                // TODO: 10/23/23 批量消息中，不同消息的一些属性是公共的，所以这里我们以第一条消息的一些数据作为初始化数据，并初始化序列ID
                // some properties are common amongst the different messages in the batch, hence we just pick it up from
                // the first message
                messageMetadata.setSequenceId(msg.getSequenceId());
                lowestSequenceId = Commands.initBatchMessageMetadata(messageMetadata, msg.getMessageBuilder());
                this.firstCallback = callback;
                // TODO: 10/23/23 分配批量消息缓冲区
                batchedMessageMetadataAndPayload = PulsarByteBufAllocator.DEFAULT
                        .buffer(Math.min(maxBatchSize, ClientCnx.getMaxMessageSize()));
                if (msg.getMessageBuilder().hasTxnidMostBits() && currentTxnidMostBits == -1) {
                    currentTxnidMostBits = msg.getMessageBuilder().getTxnidMostBits();
                }
                if (msg.getMessageBuilder().hasTxnidLeastBits() && currentTxnidLeastBits == -1) {
                    currentTxnidLeastBits = msg.getMessageBuilder().getTxnidLeastBits();
                }
            } catch (Throwable e) {
                log.error("construct first message failed, exception is ", e);
                if (batchedMessageMetadataAndPayload != null) {
                    // if payload has been allocated release it
                    batchedMessageMetadataAndPayload.release();
                }
                discard(new PulsarClientException(e));
                return false;
            }
        }

        // TODO: 10/23/23 这里构建一个回调链，这回调链可参考 SendCallback 接口
        if (previousCallback != null) {
            previousCallback.addCallback(msg, callback);
        }
        previousCallback = callback;
        // TODO: 10/23/23 记录当前消息的大小，然后累计历史消息大小
        currentBatchSizeBytes += msg.getDataBuffer().readableBytes();
        // TODO: 10/23/23  把当前消息放入消息队列
        messages.add(msg);

        if (lowestSequenceId == -1L) {
            lowestSequenceId = msg.getSequenceId();
            messageMetadata.setSequenceId(lowestSequenceId);
        }
        highestSequenceId = msg.getSequenceId();
        ProducerImpl.LAST_SEQ_ID_PUSHED_UPDATER.getAndUpdate(producer, prev -> Math.max(prev, msg.getSequenceId()));

        return isBatchFull();
    }

    private ByteBuf getCompressedBatchMetadataAndPayload() {
        int batchWriteIndex = batchedMessageMetadataAndPayload.writerIndex();
        int batchReadIndex = batchedMessageMetadataAndPayload.readerIndex();

        for (int i = 0, n = messages.size(); i < n; i++) {
            MessageImpl<?> msg = messages.get(i);
            msg.getDataBuffer().markReaderIndex();
            try {
                batchedMessageMetadataAndPayload = Commands.serializeSingleMessageInBatchWithPayload(
                        msg.getMessageBuilder(), msg.getDataBuffer(), batchedMessageMetadataAndPayload);
            } catch (Throwable th) {
                // serializing batch message can corrupt the index of message and batch-message. Reset the index so,
                // next iteration doesn't send corrupt message to broker.
                for (int j = 0; j <= i; j++) {
                    MessageImpl<?> previousMsg = messages.get(j);
                    previousMsg.getDataBuffer().resetReaderIndex();
                }
                batchedMessageMetadataAndPayload.writerIndex(batchWriteIndex);
                batchedMessageMetadataAndPayload.readerIndex(batchReadIndex);
                throw new RuntimeException(th);
            }
        }

        int uncompressedSize = batchedMessageMetadataAndPayload.readableBytes();
        ByteBuf compressedPayload = compressor.encode(batchedMessageMetadataAndPayload);
        batchedMessageMetadataAndPayload.release();
        if (compressionType != CompressionType.NONE) {
            messageMetadata.setCompression(compressionType);
            messageMetadata.setUncompressedSize(uncompressedSize);
        }

        // Update the current max batch size using the uncompressed size, which is what we need in any case to
        // accumulate the batch content
        maxBatchSize = Math.max(maxBatchSize, uncompressedSize);
        return compressedPayload;
    }

    @Override
    public void clear() {
        messages = new ArrayList<>();
        firstCallback = null;
        previousCallback = null;
        messageMetadata.clear();
        numMessagesInBatch = 0;
        currentBatchSizeBytes = 0;
        lowestSequenceId = -1L;
        highestSequenceId = -1L;
        batchedMessageMetadataAndPayload = null;
        currentTxnidMostBits = -1L;
        currentTxnidLeastBits = -1L;
    }

    @Override
    public boolean isEmpty() {
        return messages.isEmpty();
    }

    @Override
    public void discard(Exception ex) {
        try {
            // Need to protect ourselves from any exception being thrown in the future handler from the application
            if (firstCallback != null) {
                firstCallback.sendComplete(ex);
            }
        } catch (Throwable t) {
            log.warn("[{}] [{}] Got exception while completing the callback for msg {}:", topicName, producerName,
                    lowestSequenceId, t);
        }
        clear();
    }

    @Override
    public boolean isMultiBatches() {
        return false;
    }

    @Override
    public OpSendMsg createOpSendMsg() throws IOException {
        // TODO: 10/23/23 加密消息
        ByteBuf encryptedPayload = producer.encryptMessage(messageMetadata, getCompressedBatchMetadataAndPayload());
        if (encryptedPayload.readableBytes() > ClientCnx.getMaxMessageSize()) {
            discard(new PulsarClientException.InvalidMessageException(
                    "Message size is bigger than " + ClientCnx.getMaxMessageSize() + " bytes"));
            return null;
        }
        // TODO: 10/23/23 消息数
        messageMetadata.setNumMessagesInBatch(numMessagesInBatch);
        messageMetadata.setHighestSequenceId(highestSequenceId);
        if (currentTxnidMostBits != -1) {
            messageMetadata.setTxnidMostBits(currentTxnidMostBits);
        }
        if (currentTxnidLeastBits != -1) {
            messageMetadata.setTxnidLeastBits(currentTxnidLeastBits);
        }
        // TODO: 10/23/23 发送消息命名
        ByteBufPair cmd = producer.sendMessage(producer.producerId, messageMetadata.getSequenceId(),
                messageMetadata.getHighestSequenceId(), numMessagesInBatch, messageMetadata, encryptedPayload);

        // TODO: 10/23/23 创建发送消息操作
        OpSendMsg op = OpSendMsg.create(messages, cmd, messageMetadata.getSequenceId(),
                messageMetadata.getHighestSequenceId(), firstCallback);

        op.setNumMessagesInBatch(numMessagesInBatch);
        op.setBatchSizeByte(currentBatchSizeBytes);
        lowestSequenceId = -1L;
        return op;
    }

    @Override
    public boolean hasSameSchema(MessageImpl<?> msg) {
        if (numMessagesInBatch == 0) {
            return true;
        }
        if (!messageMetadata.hasSchemaVersion()) {
            return msg.getSchemaVersion() == null;
        }
        return Arrays.equals(msg.getSchemaVersion(), messageMetadata.getSchemaVersion());
    }

    private static final Logger log = LoggerFactory.getLogger(BatchMessageContainerImpl.class);
}
