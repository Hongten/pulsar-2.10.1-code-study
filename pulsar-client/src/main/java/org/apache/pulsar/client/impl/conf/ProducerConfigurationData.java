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

import static com.google.common.base.Preconditions.checkArgument;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Sets;
import java.io.Serializable;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.BatcherBuilder;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.MessageCrypto;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.ProducerCryptoFailureAction;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ProducerConfigurationData implements Serializable, Cloneable {

    private static final long serialVersionUID = 1L;

    // TODO: 2/24/23 默认batch消息条数
    public static final int DEFAULT_BATCHING_MAX_MESSAGES = 1000;
    public static final int DEFAULT_MAX_PENDING_MESSAGES = 0;
    public static final int DEFAULT_MAX_PENDING_MESSAGES_ACROSS_PARTITIONS = 0;
    // TODO: 2/24/23 topic name
    private String topicName = null;
    // TODO: 2/24/23 producer name
    private String producerName = null;
    // TODO: 2/24/23 发送超时时间，默认30s
    private long sendTimeoutMs = 30000;
    // TODO: 2/24/23 达到最大缓存是否block客户端，默认是false
    private boolean blockIfQueueFull = false;
    // TODO: 2/24/23 最大缓存的生成请求数，默认为0，即生成一条立刻发送
    private int maxPendingMessages = DEFAULT_MAX_PENDING_MESSAGES;
    // TODO: 2/24/23 分区topic配置，所有分区的最大缓存数量，默认为0，即生成一条立刻发送
    private int maxPendingMessagesAcrossPartitions = DEFAULT_MAX_PENDING_MESSAGES_ACROSS_PARTITIONS;
    private MessageRoutingMode messageRoutingMode = null;
    private HashingScheme hashingScheme = HashingScheme.JavaStringHash;

    private ProducerCryptoFailureAction cryptoFailureAction = ProducerCryptoFailureAction.FAIL;

    @JsonIgnore
    private MessageRouter customMessageRouter = null;

    // TODO: 2/24/23 batch的最大等待时间
    private long batchingMaxPublishDelayMicros = TimeUnit.MILLISECONDS.toMicros(1);
    // TODO: 2/24/23 开启batch时，分区切换评率
    private int batchingPartitionSwitchFrequencyByPublishDelay = 10;
    // TODO: 10/23/23 默认1000条msg
    private int batchingMaxMessages = DEFAULT_BATCHING_MAX_MESSAGES;
    // TODO: 10/23/23 最大的批大小
    private int batchingMaxBytes = 128 * 1024; // 128KB (keep the maximum consistent as previous versions)
    // TODO: 2/24/23 客户端默认开启了batch 发送数据
    private boolean batchingEnabled = true; // enabled by default
    @JsonIgnore
    private BatcherBuilder batcherBuilder = BatcherBuilder.DEFAULT;
    // TODO: 2/24/23 是否开启chunking特性，默认为false
    private boolean chunkingEnabled = false;

    @JsonIgnore
    private CryptoKeyReader cryptoKeyReader;

    @JsonIgnore
    private transient MessageCrypto messageCrypto = null;

    @JsonIgnore
    private Set<String> encryptionKeys = new TreeSet<>();

    // TODO: 2/24/23 压缩类型，默认为NONE
    private CompressionType compressionType = CompressionType.NONE;

    // Cannot use Optional<Long> since it's not serializable
    private Long initialSequenceId = null;

    // TODO: 10/23/23 默认情况下，自动partition变更打开
    private boolean autoUpdatePartitions = true;

    // TODO: 10/23/23 监听partition变更时间为默认60s
    private long autoUpdatePartitionsIntervalSeconds = 60;

    private boolean multiSchema = true;

    // TODO: 2/24/23 默认为Shared
    private ProducerAccessMode accessMode = ProducerAccessMode.Shared;

    private boolean lazyStartPartitionedProducers = false;

    private SortedMap<String, String> properties = new TreeMap<>();

    private String initialSubscriptionName = null;

    /**
     *
     * Returns true if encryption keys are added.
     *
     */
    @JsonIgnore
    public boolean isEncryptionEnabled() {
        return (this.encryptionKeys != null) && !this.encryptionKeys.isEmpty() && (this.cryptoKeyReader != null);
    }

    public ProducerConfigurationData clone() {
        try {
            ProducerConfigurationData c = (ProducerConfigurationData) super.clone();
            c.encryptionKeys = Sets.newTreeSet(this.encryptionKeys);
            c.properties = new TreeMap<>(this.properties);
            return c;
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException("Failed to clone ProducerConfigurationData", e);
        }
    }

    public void setProducerName(String producerName) {
        checkArgument(StringUtils.isNotBlank(producerName), "producerName cannot be blank");
        this.producerName = producerName;
    }

    public void setMaxPendingMessages(int maxPendingMessages) {
        checkArgument(maxPendingMessages >= 0, "maxPendingMessages needs to be >= 0");
        this.maxPendingMessages = maxPendingMessages;
    }

    public void setMaxPendingMessagesAcrossPartitions(int maxPendingMessagesAcrossPartitions) {
        checkArgument(maxPendingMessagesAcrossPartitions >= maxPendingMessages,
                "maxPendingMessagesAcrossPartitions needs to be >= maxPendingMessages");
        this.maxPendingMessagesAcrossPartitions = maxPendingMessagesAcrossPartitions;
    }

    public void setBatchingMaxMessages(int batchingMaxMessages) {
        this.batchingMaxMessages = batchingMaxMessages;
    }

    public void setBatchingMaxBytes(int batchingMaxBytes) {
        this.batchingMaxBytes = batchingMaxBytes;
    }

    public void setSendTimeoutMs(int sendTimeout, TimeUnit timeUnit) {
        checkArgument(sendTimeout >= 0, "sendTimeout needs to be >= 0");
        this.sendTimeoutMs = timeUnit.toMillis(sendTimeout);
    }

    public void setBatchingMaxPublishDelayMicros(long batchDelay, TimeUnit timeUnit) {
        long delayInMs = timeUnit.toMillis(batchDelay);
        checkArgument(delayInMs >= 1, "configured value for batch delay must be at least 1ms");
        this.batchingMaxPublishDelayMicros = timeUnit.toMicros(batchDelay);
    }

    public void setBatchingPartitionSwitchFrequencyByPublishDelay(int frequencyByPublishDelay) {
        checkArgument(frequencyByPublishDelay >= 1, "configured value for partition switch frequency must be >= 1");
        this.batchingPartitionSwitchFrequencyByPublishDelay = frequencyByPublishDelay;
    }

    public long batchingPartitionSwitchFrequencyIntervalMicros() {
        // TODO: 10/23/23 开启batch时，分区切换评率 ，batch的最大等待时间 即10ms进行切换
        return this.batchingPartitionSwitchFrequencyByPublishDelay * batchingMaxPublishDelayMicros;
    }

    public void setAutoUpdatePartitionsIntervalSeconds(int interval, TimeUnit timeUnit) {
        checkArgument(interval > 0, "interval needs to be > 0");
        this.autoUpdatePartitionsIntervalSeconds = timeUnit.toSeconds(interval);
    }
}
