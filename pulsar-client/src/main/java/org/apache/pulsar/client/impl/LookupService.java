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

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace.Mode;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.schema.SchemaInfo;

/**
 * Provides lookup service to find broker which serves given topic. It helps to
 * lookup
 * <ul>
 * <li><b>topic-lookup:</b> lookup to find broker-address which serves given
 * topic</li>
 * <li><b>Partitioned-topic-Metadata-lookup:</b> lookup to find
 * PartitionedMetadata for a given topic</li>
 * </ul>
 *
 */
public interface LookupService extends AutoCloseable {

    /**
     * 用于动态更新serviceUrl
     * Instruct the LookupService to switch to a new service URL for all subsequent requests.
     */
    void updateServiceUrl(String serviceUrl) throws PulsarClientException;

    /**
     * 根据Topic来获取活的broker地址
     * 通过查找服务中 getBroker 方法获取存活的 broker 地址，有两种实现，一个是 Http 协议实现，
     * 一个是 Native（TCP）实现，这里只取一个实现分析，TCP实现的：BinaryProtoLookupService 类
     *
     * Calls broker lookup-api to get broker {@link InetSocketAddress} which serves namespace bundle that contains given
     * topic.
     *
     * @param topicName
     *            topic-name
     * @return a pair of addresses, representing the logical and physical address of the broker that serves given topic
     */
    CompletableFuture<Pair<InetSocketAddress, InetSocketAddress>> getBroker(TopicName topicName);

    /**
     * 根据Topic获取Topic分区信息
     * Returns {@link PartitionedTopicMetadata} for a given topic.
     *
     * @param topicName topic-name
     * @return
     */
    CompletableFuture<PartitionedTopicMetadata> getPartitionedTopicMetadata(TopicName topicName);

    /**
     * 根据Topic获取Schema信息
     * Returns current SchemaInfo {@link SchemaInfo} for a given topic.
     *
     * @param topicName topic-name
     * @return SchemaInfo
     */
    CompletableFuture<Optional<SchemaInfo>> getSchema(TopicName topicName);

    /**
     * Returns specific version SchemaInfo {@link SchemaInfo} for a given topic.
     *
     * @param topicName topic-name
     * @param version schema info version
     * @return SchemaInfo
     */
    CompletableFuture<Optional<SchemaInfo>> getSchema(TopicName topicName, byte[] version);

    /**
     * 获取serviceUrl
     * Returns broker-service lookup api url.
     *
     * @return
     */
    String getServiceUrl();

    /**
     * 获取Namespace下所有的topic
     * Returns all the topics name for a given namespace.
     *
     * @param namespace : namespace-name
     * @return
     */
    CompletableFuture<List<String>> getTopicsUnderNamespace(NamespaceName namespace, Mode mode);

}
