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
package org.apache.pulsar.client.api;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * {@link ConsumerBuilder} is used to configure and create instances of {@link Consumer}.
 *
 * @see PulsarClient#newConsumer()
 *
 * @since 2.0.0
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface ConsumerBuilder<T> extends Cloneable {

    /**
     * Create a copy of the current consumer builder.
     *
     * <p>Cloning the builder can be used to share an incomplete configuration and specialize it multiple times. For
     * example:
     * <pre>{@code
     * ConsumerBuilder<String> builder = client.newConsumer(Schema.STRING)
     *         .subscriptionName("my-subscription-name")
     *         .subscriptionType(SubscriptionType.Shared)
     *         .receiverQueueSize(10);
     *
     * 不同的Topic下，一些相同的配置进行订阅
     * Consumer<String> consumer1 = builder.clone().topic("my-topic-1").subscribe();
     * Consumer<String> consumer2 = builder.clone().topic("my-topic-2").subscribe();
     * }</pre>
     *
     * @return a cloned consumer builder object
     */
    ConsumerBuilder<T> clone();

    /**
     * 从Map中读取配置信息
     * Load the configuration from provided <tt>config</tt> map.
     *
     * <p>Example:
     * <pre>{@code
     * Map<String, Object> config = new HashMap<>();
     * config.put("ackTimeoutMillis", 1000);
     * config.put("receiverQueueSize", 2000);
     *
     * Consumer<byte[]> builder = client.newConsumer()
     *              .loadConf(config)
     *              .subscribe();
     *
     * Consumer<byte[]> consumer = builder.subscribe();
     * }</pre>
     *
     * @param config configuration to load
     * @return the consumer builder instance
     */
    ConsumerBuilder<T> loadConf(Map<String, Object> config);

    /**
     * 订阅指定Topic，如果订阅不存在，则创建新的订阅，默认配置下，这订阅将从Topic的尾部开始接受消息，
     * 可以看 {@link #subscriptionInitialPosition(SubscriptionInitialPosition)} 通过此方法来配置订阅行为，
     * 即订阅开始时从什么位置开始读取消息。
     * Finalize the {@link Consumer} creation by subscribing to the topic.
     *
     * <p>If the subscription does not exist, a new subscription will be created. By default the subscription
     * will be created at the end of the topic. See {@link #subscriptionInitialPosition(SubscriptionInitialPosition)}
     * to configure the initial position behavior.
     *
     * <p>Once a subscription is created, it will retain the data and the subscription cursor even if the consumer
     * is not connected.
     *
     * @return the consumer builder instance
     * @throws PulsarClientException
     *             if the the subscribe operation fails
     */
    Consumer<T> subscribe() throws PulsarClientException;

    /**
     * Finalize the {@link Consumer} creation by subscribing to the topic in asynchronous mode.
     *
     * <p>If the subscription does not exist, a new subscription will be created. By default the subscription
     * will be created at the end of the topic. See {@link #subscriptionInitialPosition(SubscriptionInitialPosition)}
     * to configure the initial position behavior.
     *
     * <p>Once a subscription is created, it will retain the data and the subscription cursor even
     * if the consumer is not connected.
     *
     * @return a future that will yield a {@link Consumer} instance
     * @throws PulsarClientException
     *             if the the subscribe operation fails
     */
    CompletableFuture<Consumer<T>> subscribeAsync();

    /**
     * 设置多个 Topic，消费者将消费多个 Topic 中的消息。（注意，由于ACL原因，这里多个Topic 只能是同一个 Namespace 中的）
     * Specify the topics this consumer will subscribe on.
     *
     * @param topicNames a set of topic that the consumer will subscribe on
     * @return the consumer builder instance
     */
    ConsumerBuilder<T> topic(String... topicNames);

    /**
     * Specify a list of topics that this consumer will subscribe on.
     *
     * @param topicNames a list of topic that the consumer will subscribe on
     * @return the consumer builder instance
     */
    ConsumerBuilder<T> topics(List<String> topicNames);

    /**
     * 设置 Topic 正则表达式订阅,消费者将通过这个表达式匹配 Topic 名并自动订阅。（这里所有的  Topic 只能在同一 Namespace）
     * Specify a pattern for topics that this consumer will subscribe on.
     *
     * <p>The pattern will be applied to subscribe to all the topics, within a single namespace, that will match the
     * pattern.
     *
     * <p>The consumer will automatically subscribe to topics created after itself.
     *
     * @param topicsPattern
     *            a regular expression to select a list of topics to subscribe to
     * @return the consumer builder instance
     */
    ConsumerBuilder<T> topicsPattern(Pattern topicsPattern);

    /**
     * Specify a pattern for topics that this consumer will subscribe on.
     *
     * <p>It accepts regular expression and will be compiled into a pattern internally. Eg.
     * "persistent://public/default/pattern-topic-.*"
     *
     * <p>The pattern will be applied to subscribe to all the topics, within a single namespace, that will match the
     * pattern.
     *
     * <p>The consumer will automatically subscribe to topics created after itself.
     *
     * @param topicsPattern
     *            given regular expression for topics pattern
     * @return the consumer builder instance
     */
    ConsumerBuilder<T> topicsPattern(String topicsPattern);

    /**
     * 设置订阅名
     * Specify the subscription name for this consumer.
     *
     * <p>This argument is required when constructing the consumer.
     *
     * @param subscriptionName the name of the subscription that this consumer should attach to
     *
     * @return the consumer builder instance
     */
    ConsumerBuilder<T> subscriptionName(String subscriptionName);

    /**
     * Specify the subscription properties for this subscription.
     * Properties are immutable, and consumers under the same subscription will fail to create a subscription
     * if they use different properties.
     * @param subscriptionProperties
     * @return
     */
    ConsumerBuilder<T> subscriptionProperties(Map<String, String> subscriptionProperties);


    /**
     * 设置未确认消息的确认超时时间，截断为最接近的毫秒。 超时时间需要大于10秒。 默认情况下，确认超时被禁用，这意味着除非消费者崩溃，
     * 否则不会重新传递传递给消费者的消息。当启用确认超时时，如果在指定的超时时间内未确认消息，
     * 则将重新传递给消费者（在共享订阅的情况下可能传递给其他消费者）。
     * Set the timeout for unacked messages, truncated to the nearest millisecond. The timeout needs to be greater than
     * 1 second.
     *
     * <p>By default, the acknowledge timeout is disabled and that means that messages delivered to a
     * consumer will not be re-delivered unless the consumer crashes.
     *
     * <p>When enabling ack timeout, if a message is not acknowledged within the specified timeout
     * it will be re-delivered to the consumer (possibly to a different consumer in case of
     * a shared subscription).
     *
     * @param ackTimeout
     *            for unacked messages.
     * @param timeUnit
     *            unit in which the timeout is provided.
     * @return the consumer builder instance
     */
    ConsumerBuilder<T> ackTimeout(long ackTimeout, TimeUnit timeUnit);

    /**
     * Ack will return receipt but does not mean that the message will not be resent after get receipt.
     *
     * @param isAckReceiptEnabled {@link Boolean} is enable ack for receipt
     * @return the consumer builder instance
     */
    ConsumerBuilder<T> isAckReceiptEnabled(boolean isAckReceiptEnabled);

    /**
     * Define the granularity of the ack-timeout redelivery.
     *
     * <p>By default, the tick time is set to 1 second. Using an higher tick time will
     * reduce the memory overhead to track messages when the ack-timeout is set to
     * bigger values (eg: 1hour).
     *
     * @param tickTime
     *            the min precision for the ack timeout messages tracker
     * @param timeUnit
     *            unit in which the timeout is provided.
     * @return the consumer builder instance
     */
    ConsumerBuilder<T> ackTimeoutTickTime(long tickTime, TimeUnit timeUnit);

    /**
     * Set the delay to wait before re-delivering messages that have failed to be process.
     *
     * <p>When application uses {@link Consumer#negativeAcknowledge(Message)}, the failed message
     * will be redelivered after a fixed timeout. The default is 1 min.
     *
     * @param redeliveryDelay
     *            redelivery delay for failed messages
     * @param timeUnit
     *            unit in which the timeout is provided.
     * @return the consumer builder instance
     * @see Consumer#negativeAcknowledge(Message)
     */
    ConsumerBuilder<T> negativeAckRedeliveryDelay(long redeliveryDelay, TimeUnit timeUnit);

    /**
     * 订阅类型，目前支持3种：Exclusive（独占）默认配置，Failover（失败转移），Shared（共享）
     * Select the subscription type to be used when subscribing to the topic.
     *
     * <p>Options are:
     * <ul>
     *  <li>{@link SubscriptionType#Exclusive} (Default)</li>
     *  <li>{@link SubscriptionType#Failover}</li>
     *  <li>{@link SubscriptionType#Shared}</li>
     * </ul>
     *
     * @param subscriptionType
     *            the subscription type value
     * @return the consumer builder instance
     */
    ConsumerBuilder<T> subscriptionType(SubscriptionType subscriptionType);

    /**
     * Select the subscription mode to be used when subscribing to the topic.
     *
     * <p>Options are:
     * <ul>
     *  <li>{@link SubscriptionMode#Durable} (Default)</li>
     *  <li>{@link SubscriptionMode#NonDurable}</li>
     * </ul>
     *
     * @param subscriptionMode
     *            the subscription mode value
     * @return the consumer builder instance
     */
    ConsumerBuilder<T> subscriptionMode(SubscriptionMode subscriptionMode);

    /**
     * 设置消费者监听器，一旦有消息到来，将自动调用此接口
     * Sets a {@link MessageListener} for the consumer
     *
     * <p>When a {@link MessageListener} is set, application will receive messages through it. Calls to
     * {@link Consumer#receive()} will not be allowed.
     *
     * @param messageListener
     *            the listener object
     * @return the consumer builder instance
     */
    ConsumerBuilder<T> messageListener(MessageListener<T> messageListener);

    /**
     * 加密配置信息读取器，读取的加密配置将用于加密消息
     * Sets a {@link CryptoKeyReader}.
     *
     * <p>Configure the key reader to be used to decrypt the message payloads.
     *
     * @param cryptoKeyReader
     *            CryptoKeyReader object
     * @return the consumer builder instance
     */
    ConsumerBuilder<T> cryptoKeyReader(CryptoKeyReader cryptoKeyReader);

    /**
     * Sets the default implementation of {@link CryptoKeyReader}.
     *
     * <p>Configure the key reader to be used to decrypt the message payloads.
     *
     * @param privateKey
     *            the private key that is always used to decrypt message payloads.
     * @return the consumer builder instance
     * @since 2.8.0
     */
    ConsumerBuilder<T> defaultCryptoKeyReader(String privateKey);

    /**
     * Sets the default implementation of {@link CryptoKeyReader}.
     *
     * <p>Configure the key reader to be used to decrypt the message payloads.
     *
     * @param privateKeys
     *            the map of private key names and their URIs used to decrypt message payloads.
     * @return the consumer builder instance
     * @since 2.8.0
     */
    ConsumerBuilder<T> defaultCryptoKeyReader(Map<String, String> privateKeys);

    /**
     * Sets a {@link MessageCrypto}.
     *
     * <p>Contains methods to encrypt/decrypt message for End to End Encryption.
     *
     * @param messageCrypto
     *            MessageCrypto object
     * @return the consumer builder instance
     */
    ConsumerBuilder<T> messageCrypto(MessageCrypto messageCrypto);

    /**
     * 加密失败执行的动作，参考枚举类ConsumerCryptoFailureAction
     * Sets the ConsumerCryptoFailureAction to the value specified.
     *
     * @param action
     *            the action the consumer will take in case of decryption failures
     * @return the consumer builder instance
     */
    ConsumerBuilder<T> cryptoFailureAction(ConsumerCryptoFailureAction action);

    /**
     * 设置接收队列大小，默认值为1000（大多数场景推荐值）
     * Sets the size of the consumer receive queue.
     *
     * <p>The consumer receive queue controls how many messages can be accumulated by the {@link Consumer} before the
     * application calls {@link Consumer#receive()}. Using a higher value could potentially increase the consumer
     * throughput at the expense of bigger memory utilization.
     *
     * <p><b>Setting the consumer queue size as zero</b>
     * <ul>
     * <li>Decreases the throughput of the consumer, by disabling pre-fetching of messages. This approach improves the
     * message distribution on shared subscription, by pushing messages only to the consumers that are ready to process
     * them. Neither {@link Consumer#receive(int, TimeUnit)} nor Partitioned Topics can be used if the consumer queue
     * size is zero. {@link Consumer#receive()} function call should not be interrupted when the consumer queue size is
     * zero.</li>
     * <li>Doesn't support Batch-Message: if consumer receives any batch-message then it closes consumer connection with
     * broker and {@link Consumer#receive()} call will remain blocked while {@link Consumer#receiveAsync()} receives
     * exception in callback. <b> consumer will not be able receive any further message unless batch-message in pipeline
     * is removed</b></li>
     * </ul>
     * Default value is {@code 1000} messages and should be good for most use cases.
     *
     * @param receiverQueueSize
     *            the new receiver queue size value
     * @return the consumer builder instance
     */
    ConsumerBuilder<T> receiverQueueSize(int receiverQueueSize);

    /**
     * 消息确认分组提交（类似于批量确认的意思）如果设置为0，则意味着立即发送确认消息，否则按照配置的间隔时间确认，默认值为100ms确认
     * Group the consumer acknowledgments for the specified time.
     *
     * <p>By default, the consumer will use a 100 ms grouping time to send out the acknowledgments to the broker.
     *
     * <p>Setting a group time of 0, will send out the acknowledgments immediately. A longer ack group time
     * will be more efficient at the expense of a slight increase in message re-deliveries after a failure.
     *
     * @param delay
     *            the max amount of time an acknowledgemnt can be delayed
     * @param unit
     *            the time unit for the delay
     * @return the consumer builder instance
     */
    ConsumerBuilder<T> acknowledgmentGroupTime(long delay, TimeUnit unit);

    /**
     *
     * @param replicateSubscriptionState
     */
    ConsumerBuilder<T> replicateSubscriptionState(boolean replicateSubscriptionState);

    /**
     * 跨分区时，设置最大总消息队列大小，默认为50000
     * Set the max total receiver queue size across partitons.
     *
     * <p>This setting will be used to reduce the receiver queue size for individual partitions
     * {@link #receiverQueueSize(int)} if the total exceeds this value (default: 50000).
     * The purpose of this setting is to have an upper-limit on the number
     * of messages that a consumer can be pushed at once from a broker, across all
     * the partitions.
     *
     * @param maxTotalReceiverQueueSizeAcrossPartitions
     *            max pending messages across all the partitions
     * @return the consumer builder instance
     */
    ConsumerBuilder<T> maxTotalReceiverQueueSizeAcrossPartitions(int maxTotalReceiverQueueSizeAcrossPartitions);

    /**
     * 设置消费者名称
     * Set the consumer name.
     *
     * <p>Consumer name is informative and it can be used to indentify a particular consumer
     * instance from the topic stats.
     *
     * @param consumerName
     * @return the consumer builder instance
     */
    ConsumerBuilder<T> consumerName(String consumerName);

    /**
     * 消费者事件监听器，消费组接收状态改变（如：消费组里有成员宕机），应用程序依靠这种机制能对状态变更做出反应
     * Sets a {@link ConsumerEventListener} for the consumer.
     *
     * <p>The consumer group listener is used for receiving consumer state change in a consumer group for failover
     * subscription. Application can then react to the consumer state changes.
     *
     * @param consumerEventListener
     *            the consumer group listener object
     * @return the consumer builder instance
     */
    ConsumerBuilder<T> consumerEventListener(ConsumerEventListener consumerEventListener);

    /**
     * 如果启用，则使用者将从 Compact Topic 中读取消息，而不是读 Topic 中完整消息积压。 这意味着，如果 Topic 已被压缩，
     * 则使用者将只看到主题中每个键的最新值，直到 Topic 积压的消息全部已经压缩为止。 除此之外，消息将照常发送。
     * readCompacted只能启用对持久 Topic 的订阅，这些 Topic 只能有一个活动消费者（即失败或独占模式订阅）。
     * 尝试在订阅非持久性 Topic 或共享订阅时启用它，将导致订阅调用抛出PulsarClientException。
     * If enabled, the consumer will read messages from the compacted topic rather than reading the full message backlog
     * of the topic. This means that, if the topic has been compacted, the consumer will only see the latest value for
     * each key in the topic, up until the point in the topic message backlog that has been compacted. Beyond that
     * point, the messages will be sent as normal.
     *
     * <p>readCompacted can only be enabled subscriptions to persistent topics, which have a single active consumer
     * (i.e. failure or exclusive subscriptions). Attempting to enable it on subscriptions to a non-persistent topics
     * or on a shared subscription, will lead to the subscription call throwing a PulsarClientException.
     *
     * @param readCompacted
     *            whether to read from the compacted topic
     * @return the consumer builder instance
     */
    ConsumerBuilder<T> readCompacted(boolean readCompacted);

    /**
     * 当通过正则表达式订阅时，系统将通过设置指定时间间隔来自动发现新的符合正则表达式的 Topic。
     * 此参数就是设置时间间隔使用的。（注意，这里最新时间单位为分钟，最小值为1分钟）
     * Set topics auto discovery period when using a pattern for topics consumer.
     * The period is in minute, and default and minimum value is 1 minute.
     *
     * @param periodInMinutes
     *            number of minutes between checks for
     *            new topics matching pattern set with {@link #topicsPattern(String)}
     * @return the consumer builder instance
     */
    ConsumerBuilder<T> patternAutoDiscoveryPeriod(int periodInMinutes);


    /**
     * Set topics auto discovery period when using a pattern for topics consumer.
     *
     * @param interval
     *            the amount of delay between checks for
     *            new topics matching pattern set with {@link #topicsPattern(String)}
     * @param unit
     *            the unit of the topics auto discovery period
     *
     * @return the consumer builder instance
     */
    ConsumerBuilder<T> patternAutoDiscoveryPeriod(int interval, TimeUnit unit);


    /**
     * 订阅共享模式时有效，如果消费者有更高的优先级（priorityLevel），将优先分发消息。
     * （其中，0为最高优先级，以此类推）在共享订阅模式下，如果拥有许可（permits），
     * broker 将首先将消息分派给最高优先级消费者，否则代理将考虑下一个优先级消费者。
     * 如果订阅具有带有priorityLevel 0的consumer-A和带有priorityLevel 1的Consumer-B，
     * 则broker将仅将消息分派给consumer-A，直到它用完permit，然后broker开始将消息分派给Consumer-B。请看如下例子：
     * <b>Shared subscription</b>
     * Sets priority level for the shared subscription consumers to which broker gives more priority while dispatching
     * messages. Here, broker follows descending priorities. (eg: 0=max-priority, 1, 2,..)
     *
     * <p>In Shared subscription mode, broker will first dispatch messages to max priority-level
     * consumers if they have permits, else broker will consider next priority level consumers.
     *
     * <p>If subscription has consumer-A with priorityLevel 0 and Consumer-B with priorityLevel 1
     * then broker will dispatch messages to only consumer-A until it runs out permit and then broker
     * starts dispatching messages to Consumer-B.
     *
     * <p><pre>
     * Consumer PriorityLevel Permits
     * C1       0             2
     * C2       0             1
     * C3       0             1
     * C4       1             2
     * C5       1             1
     * Order in which broker dispatches messages to consumers: C1, C2, C3, C1, C4, C5, C4
     * </pre>
     *
     * <p><b>Failover subscription</b>
     * Broker selects active consumer for a failover-subscription based on consumer's priority-level and
     * lexicographical sorting of a consumer name.
     * eg:
     * <pre>
     * 1. Active consumer = C1 : Same priority-level and lexicographical sorting
     * Consumer PriorityLevel Name
     * C1       0             aaa
     * C2       0             bbb
     *
     * 2. Active consumer = C2 : Consumer with highest priority
     * Consumer PriorityLevel Name
     * C1       1             aaa
     * C2       0             bbb
     *
     * Partitioned-topics:
     * Broker evenly assigns partitioned topics to highest priority consumers.
     * </pre>
     *
     * @param priorityLevel the priority of this consumer
     * @return the consumer builder instance
     */
    ConsumerBuilder<T> priorityLevel(int priorityLevel);

    /**
     * 设置消费者的键值对
     * Set a name/value property with this consumer.
     *
     * <p>Properties are application defined metadata that can be attached to the consumer.
     * When getting the topic stats, this metadata will be associated to the consumer stats for easier identification.
     *
     * @param key
     *            the property key
     * @param value
     *            the property value
     * @return the consumer builder instance
     */
    ConsumerBuilder<T> property(String key, String value);

    /**
     * 指定Map设置消费者的键值对
     * Add all the properties in the provided map to the consumer.
     *
     * <p>Properties are application defined metadata that can be attached to the consumer.
     * When getting the topic stats, this metadata will be associated to the consumer stats for easier identification.
     *
     * @param properties the map with properties
     * @return the consumer builder instance
     */
    ConsumerBuilder<T> properties(Map<String, String> properties);

    /**
     * 当创建新订阅时，指定从什么位置开始都读取消息
     * Set the {@link SubscriptionInitialPosition} for the consumer.
     *
     * @param subscriptionInitialPosition
     *            the position where to initialize a newly created subscription
     * @return the consumer builder instance
     */
    ConsumerBuilder<T> subscriptionInitialPosition(SubscriptionInitialPosition subscriptionInitialPosition);

    /**
     * 设置订阅模式，用于决定消费者订阅持久化、非持久化和所有的 Topic，此参数只用于正则表达式订阅方式中
     * Determines to which topics this consumer should be subscribed to - Persistent, Non-Persistent, or both. Only used
     * with pattern subscriptions.
     *
     * @param regexSubscriptionMode
     *            Pattern subscription mode
     */
    ConsumerBuilder<T> subscriptionTopicsMode(RegexSubscriptionMode regexSubscriptionMode);

    /**
     * 设置消费者拦截链
     * Intercept {@link Consumer}.
     *
     * @param interceptors the list of interceptors to intercept the consumer created by this builder.
     */
    ConsumerBuilder<T> intercept(ConsumerInterceptor<T> ...interceptors);

    /**
     * 设置死信消息策略，这里可以设置当消费一定次数时，消息将会放入指定的死信队列，
     * 死信队列 Topic 的默认名为{TopicName}-{Subscription}-DLQ，当然，这里也可以指定死信队列名称
     * Set dead letter policy for consumer.
     *
     * <p>By default some message will redelivery so many times possible, even to the extent that it can be never stop.
     * By using dead letter mechanism messages will has the max redelivery count, when message exceeding the maximum
     * number of redeliveries, message will send to the Dead Letter Topic and acknowledged automatic.
     *
     * <p>You can enable the dead letter mechanism by setting dead letter policy.
     * example:
     * <pre>
     * client.newConsumer()
     *          .deadLetterPolicy(DeadLetterPolicy.builder().maxRedeliverCount(10).build())
     *          .subscribe();
     * </pre>
     * Default dead letter topic name is {TopicName}-{Subscription}-DLQ. // 默认名称
     * To setting a custom dead letter topic name
     * <pre>
     * client.newConsumer()
     *          .deadLetterPolicy(DeadLetterPolicy
     *              .builder()
     *              .maxRedeliverCount(10) // 最大的消费次数
     *              .deadLetterTopic("your-topic-name") // 自定义名称
     *              .build())
     *          .subscribe();
     * </pre>
     * When a dead letter policy is specified, and no ackTimeoutMillis is specified,
     * then the ack timeout will be set to 30000 millisecond.
     */
    ConsumerBuilder<T> deadLetterPolicy(DeadLetterPolicy deadLetterPolicy);

    /**
     * 如果启用，消费者将自动订阅新增的分区的变更（这里分区只能增长）
     * If enabled, the consumer will auto subscribe for partitions increasement.
     * This is only for partitioned consumer.
     *
     * @param autoUpdate
     *            whether to auto update partition increasement
     */
    ConsumerBuilder<T> autoUpdatePartitions(boolean autoUpdate);

    /**
     * 如果启用，消费者将自动订阅新增的分区的变更（这里分区只能增长）检查时间，默认为1分钟
     * Set the interval of updating partitions <i>(default: 1 minute)</i>. This only works if autoUpdatePartitions is
     * enabled.
     *
     * @param interval
     *            the interval of updating partitions
     * @param unit
     *            the time unit of the interval.
     * @return the consumer builder instance
     */
    ConsumerBuilder<T> autoUpdatePartitionsInterval(int interval, TimeUnit unit);

    /**
     * Set KeyShared subscription policy for consumer.
     *
     * <p>By default, KeyShared subscription use auto split hash range to maintain consumers. If you want to
     * set a different KeyShared policy, you can set by following example:
     *
     * <pre>
     * client.newConsumer()
     *          .keySharedPolicy(KeySharedPolicy.stickyHashRange().ranges(Range.of(0, 10)))
     *          .subscribe();
     * </pre>
     * Details about sticky hash range policy, please see {@link KeySharedPolicy.KeySharedPolicySticky}.
     *
     * <p>Or
     * <pre>
     * client.newConsumer()
     *          .keySharedPolicy(KeySharedPolicy.autoSplitHashRange())
     *          .subscribe();
     * </pre>
     * Details about auto split hash range policy, please see {@link KeySharedPolicy.KeySharedPolicyAutoSplit}.
     *
     * @param keySharedPolicy The {@link KeySharedPolicy} want to specify
     */
    ConsumerBuilder<T> keySharedPolicy(KeySharedPolicy keySharedPolicy);

    /**
     * Set the consumer to include the given position of any reset operation like {@link Consumer#seek(long) or
     * {@link Consumer#seek(MessageId)}}.
     *
     * @return the consumer builder instance
     */
    ConsumerBuilder<T> startMessageIdInclusive();

    /**
     * 批量消息接收策略
     * Set batch receive policy {@link BatchReceivePolicy} for consumer.
     * By default, consumer will use {@link BatchReceivePolicy#DEFAULT_POLICY} as batch receive policy.
     *
     * <p>Example:
     * <pre>
     * client.newConsumer().batchReceivePolicy(BatchReceivePolicy.builder()
     *              .maxNumMessages(100) // 最大消息数
     *              .maxNumBytes(5 * 1024 * 1024) // 最大消息的size
     *              .timeout(100, TimeUnit.MILLISECONDS) // 超时时间
     *              .build()).subscribe();
     * </pre>
     */
    ConsumerBuilder<T> batchReceivePolicy(BatchReceivePolicy batchReceivePolicy);

    /**
     * 是否可以重试，默认关闭
     * If enabled, the consumer will auto retry message.
     * default unabled. disable?
     *
     * @param retryEnable
     *            whether to auto retry message
     */
    ConsumerBuilder<T> enableRetry(boolean retryEnable);

    /**
     * Enable or disable the batch index acknowledgment. To enable this feature must ensure batch index acknowledgment
     * feature is enabled at the broker side.
     */
    ConsumerBuilder<T> enableBatchIndexAcknowledgment(boolean batchIndexAcknowledgmentEnabled);

    /**
     * Consumer buffers chunk messages into memory until it receives all the chunks of the original message. While
     * consuming chunk-messages, chunks from same message might not be contiguous in the stream and they might be mixed
     * with other messages' chunks. so, consumer has to maintain multiple buffers to manage chunks coming from different
     * messages. This mainly happens when multiple publishers are publishing messages on the topic concurrently or
     * publisher failed to publish all chunks of the messages.
     *
     * <pre>
     * eg: M1-C1, M2-C1, M1-C2, M2-C2
     * Here, Messages M1-C1 and M1-C2 belong to original message M1, M2-C1 and M2-C2 messages belong to M2 message.
     * </pre>
     * Buffering large number of outstanding uncompleted chunked messages can create memory pressure and it can be
     * guarded by providing this @maxPendingChuckedMessage threshold. Once, consumer reaches this threshold, it drops
     * the outstanding unchunked-messages by silently acking or asking broker to redeliver later by marking it unacked.
     * This behavior can be controlled by configuration: @autoAckOldestChunkedMessageOnQueueFull
     *
     * The default value is 10.
     *
     * @param maxPendingChuckedMessage
     * @return
     * @deprecated use {@link #maxPendingChunkedMessage(int)}
     */
    @Deprecated
    ConsumerBuilder<T> maxPendingChuckedMessage(int maxPendingChuckedMessage);

    /**
     * 消息块会存放到consumer的内存中知道所有的消息块都被接收
     * Consumer buffers chunk messages into memory until it receives all the chunks of the original message. While
     * consuming chunk-messages, chunks from same message might not be contiguous in the stream and they might be mixed
     * with other messages' chunks. so, consumer has to maintain multiple buffers to manage chunks coming from different
     * messages. This mainly happens when multiple publishers are publishing messages on the topic concurrently or
     * publisher failed to publish all chunks of the messages.
     *
     * <pre>
     * eg: M1-C1, M2-C1, M1-C2, M2-C2
     * Here, Messages M1-C1 and M1-C2 belong to original message M1, M2-C1 and M2-C2 messages belong to M2 message.
     * </pre>
     * Buffering large number of outstanding uncompleted chunked messages can create memory pressure and it can be
     * guarded by providing this @maxPendingChunkedMessage threshold. Once, consumer reaches this threshold, it drops
     * the outstanding unchunked-messages by silently acking or asking broker to redeliver later by marking it unacked.
     * This behavior can be controlled by configuration: @autoAckOldestChunkedMessageOnQueueFull
     *
     * The default value is 10.
     *
     * @param maxPendingChunkedMessage
     * @return
     */
    ConsumerBuilder<T> maxPendingChunkedMessage(int maxPendingChunkedMessage);

    /**
     * Buffering large number of outstanding uncompleted chunked messages can create memory pressure and it can be
     * guarded by providing this @maxPendingChunkedMessage threshold. Once, consumer reaches this threshold, it drops
     * the outstanding unchunked-messages by silently acking if autoAckOldestChunkedMessageOnQueueFull is true else it
     * marks them for redelivery.
     *
     * @default false
     *
     * @param autoAckOldestChunkedMessageOnQueueFull
     * @return
     */
    ConsumerBuilder<T> autoAckOldestChunkedMessageOnQueueFull(boolean autoAckOldestChunkedMessageOnQueueFull);

    /**
     * 消息块的过期时间，默认为1mins
     * If producer fails to publish all the chunks of a message then consumer can expire incomplete chunks if consumer
     * won't be able to receive all chunks in expire times (default 1 minute).
     *
     * @param duration
     * @param unit
     * @return
     */
    ConsumerBuilder<T> expireTimeOfIncompleteChunkedMessage(long duration, TimeUnit unit);

    /**
     * Enable pooling of messages and the underlying data buffers.
     * <p/>
     * When pooling is enabled, the application is responsible for calling Message.release() after the handling of every
     * received message. If “release()” is not called on a received message, there will be a memory leak. If an
     * application attempts to use and already “released” message, it might experience undefined behavior (eg: memory
     * corruption, deserialization error, etc.).
     */
    ConsumerBuilder<T> poolMessages(boolean poolMessages);

    /**
     * If it's configured with a non-null value, the consumer will use the processor to process the payload, including
     * decoding it to messages and triggering the listener.
     *
     * Default: null
     */
    ConsumerBuilder<T> messagePayloadProcessor(MessagePayloadProcessor payloadProcessor);

    /**
     * Notice: the negativeAckRedeliveryBackoff will not work with `consumer.negativeAcknowledge(MessageId messageId)`
     * because we are not able to get the redelivery count from the message ID.
     *
     * <p>Example:
     * <pre>
     * client.newConsumer().negativeAckRedeliveryBackoff(ExponentialRedeliveryBackoff.builder()
     *              .minNackTimeMs(1000)
     *              .maxNackTimeMs(60 * 1000)
     *              .build()).subscribe();
     * </pre>
     */
    ConsumerBuilder<T> negativeAckRedeliveryBackoff(RedeliveryBackoff negativeAckRedeliveryBackoff);

    /**
     * Notice: the redeliveryBackoff will not work with `consumer.negativeAcknowledge(MessageId messageId)`
     * because we are not able to get the redelivery count from the message ID.
     *
     * <p>Example:
     * <pre>
     * client.newConsumer().ackTimeout(10, TimeUnit.SECOND)
     *              .ackTimeoutRedeliveryBackoff(ExponentialRedeliveryBackoff.builder()
     *              .minNackTimeMs(1000)
     *              .maxNackTimeMs(60 * 1000)
     *              .build()).subscribe();
     * </pre>
     */
    ConsumerBuilder<T> ackTimeoutRedeliveryBackoff(RedeliveryBackoff ackTimeoutRedeliveryBackoff);

    /**
     * Start the consumer in a paused state. When enabled, the consumer does not immediately fetch messages when
     * {@link #subscribe()} is called. Instead, the consumer waits to fetch messages until {@link Consumer#resume()} is
     * called.
     * <p/>
     * See also {@link Consumer#pause()}.
     * @default false
     */
    ConsumerBuilder<T> startPaused(boolean paused);
}
