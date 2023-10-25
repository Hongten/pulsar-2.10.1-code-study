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
package org.apache.pulsar.broker.service.persistent;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.apache.pulsar.common.policies.data.impl.DispatchRateImpl;
import org.apache.pulsar.common.util.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: 2/21/23 在很在之前消费速率限流就已经存在，可以看到，可以在topic，subscription，replicator， broker等级别设置消费限流
// todo dispatch rate limiter
public class DispatchRateLimiter {

    // TODO: 2/21/23 消费限流类型
    public enum Type {
        TOPIC,
        SUBSCRIPTION,
        REPLICATOR,
        BROKER
    }

    private final PersistentTopic topic;
    private final String topicName;
    private final Type type;

    private final BrokerService brokerService;
    // todo RateLimiter 里面有线程一个线程池，也意味着，我们只要设置了topic的限流策略后，每个topic上面至少一个线程池。
    //  如果我们有很多个topic（e.g 100w topic)，是否存在性能问题呢？
    // todo msg 数量限制器
    private RateLimiter dispatchRateLimiterOnMessage;
    // todo msg 数据大小限制器
    private RateLimiter dispatchRateLimiterOnByte;

    // TODO: 2/21/23 构造器
    public DispatchRateLimiter(PersistentTopic topic, Type type) {
        this.topic = topic;
        this.topicName = topic.getName();
        this.brokerService = topic.getBrokerService();
        this.type = type;
        // TODO: 2/21/23 更新消费速率
        updateDispatchRate();
    }

    // TODO: 2/21/23 设置broker级别的消费限流速率
    public DispatchRateLimiter(BrokerService brokerService) {
        this.topic = null;
        this.topicName = null;
        this.brokerService = brokerService;
        this.type = Type.BROKER;
        // TODO: 2/21/23 跟同学消费速率
        updateDispatchRate();
    }

    /**
     * returns available msg-permit if msg-dispatch-throttling is enabled else it returns -1.
     *
     * @return
     */
    public long getAvailableDispatchRateLimitOnMsg() {
        return dispatchRateLimiterOnMessage == null ? -1 : dispatchRateLimiterOnMessage.getAvailablePermits();
    }

    /**
     * returns available byte-permit if msg-dispatch-throttling is enabled else it returns -1.
     *
     * @return
     */
    public long getAvailableDispatchRateLimitOnByte() {
        return dispatchRateLimiterOnByte == null ? -1 : dispatchRateLimiterOnByte.getAvailablePermits();
    }

    /**
     * It acquires msg and bytes permits from rate-limiter and returns if acquired permits succeed.
     *
     * @param msgPermits
     * @param bytePermits
     * @return
     */
    public boolean tryDispatchPermit(long msgPermits, long bytePermits) {
        // todo 是否超过限流值进行判断
        boolean acquiredMsgPermit = msgPermits <= 0 || dispatchRateLimiterOnMessage == null
        // acquiring permits must be < configured msg-rate;
                || dispatchRateLimiterOnMessage.tryAcquire(msgPermits);
        boolean acquiredBytePermit = bytePermits <= 0 || dispatchRateLimiterOnByte == null
        // acquiring permits must be < configured msg-rate;
                || dispatchRateLimiterOnByte.tryAcquire(bytePermits);
        return acquiredMsgPermit && acquiredBytePermit;
    }

    /**
     * checks if dispatch-rate limit is configured and if it's configured then check if permits are available or not.
     *
     * @return
     */
    public boolean hasMessageDispatchPermit() {
        // TODO: 2/22/23 是否还有可用的permit，message有可用permit，并且byte也有可用的permit。即至少有一个超quota了，返回false，都没有超quota，返回true
        return (dispatchRateLimiterOnMessage == null || dispatchRateLimiterOnMessage.getAvailablePermits() > 0)
                && (dispatchRateLimiterOnByte == null || dispatchRateLimiterOnByte.getAvailablePermits() > 0);
    }

    /**
     * Checks if dispatch-rate limiting is enabled.
     *
     * @return
     */
    public boolean isDispatchRateLimitingEnabled() {
        // TODO: 2/22/23 两个task 至少一个不为null
        return dispatchRateLimiterOnMessage != null || dispatchRateLimiterOnByte != null;
    }

    /**
     * createDispatchRate according to broker service config.
     *
     * @return
     */
    private DispatchRate createDispatchRate() {
        int dispatchThrottlingRateInMsg;
        long dispatchThrottlingRateInByte;
        ServiceConfiguration config = brokerService.pulsar().getConfiguration();

        switch (type) {
            case TOPIC:
                dispatchThrottlingRateInMsg = config.getDispatchThrottlingRatePerTopicInMsg();
                dispatchThrottlingRateInByte = config.getDispatchThrottlingRatePerTopicInByte();
                break;
            case SUBSCRIPTION:
                dispatchThrottlingRateInMsg = config.getDispatchThrottlingRatePerSubscriptionInMsg();
                dispatchThrottlingRateInByte = config.getDispatchThrottlingRatePerSubscriptionInByte();
                break;
            case REPLICATOR:
                dispatchThrottlingRateInMsg = config.getDispatchThrottlingRatePerReplicatorInMsg();
                dispatchThrottlingRateInByte = config.getDispatchThrottlingRatePerReplicatorInByte();
                break;
            case BROKER: // TODO: 2/22/23 broker的消费限流配置
                dispatchThrottlingRateInMsg = config.getDispatchThrottlingRateInMsg();
                dispatchThrottlingRateInByte = config.getDispatchThrottlingRateInByte();
                break;
            default:
                dispatchThrottlingRateInMsg = -1;
                dispatchThrottlingRateInByte = -1;
        }


        return DispatchRate.builder()
                .dispatchThrottlingRateInMsg(dispatchThrottlingRateInMsg)
                .dispatchThrottlingRateInByte(dispatchThrottlingRateInByte)
                .ratePeriodInSecond(1)
                // TODO: 2/22/23 broker没有这样的配置的
                .relativeToPublishRate(type != Type.BROKER && config.isDispatchThrottlingRateRelativeToPublishRate())
                .build();
    }

    // TODO: 2/21/23 更新消费限流速率
    /**
     * Update dispatch-throttling-rate.
     * Topic-level has the highest priority, then namespace-level, and finally use dispatch-throttling-rate in
     * broker-level
     */
    public void updateDispatchRate() {
        // TODO: 2/21/23 这里为什么要单独处理 SUBSCRIPTION，应该是之前想把每一种type单独处理吧
        switch (type) {
            case SUBSCRIPTION:
                updateDispatchRate(topic.getSubscriptionDispatchRate());
                return;
        }

        // TODO: 2/22/23 如果是type=broker ，dispatchRate是获取不到值的
        Optional<DispatchRate> dispatchRate = getTopicPolicyDispatchRate(brokerService, topicName, type);
        if (!dispatchRate.isPresent()) {
            getPoliciesDispatchRateAsync(brokerService).thenAccept(dispatchRateOp -> {
                // todo 不存在，则创建
                if (!dispatchRateOp.isPresent()) {
                    dispatchRateOp = Optional.of(createDispatchRate());
                }
                // todo 存在，则update操作
                updateDispatchRate(dispatchRateOp.get());
                if (type == Type.BROKER) {
                  log.info("configured broker message-dispatch rate {}", dispatchRateOp.get());
                } else {
                  log.info("[{}] configured {} message-dispatch rate at broker {}",
                          this.topicName, type, dispatchRateOp.get());
                }
            }).exceptionally(ex -> {
                log.error("[{}] failed to get the dispatch rate policy from the namespace resource for type {}",
                        topicName, type, ex);
                return null;
            });
        } else {
            updateDispatchRate(dispatchRate.get());
            log.info("[{}] configured {} message-dispatch rate at broker {}", this.topicName, type, dispatchRate.get());
        }
    }

    public static boolean isDispatchRateNeeded(BrokerService brokerService, Optional<Policies> policies,
            String topicName, Type type) {
        final ServiceConfiguration serviceConfig = brokerService.pulsar().getConfiguration();
        if (type == Type.BROKER) {
            return brokerService.getBrokerDispatchRateLimiter().isDispatchRateLimitingEnabled();
        }

        Optional<DispatchRate> dispatchRate = getTopicPolicyDispatchRate(brokerService, topicName, type);
        if (dispatchRate.isPresent()) {
            return true;
        }

        policies = policies.isPresent() ? policies : getPolicies(brokerService, topicName);
        return isDispatchRateNeeded(serviceConfig, policies, topicName, type);
    }

    // TODO: 2/21/23 获取topic的消费限流策略
    public static Optional<DispatchRate> getTopicPolicyDispatchRate(BrokerService brokerService,
                                                                    String topicName, Type type) {
        Optional<DispatchRate> dispatchRate = Optional.empty();
        final ServiceConfiguration serviceConfiguration = brokerService.pulsar().getConfiguration();
        // TODO: 2/21/23 topic策略必须在 systemTopicEnabled=true，topicLevelPoliciesEnabled=true下面才有效
        if (serviceConfiguration.isSystemTopicEnabled() && serviceConfiguration.isTopicLevelPoliciesEnabled()) {
            try {
                // todo 如果设置了topic 的policy
                switch (type) {
                    case TOPIC:
                        dispatchRate = Optional.ofNullable(brokerService.pulsar().getTopicPoliciesService()
                                .getTopicPolicies(TopicName.get(topicName)))
                                .map(TopicPolicies::getDispatchRate);
                        break;
                    case SUBSCRIPTION:
                        dispatchRate = Optional.ofNullable(brokerService.pulsar().getTopicPoliciesService()
                                .getTopicPolicies(TopicName.get(topicName)))
                                .map(TopicPolicies::getSubscriptionDispatchRate);
                        break;
                    case REPLICATOR:
                        dispatchRate = Optional.ofNullable(brokerService.pulsar().getTopicPoliciesService()
                                .getTopicPolicies(TopicName.get(topicName)))
                                .map(TopicPolicies::getReplicatorDispatchRate);
                        break;
                    default:
                        break;
                }
            } catch (BrokerServiceException.TopicPoliciesCacheNotInitException e) {
                log.debug("Topic {} policies have not been initialized yet.", topicName);
            } catch (Exception e) {
                log.debug("[{}] Failed to get topic dispatch rate. ", topicName, e);
            }
        }

        return dispatchRate;
    }

    public static boolean isDispatchRateNeeded(final ServiceConfiguration serviceConfig,
            final Optional<Policies> policies, final String topicName, final Type type) {
        DispatchRate dispatchRate = getPoliciesDispatchRate(serviceConfig.getClusterName(), policies, type);
        if (dispatchRate == null) {
            switch (type) {
                case TOPIC:
                    return serviceConfig.getDispatchThrottlingRatePerTopicInMsg() > 0
                        || serviceConfig.getDispatchThrottlingRatePerTopicInByte() > 0;
                case SUBSCRIPTION:
                    return serviceConfig.getDispatchThrottlingRatePerSubscriptionInMsg() > 0
                        || serviceConfig.getDispatchThrottlingRatePerSubscriptionInByte() > 0;
                case REPLICATOR:
                    return serviceConfig.getDispatchThrottlingRatePerReplicatorInMsg() > 0
                        || serviceConfig.getDispatchThrottlingRatePerReplicatorInByte() > 0;
                default:
                    log.error("error DispatchRateLimiter type: {} ", type);
                    return false;
            }
        }
        return true;
    }

    @SuppressWarnings("deprecation")
    public void onPoliciesUpdate(Policies data) {
        String cluster = brokerService.pulsar().getConfiguration().getClusterName();

        DispatchRate dispatchRate;

        switch (type) {
            case TOPIC:
                dispatchRate = data.topicDispatchRate.get(cluster);
                if (dispatchRate == null) {
                    dispatchRate = data.clusterDispatchRate.get(cluster);
                }
                break;
            case SUBSCRIPTION:
                dispatchRate = data.subscriptionDispatchRate.get(cluster);
                break;
            case REPLICATOR:
                dispatchRate = data.replicatorDispatchRate.get(cluster);
                break;
            default:
                log.error("error DispatchRateLimiter type: {} ", type);
                dispatchRate = null;
        }

        // update dispatch-rate only if it's configured in policies else ignore
        if (dispatchRate != null) {
            final DispatchRate newDispatchRate = createDispatchRate();

            // if policy-throttling rate is disabled and cluster-throttling is enabled then apply
            // cluster-throttling rate
            if (!isDispatchRateEnabled(dispatchRate) && isDispatchRateEnabled(newDispatchRate)) {
                dispatchRate = newDispatchRate;
            }
            updateDispatchRate(dispatchRate);
        }
    }

    @SuppressWarnings("deprecation")
    public static DispatchRateImpl getPoliciesDispatchRate(final String cluster,
                                                           Optional<Policies> policies,
                                                           Type type) {
        // return policy-dispatch rate only if it's enabled in policies
        return policies.map(p -> {
            DispatchRateImpl dispatchRate;
            switch (type) {
                case TOPIC:
                    dispatchRate = p.topicDispatchRate.get(cluster);
                    if (dispatchRate == null) {
                        dispatchRate = p.clusterDispatchRate.get(cluster);
                    }
                    break;
                case SUBSCRIPTION:
                    dispatchRate = p.subscriptionDispatchRate.get(cluster);
                    break;
                case REPLICATOR:
                    dispatchRate = p.replicatorDispatchRate.get(cluster);
                    break;
                default:
                    log.error("error DispatchRateLimiter type: {} ", type);
                    return null;
            }
            return isDispatchRateEnabled(dispatchRate) ? dispatchRate : null;
        }).orElse(null);
    }


    /**
     * Gets configured dispatch-rate from namespace policies. Returns null if dispatch-rate is not configured
     *
     * @return
     */
    public CompletableFuture<Optional<DispatchRate>> getPoliciesDispatchRateAsync(BrokerService brokerService) {
        if (topicName == null) {
            return CompletableFuture.completedFuture(Optional.empty());
        }
        final String cluster = brokerService.pulsar().getConfiguration().getClusterName();
        return getPoliciesAsync(brokerService, topicName).thenApply(policiesOp ->
                Optional.ofNullable(getPoliciesDispatchRate(cluster, policiesOp, type)));
    }

    public static CompletableFuture<Optional<Policies>> getPoliciesAsync(BrokerService brokerService,
         String topicName) {
        final NamespaceName namespace = TopicName.get(topicName).getNamespaceObject();
        return brokerService.pulsar().getPulsarResources().getNamespaceResources().getPoliciesAsync(namespace);
    }

    public static Optional<Policies> getPolicies(BrokerService brokerService, String topicName) {
        final NamespaceName namespace = TopicName.get(topicName).getNamespaceObject();
        return brokerService.pulsar().getPulsarResources().getNamespaceResources().getPoliciesIfCached(namespace);
    }

    /**
     * Update dispatch rate by updating msg and byte rate-limiter. If dispatch-rate is configured < 0 then it closes
     * the rate-limiter and disables appropriate rate-limiter.
     *
     * @param dispatchRate
     */
    public synchronized void updateDispatchRate(DispatchRate dispatchRate) {
        // synchronized to prevent race condition from concurrent zk-watch
        log.info("setting message-dispatch-rate {}", dispatchRate);

        // TODO: 2/22/23 msg，byte速率+时间
        long msgRate = dispatchRate.getDispatchThrottlingRateInMsg();
        long byteRate = dispatchRate.getDispatchThrottlingRateInByte();
        long ratePeriod = dispatchRate.getRatePeriodInSecond();

        // TODO: 2/22/23 是否和生产速率关联
        Supplier<Long> permitUpdaterMsg = dispatchRate.isRelativeToPublishRate()
                ? () -> getRelativeDispatchRateInMsg(dispatchRate)
                : null;
        // update msg-rateLimiter
        if (msgRate > 0) {
            if (this.dispatchRateLimiterOnMessage == null) {
                // TODO: 2/22/23 创建一个新的task
                this.dispatchRateLimiterOnMessage =
                        RateLimiter.builder()
                                .scheduledExecutorService(brokerService.pulsar().getExecutor())
                                .permits(msgRate)
                                .rateTime(ratePeriod)
                                .timeUnit(TimeUnit.SECONDS)
                                .permitUpdater(permitUpdaterMsg)
                                .isDispatchOrPrecisePublishRateLimiter(true) // TODO: 2/22/23 消费limiter需要设置为true
                                .build();
            } else {
                // TODO: 2/22/23 先把现有的task停掉，再启动一个新的task
                this.dispatchRateLimiterOnMessage.setRate(msgRate, dispatchRate.getRatePeriodInSecond(),
                        TimeUnit.SECONDS, permitUpdaterMsg);
            }
        } else {// TODO: 2/22/23 如果设置为<=0的值，则关闭现有的task
            // message-rate should be disable and close
            if (this.dispatchRateLimiterOnMessage != null) {
                this.dispatchRateLimiterOnMessage.close();
                this.dispatchRateLimiterOnMessage = null;
            }
        }

        Supplier<Long> permitUpdaterByte = dispatchRate.isRelativeToPublishRate()
                ? () -> getRelativeDispatchRateInByte(dispatchRate)
                : null;
        // TODO: 2/22/23 byte和上面一样的逻辑
        // update byte-rateLimiter
        if (byteRate > 0) {
            if (this.dispatchRateLimiterOnByte == null) {
                this.dispatchRateLimiterOnByte =
                        RateLimiter.builder()
                                .scheduledExecutorService(brokerService.pulsar().getExecutor())
                                .permits(byteRate)
                                .rateTime(ratePeriod)
                                .timeUnit(TimeUnit.SECONDS)
                                .permitUpdater(permitUpdaterByte)
                                .isDispatchOrPrecisePublishRateLimiter(true)
                                .build();
            } else {
                this.dispatchRateLimiterOnByte.setRate(byteRate, dispatchRate.getRatePeriodInSecond(),
                        TimeUnit.SECONDS, permitUpdaterByte);
            }
        } else {
            // message-rate should be disable and close
            if (this.dispatchRateLimiterOnByte != null) {
                this.dispatchRateLimiterOnByte.close();
                this.dispatchRateLimiterOnByte = null;
            }
        }
    }

    private long getRelativeDispatchRateInMsg(DispatchRate dispatchRate) {
        return (topic != null && dispatchRate != null)
                ? (long) topic.getLastUpdatedAvgPublishRateInMsg() + dispatchRate.getDispatchThrottlingRateInMsg()
                : 0;
    }

    private long getRelativeDispatchRateInByte(DispatchRate dispatchRate) {
        return (topic != null && dispatchRate != null)
                ? (long) topic.getLastUpdatedAvgPublishRateInByte() + dispatchRate.getDispatchThrottlingRateInByte()
                : 0;
    }

    /**
     * Get configured msg dispatch-throttling rate. Returns -1 if not configured
     *
     * @return
     */
    public long getDispatchRateOnMsg() {
        return dispatchRateLimiterOnMessage != null ? dispatchRateLimiterOnMessage.getRate() : -1;
    }

    /**
     * Get configured byte dispatch-throttling rate. Returns -1 if not configured
     *
     * @return
     */
    public long getDispatchRateOnByte() {
        return dispatchRateLimiterOnByte != null ? dispatchRateLimiterOnByte.getRate() : -1;
    }


    public static boolean isDispatchRateEnabled(DispatchRate dispatchRate) {
        return dispatchRate != null && (dispatchRate.getDispatchThrottlingRateInMsg() > 0
                || dispatchRate.getDispatchThrottlingRateInByte() > 0);
    }

    public void close() {
        // close rate-limiter
        if (dispatchRateLimiterOnMessage != null) {
            dispatchRateLimiterOnMessage.close();
            dispatchRateLimiterOnMessage = null;
        }
        if (dispatchRateLimiterOnByte != null) {
            dispatchRateLimiterOnByte.close();
            dispatchRateLimiterOnByte = null;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(DispatchRateLimiter.class);
}
