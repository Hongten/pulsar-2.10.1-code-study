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
package org.apache.pulsar.broker.resourcegroup;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.resourcegroup.ResourceGroup.BytesAndMessagesCount;
import org.apache.pulsar.broker.service.PublishRateLimiter;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.policies.data.ResourceGroup;
import org.apache.pulsar.common.util.RateLimitFunction;
import org.apache.pulsar.common.util.RateLimiter;

public class ResourceGroupPublishLimiter implements PublishRateLimiter, RateLimitFunction, AutoCloseable  {
    // todo publish msg 最大值
    protected volatile long publishMaxMessageRate = 0;
    // todo publish byte 最大值
    protected volatile long publishMaxByteRate = 0;
    protected volatile boolean publishThrottlingEnabled = false;
    // TODO: 2/14/23 message的LImiter
    private volatile RateLimiter publishRateLimiterOnMessage;
    // TODO: 2/14/23 byte的Limiter
    private volatile RateLimiter publishRateLimiterOnByte;
    private final ScheduledExecutorService scheduledExecutorService;

    // TODO: 2/14/23 这里是RateLimitFunction map <topicName, fun>
    ConcurrentHashMap<String, RateLimitFunction> rateLimitFunctionMap = new ConcurrentHashMap<>();

    // TODO: 2/10/23 初始化ResourceGroupPublishLimiter。当RG数量多以后，scheduledExecutorService线程池大小为20，有性能问题
    public ResourceGroupPublishLimiter(ResourceGroup resourceGroup, ScheduledExecutorService scheduledExecutorService) {
        this.scheduledExecutorService = scheduledExecutorService;
        update(resourceGroup);
    }

    @Override
    public void checkPublishRate() {
        // No-op
    }

    @Override
    public void incrementPublishCount(int numOfMessages, long msgSizeInBytes) {
        // No-op
    }

    @Override
    public boolean resetPublishCount() {
        return true;
    }

    @Override
    public boolean isPublishRateExceeded() {
        return false;
    }

    @Override
    public void update(Policies policies, String clusterName) {
      // No-op
    }

    @Override
    public void update(PublishRate maxPublishRate) {
      // No-op
    }

    public void update(BytesAndMessagesCount maxPublishRate) {
        // TODO: 2/14/23 更新调整后的限流quota值
        update(maxPublishRate.messages, maxPublishRate.bytes);
    }

    public BytesAndMessagesCount getResourceGroupPublishValues() {
        BytesAndMessagesCount bmc = new BytesAndMessagesCount();
        bmc.bytes = this.publishMaxByteRate;
        bmc.messages = this.publishMaxMessageRate;
        return bmc;
    }

    public void update(ResourceGroup resourceGroup) {
        long publishRateInMsgs = 0, publishRateInBytes = 0;
        if (resourceGroup != null) {
            // TODO: 2/10/23 获取到对应的bytesin， messageIn
            publishRateInBytes = resourceGroup.getPublishRateInBytes() == null
                    ? -1 : resourceGroup.getPublishRateInBytes();
            publishRateInMsgs = resourceGroup.getPublishRateInMsgs() == null
                    ? -1 : resourceGroup.getPublishRateInMsgs();
        }

        update(publishRateInMsgs, publishRateInBytes);
    }

    public void update(long publishRateInMsgs, long publishRateInBytes) {
        replaceLimiters(() -> {
            if (publishRateInMsgs > 0 || publishRateInBytes > 0) {
                // todo 感觉这个参数没有起任何作用
                this.publishThrottlingEnabled = true;
                // TODO: 2/10/23 获取大于等于0的值 这里的值是从RG里面获取，这里的值为RG调整后的quota值
                this.publishMaxMessageRate = Math.max(publishRateInMsgs, 0);
                this.publishMaxByteRate = Math.max(publishRateInBytes, 0);
                if (this.publishMaxMessageRate > 0) {
                    // TODO: 2/10/23 每隔1s执行一次apply方法
                    publishRateLimiterOnMessage = RateLimiter.builder()
                            .scheduledExecutorService(scheduledExecutorService)
                            .permits(publishMaxMessageRate)
                            .rateTime(1L)
                            .timeUnit(TimeUnit.SECONDS)
                            .rateLimitFunction(this::apply)
                            .build();
                }
                if (this.publishMaxByteRate > 0) {
                    // TODO: 2/10/23 每隔1s执行一次apply方法
                    publishRateLimiterOnByte =
                    RateLimiter.builder()
                            .scheduledExecutorService(scheduledExecutorService)
                            .permits(publishMaxByteRate)
                            .rateTime(1L)
                            .timeUnit(TimeUnit.SECONDS)
                            .rateLimitFunction(this::apply)
                            .build();
                }
            } else {
                this.publishMaxMessageRate = 0;
                this.publishMaxByteRate = 0;
                this.publishThrottlingEnabled = false;
                publishRateLimiterOnMessage = null;
                publishRateLimiterOnByte = null;
            }
        });
    }

    // TODO: 2/15/23 返回true，说明都没有超quota。 false说明至少一个超quota了
    public boolean tryAcquire(int numbers, long bytes) {
        return (publishRateLimiterOnMessage == null || publishRateLimiterOnMessage.tryAcquire(numbers))
            && (publishRateLimiterOnByte == null || publishRateLimiterOnByte.tryAcquire(bytes));
    }

    public void registerRateLimitFunction(String name, RateLimitFunction func) {
        // TODO: 2/15/23 <topicName, func>
        rateLimitFunctionMap.put(name, func);
    }

    public void unregisterRateLimitFunction(String name) {
        rateLimitFunctionMap.remove(name);
    }

    private void replaceLimiters(Runnable updater) {
        RateLimiter previousPublishRateLimiterOnMessage = publishRateLimiterOnMessage;
        publishRateLimiterOnMessage = null;
        RateLimiter previousPublishRateLimiterOnByte = publishRateLimiterOnByte;
        publishRateLimiterOnByte = null;
        try {
            if (updater != null) {
                // TODO: 2/10/23 调用run方法
                updater.run();
            }
        } finally {
            // Close previous limiters to prevent resource leakages.
            // Delay closing of previous limiters after new ones are in place so that updating the limiter
            // doesn't cause unavailability.
            if (previousPublishRateLimiterOnMessage != null) {
                previousPublishRateLimiterOnMessage.close();
            }
            if (previousPublishRateLimiterOnByte != null) {
                previousPublishRateLimiterOnByte.close();
            }
        }
    }

    @Override
    public void close() {
        // Unblock any producers, consumers waiting first.
        // This needs to be done before replacing the filters to null
        this.apply();
        replaceLimiters(null);
    }

    @Override
    public void apply() {
        // TODO: 2/14/23 获取到msg，byte limiter
        // Make sure that both the rate limiters are applied before opening the flood gates.
        RateLimiter currentTopicPublishRateLimiterOnMessage = publishRateLimiterOnMessage;
        RateLimiter currentTopicPublishRateLimiterOnByte = publishRateLimiterOnByte;
        if ((currentTopicPublishRateLimiterOnMessage != null
                && currentTopicPublishRateLimiterOnMessage.getAvailablePermits() <= 0)
            || (currentTopicPublishRateLimiterOnByte != null
                && currentTopicPublishRateLimiterOnByte.getAvailablePermits() <= 0)) {
            // TODO: 2/14/23 可用的permit小于零，直接返回
            return;
        }

        for (Map.Entry<String, RateLimitFunction> entry: rateLimitFunctionMap.entrySet()) {
            entry.getValue().apply();
        }
    }
}