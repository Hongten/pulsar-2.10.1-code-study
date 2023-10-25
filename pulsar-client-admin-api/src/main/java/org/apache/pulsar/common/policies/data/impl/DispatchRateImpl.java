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
package org.apache.pulsar.common.policies.data.impl;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.pulsar.common.policies.data.DispatchRate;

/**
 * Dispatch rate.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public final class DispatchRateImpl implements DispatchRate {

    private int dispatchThrottlingRateInMsg;
    private long dispatchThrottlingRateInByte;
    private boolean relativeToPublishRate;
    private int ratePeriodInSecond;

    public static DispatchRateImplBuilder builder() {
        return new DispatchRateImplBuilder();
    }

    public static class DispatchRateImplBuilder implements DispatchRate.Builder {

        // todo 消费msg个数控制
        private int dispatchThrottlingRateInMsg = -1;
        // todo 消费msg大小限制
        private long dispatchThrottlingRateInByte = -1;
        // todo 这个值默认为FALSE，如果设置为TRUE，则它会动态更新消费速率。
        //  可以理解为当生产速率增加的时候，那么动态调整消费速率来保障消费job的速率，避免消费不及的消息积压问题.
        //  但是需要指出的是，这样设置会给pulsar集群带来危险，可能把集群打卦，失去了消费端限流的意义。
        // todo throttle-dispatch-rate = (publish-rate + configured dispatch-rate)
        private boolean relativeToPublishRate = false; /* throttles dispatch relatively publish-rate */
        // todo 时长设置，默认为1s
        private int ratePeriodInSecond = 1; /* by default dispatch-rate will be calculate per 1 second */


        public DispatchRateImplBuilder dispatchThrottlingRateInMsg(int dispatchThrottlingRateInMsg) {
            this.dispatchThrottlingRateInMsg = dispatchThrottlingRateInMsg;
            return this;
        }

        public DispatchRateImplBuilder dispatchThrottlingRateInByte(long dispatchThrottlingRateInByte) {
            this.dispatchThrottlingRateInByte = dispatchThrottlingRateInByte;
            return this;
        }

        public DispatchRateImplBuilder relativeToPublishRate(boolean relativeToPublishRate) {
            this.relativeToPublishRate = relativeToPublishRate;
            return this;
        }

        public DispatchRateImplBuilder ratePeriodInSecond(int ratePeriodInSecond) {
            this.ratePeriodInSecond = ratePeriodInSecond;
            return this;
        }

        public DispatchRateImpl build() {
            return new DispatchRateImpl(dispatchThrottlingRateInMsg, dispatchThrottlingRateInByte,
                    relativeToPublishRate, ratePeriodInSecond);
        }
    }
}
