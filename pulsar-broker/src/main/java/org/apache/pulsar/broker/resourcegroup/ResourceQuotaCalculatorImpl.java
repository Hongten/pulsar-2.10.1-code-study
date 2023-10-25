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

import static java.lang.Float.max;
import static java.lang.Math.abs;
import lombok.val;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResourceQuotaCalculatorImpl implements ResourceQuotaCalculator {
    @Override
    public long computeLocalQuota(long confUsage, long myUsage, long[] allUsages) throws PulsarAdminException {
        // ToDo: work out the initial conditions: we may allow a small number of "first few iterations" to go
        // unchecked as we get some history of usage, or follow some other "TBD" method.

        long totalUsage = 0;

        // todo 统计所有的使用量
        for (long usage : allUsages) {
            totalUsage += usage;
        }

        // todo 这个代码可以移动到最前面
        if (confUsage < 0) {
            // This can happen if the RG is not configured with this particular limit (message or byte count) yet.
            val retVal = -1;
            if (log.isDebugEnabled()) {
                log.debug("Configured usage ({}) is not set; returning a special value ({}) for calculated quota",
                        confUsage, retVal);
            }
            return retVal;
        }

        if (myUsage < 0 || totalUsage < 0) {
            String errMesg = String.format("Local usage (%d) or total usage (%d) is negative",
                    myUsage, totalUsage);
            log.error(errMesg);
            throw new PulsarAdminException(errMesg);
        }

        // If the total usage is zero (which may happen during initial transients), just return the configured value.
        // The caller is expected to check the value returned, or not call here with a zero global usage.
        // [This avoids a division by zero when calculating the local share.]
        if (totalUsage == 0) {
            if (log.isDebugEnabled()) {
                log.debug("computeLocalQuota: totalUsage is zero; "
                        + "returning the configured usage ({}) as new local quota",
                        confUsage);
            }
            return confUsage;
        }

        if (myUsage > totalUsage) {
            String errMesg = String.format("Local usage (%d) is greater than total usage (%d)",
                    myUsage, totalUsage);
            // Log as a warning [in case this can happen transiently (?)].
            log.warn(errMesg);
        }

        // TODO: 2/10/23 剩余可用值。
        //  如果我们配置的quota值超过了总流量，说明还有剩余。
        //  如果我们配置的quota值小于总流量，说明超quota了，没有剩余流量
        // How much unused capacity is left over?
        float residual = confUsage - totalUsage;

        // New quota is the old usage incremented by any residual as a ratio of the local usage to the total usage.
        // This should result in the calculatedQuota increasing proportionately if total usage is less than the
        // configured usage, and reducing proportionately if the total usage is greater than the configured usage.
        // Capped to 1, to prevent negative or zero setting of quota.
        // the rate limiter code assumes that rate value of 0 or less to mean that no rate limit should be applied
        // TODO: 2/10/23 计算当前流量和总流量的比值，根据比例调整接下来的流量变化。
        //  e.g.如果某个Resourcegroup限制流量为100,某时刻有三个broker本别使用了10，50，30的流量。
        //  第一阶段，后台会先计算出剩余流量配额为10，这时每个broker就会增加未使用的流量配额10，此时三个broker的流量分别为10， 60， 40.
        //  第二阶段，后台计算后，总的使用流量120，已经超过了quota值100，这个时候，后台会按照比例对broker进行限制，最终三个broker的流量变为：11.11， 55.56， 33.33
        float myUsageFraction = (float) myUsage / totalUsage;
        float calculatedQuota = max(myUsage + residual * myUsageFraction, 1);

        // TODO: 2/10/23 调整后的本地broker流量quota值
        val longCalculatedQuota = (long) calculatedQuota;
        log.info("computeLocalQuota: myUsage={}, totalUsage={}, myFraction={}; newQuota returned={} [long: {}]",
                myUsage, totalUsage, myUsageFraction, calculatedQuota, longCalculatedQuota);

        return longCalculatedQuota;
    }

    @Override
    // Return true if a report needs to be sent for the current round; false if it can be suppressed for this round.
    public boolean needToReportLocalUsage(long currentBytesUsed, long lastReportedBytes,
                                                    long currentMessagesUsed, long lastReportedMessages,
                                                    long lastReportTimeMSecsSinceEpoch) {
        // If we are about to go more than maxUsageReportSuppressRounds without reporting, send a report.
        long currentTimeMSecs = System.currentTimeMillis(); // todo 当前时间
        long mSecsSinceLastReport = currentTimeMSecs - lastReportTimeMSecsSinceEpoch; // todo 间隔时间
        // todo 如果间隔时间超过了默认上报时间，返回true, 即上报
        if (mSecsSinceLastReport >= ResourceGroupService.maxIntervalForSuppressingReportsMSecs) {
            return true;
        }

        // todo （message数量，大小）百分比变化（增加，减少）大于我们设置的容忍值（默认5%)，就上报
        // If the percentage change (increase or decrease) in usage is more than a threshold for
        // either bytes or messages, send a report.
        final float toleratedDriftPercentage = ResourceGroupService.UsageReportSuppressionTolerancePercentage;
        if (currentBytesUsed > 0) {
            long diff = abs(currentBytesUsed - lastReportedBytes);
            float diffPercentage = (float) diff * 100 / lastReportedBytes;
            if (diffPercentage > toleratedDriftPercentage) {
                return true;
            }
        }

        if (currentMessagesUsed > 0) {
            long diff = abs(currentMessagesUsed - lastReportedMessages);
            float diffPercentage = (float) diff * 100 / lastReportedMessages;
            if (diffPercentage > toleratedDriftPercentage) {
                return true;
            }
        }

        return false;
    }

    private static final Logger log = LoggerFactory.getLogger(ResourceQuotaCalculatorImpl.class);
}
