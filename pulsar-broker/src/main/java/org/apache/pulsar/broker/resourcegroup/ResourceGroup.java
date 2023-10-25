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

import io.prometheus.client.Counter;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import lombok.Getter;
import lombok.val;
import org.apache.pulsar.broker.resourcegroup.ResourceGroupService.ResourceGroupOpStatus;
import org.apache.pulsar.broker.service.resource.usage.NetworkUsage;
import org.apache.pulsar.broker.service.resource.usage.ResourceUsage;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The resource group (RG) data structure. One of these is created internally for every resource group configured.
 *
 * The RG name is considered a key, and must be unique. Being the key, the name can't be modified after the
 * RG is created.
 *
 * Tenants and Namespaces directly reference RGs. The usage is "vertical": i.e., a tenant or namespace references the
 * same RG across all of the monitoring classes it uses (Publish/Dispatch/...), instead of referencing one RG for
 * publish, another one for dispatch, etc.
 */
public class ResourceGroup {

    /**
     * Convenience class for bytes and messages counts, which are used together in a lot of the following code.
     */
    public static class BytesAndMessagesCount {
        public long bytes;
        public long messages;
    }

    /**
     * Usage "sense" for resource groups: publish and dispatch for now; may be more later (e.g., storage-monitoring)
     * ToDo: "class" is vague; is there a better term to call this?
     */
    public enum ResourceGroupMonitoringClass {
        Publish,
        Dispatch,
        // Storage;  // Punt this for now, until we have a clearer idea of the usage, statistics, etc.
    }

    /**
     * The entities that might reference RGs are tenants, namespaces (and maybe topics, later).
     */
    public enum ResourceGroupRefTypes {
        Tenants,
        Namespaces,
        // Topics;  // Punt this for when we support direct ref/under from topics.
    }

    // TODO: 2/10/23 每一个RG都有一个数据结构，里面存放这broker的quota，流量等信息，并且都有一个Publish Limiter
    // Default ctor: it is not expected that anything outside of this package will need to directly
    // construct a ResourceGroup (i.e., without going through ResourceGroupService).
    protected ResourceGroup(ResourceGroupService rgs, String name,
                            org.apache.pulsar.common.policies.data.ResourceGroup rgConfig) {
        this.rgs = rgs;
        // TODO: 2/10/23 resourcegroup 名称
        this.resourceGroupName = name;
        // TODO: 2/10/23 RG数据结构，里面存放这broker的quota，流量等信息
        this.setResourceGroupMonitoringClassFields();
        // TODO: 2/10/23 设置我们设置的quota值，configValuesPerPeriod
        this.setResourceGroupConfigParameters(rgConfig);
        // TODO: 2/10/23 设置默认的resource usage transport handler
        this.setDefaultResourceUsageTransportHandlers();
        // todo publish limiter , 为什么没有dispatch limiter呢？
        // TODO: 2/10/23 每一个ResourceGroup都有自己的Publish limiter ，
        //  每一个Limiter都用到了pulsar的线程池，那么如果RG的数量过大，这里有性能问题，pulsar本身的的这个线程池大小为20，
        //  是否可以单独创建一个线程池来提供服务呢？
        this.resourceGroupPublishLimiter = new ResourceGroupPublishLimiter(rgConfig, rgs.getPulsar().getExecutor());
        log.info("attaching publish rate limiter {} to {} get {}", this.resourceGroupPublishLimiter.toString(), name,
          this.getResourceGroupPublishLimiter());
    }

    // ctor for overriding the transport-manager fill/set buffer.
    // It is not expected that anything outside of this package will need to directly
    // construct a ResourceGroup (i.e., without going through ResourceGroupService).
    protected ResourceGroup(ResourceGroupService rgs, String rgName,
                            org.apache.pulsar.common.policies.data.ResourceGroup rgConfig,
                            ResourceUsagePublisher rgPublisher, ResourceUsageConsumer rgConsumer) {
        this.rgs = rgs;
        this.resourceGroupName = rgName;
        this.setResourceGroupMonitoringClassFields();
        this.setResourceGroupConfigParameters(rgConfig);
        // todo publish limiter , 为什么没有dispatch limiter呢？
        this.resourceGroupPublishLimiter = new ResourceGroupPublishLimiter(rgConfig, rgs.getPulsar().getExecutor());
        this.ruPublisher = rgPublisher;
        this.ruConsumer = rgConsumer;
    }

    // copy ctor: note does shallow copy.
    // The envisioned usage is for display of an RG's state, without scope for direct update.
    public ResourceGroup(ResourceGroup other) {
        this.resourceGroupName = other.resourceGroupName;
        this.rgs = other.rgs;
        this.resourceGroupPublishLimiter = other.resourceGroupPublishLimiter;
        this.setResourceGroupMonitoringClassFields();

        // ToDo: copy the monitoring class fields, and ruPublisher/ruConsumer from other, if required.

        this.resourceGroupNamespaceRefs = other.resourceGroupNamespaceRefs;
        this.resourceGroupTenantRefs = other.resourceGroupTenantRefs;

        for (int idx = 0; idx < ResourceGroupMonitoringClass.values().length; idx++) {
            PerMonitoringClassFields thisFields = this.monitoringClassFields[idx];
            PerMonitoringClassFields otherFields = other.monitoringClassFields[idx];

            thisFields.configValuesPerPeriod.bytes = otherFields.configValuesPerPeriod.bytes;
            thisFields.configValuesPerPeriod.messages = otherFields.configValuesPerPeriod.messages;

            thisFields.quotaForNextPeriod.bytes = otherFields.quotaForNextPeriod.bytes;
            thisFields.quotaForNextPeriod.messages = otherFields.quotaForNextPeriod.messages;

            thisFields.usedLocallySinceLastReport.bytes = otherFields.usedLocallySinceLastReport.bytes;
            thisFields.usedLocallySinceLastReport.messages = otherFields.usedLocallySinceLastReport.messages;

            thisFields.lastResourceUsageFillTimeMSecsSinceEpoch = otherFields.lastResourceUsageFillTimeMSecsSinceEpoch;

            thisFields.numSuppressedUsageReports = otherFields.numSuppressedUsageReports;

            thisFields.totalUsedLocally.bytes = otherFields.totalUsedLocally.bytes;
            thisFields.totalUsedLocally.messages = otherFields.totalUsedLocally.messages;

            // ToDo: Deep copy instead?
            thisFields.usageFromOtherBrokers = otherFields.usageFromOtherBrokers;
        }
    }

    protected void updateResourceGroup(org.apache.pulsar.common.policies.data.ResourceGroup rgConfig) {
        this.setResourceGroupConfigParameters(rgConfig);
        val pubBmc = new BytesAndMessagesCount();
        pubBmc.messages = rgConfig.getPublishRateInMsgs();
        pubBmc.bytes = rgConfig.getPublishRateInBytes();
        this.resourceGroupPublishLimiter.update(pubBmc);
    }

    protected long getResourceGroupNumOfNSRefs() {
        return this.resourceGroupNamespaceRefs.size();
    }

    protected long getResourceGroupNumOfTenantRefs() {
        return this.resourceGroupTenantRefs.size();
    }

    protected ResourceGroupOpStatus registerUsage(String name, ResourceGroupRefTypes refType, boolean ref,
                                                  ResourceUsageTransportManager transportManager) {
        Set<String> set;

        switch (refType) {
            default:
                return ResourceGroupOpStatus.NotSupported;
            case Tenants:
                set = this.resourceGroupTenantRefs;
                break;
            case Namespaces:
                set = this.resourceGroupNamespaceRefs;
                break;
        }

        if (ref) {
            // register operation
            if (set.contains(name)) {
                return ResourceGroupOpStatus.Exists;
            }
            set.add(name);

            // If this is the first ref, register with the transport manager.
            if (this.resourceGroupTenantRefs.size() + this.resourceGroupNamespaceRefs.size() == 1) {
                if (log.isDebugEnabled()) {
                    log.debug("registerUsage for RG={}: registering with transport-mgr", this.resourceGroupName);
                }
                // todo 只要有一个resourcegroup 被namespace引用，则会注册一个publisher,和一个consumer
                transportManager.registerResourceUsagePublisher(this.ruPublisher);
                transportManager.registerResourceUsageConsumer(this.ruConsumer);
            }
        } else {
            // unregister operation
            if (!set.contains(name)) {
                return ResourceGroupOpStatus.DoesNotExist;
            }
            set.remove(name);

            // If this was the last ref, unregister from the transport manager.
            if (this.resourceGroupTenantRefs.size() + this.resourceGroupNamespaceRefs.size() == 0) {
                if (log.isDebugEnabled()) {
                    log.debug("unRegisterUsage for RG={}: un-registering from transport-mgr", this.resourceGroupName);
                }
                transportManager.unregisterResourceUsageConsumer(this.ruConsumer);
                transportManager.unregisterResourceUsagePublisher(this.ruPublisher);
            }
        }

        return ResourceGroupOpStatus.OK;
    }

    // Transport manager mandated op.
    public String getID() {
        return this.resourceGroupName;
    }

    // Transport manager mandated op.
    public void rgFillResourceUsage(ResourceUsage resourceUsage) {
        NetworkUsage p;
        // TODO: 2/14/23 owner= RG name
        resourceUsage.setOwner(this.getID());

        // TODO: 2/14/23 获取publish的网络情况
        p = resourceUsage.setPublish();
        // TODO: 2/14/23 这里好像没有使用到返回值 true or false
        this.setUsageInMonitoredEntity(ResourceGroupMonitoringClass.Publish, p);

        p = resourceUsage.setDispatch();
        this.setUsageInMonitoredEntity(ResourceGroupMonitoringClass.Dispatch, p);

        // Punt storage for now.
    }

    // Transport manager mandated op.
    public void rgResourceUsageListener(String broker, ResourceUsage resourceUsage) {
        NetworkUsage p;
        // TODO: 2/14/23 获取到broker的 publish 的网络使用情况 
        p = resourceUsage.getPublish();
        // TODO: 2/14/23 更新该broker的 publish 网络使用情况 
        this.getUsageFromMonitoredEntity(ResourceGroupMonitoringClass.Publish, p, broker);

        // TODO: 2/14/23 获取到broker的 dispatch 的网络使用情况 
        p = resourceUsage.getDispatch();
        this.getUsageFromMonitoredEntity(ResourceGroupMonitoringClass.Dispatch, p, broker);

        // Punt storage for now.
    }

    protected BytesAndMessagesCount getConfLimits(ResourceGroupMonitoringClass monClass) throws PulsarAdminException {
        this.checkMonitoringClass(monClass);
        BytesAndMessagesCount retval = new BytesAndMessagesCount();
        final PerMonitoringClassFields monEntity = this.monitoringClassFields[monClass.ordinal()];
        monEntity.localUsageStatsLock.lock();
        try {
            // TODO: 2/14/23 这是我们配置的该RG上面的quota值
            retval.bytes = monEntity.configValuesPerPeriod.bytes;
            retval.messages = monEntity.configValuesPerPeriod.messages;
        } finally {
            monEntity.localUsageStatsLock.unlock();
        }

        return retval;
    }

    // TODO: 2/14/23 这里只是做增量操作，为啥呢？
    protected void incrementLocalUsageStats(ResourceGroupMonitoringClass monClass, BytesAndMessagesCount stats)
                                                                                        throws PulsarAdminException {
        this.checkMonitoringClass(monClass);
        // TODO: 2/14/23 获取到该RG的publish，or dispatch的数据结构 
        final PerMonitoringClassFields monEntity = this.monitoringClassFields[monClass.ordinal()];
        monEntity.localUsageStatsLock.lock();
        try {
            // TODO: 2/14/23 把该broker上面的RG的网络使用情况增加 
            monEntity.usedLocallySinceLastReport.bytes += stats.bytes;
            monEntity.usedLocallySinceLastReport.messages += stats.messages;
        } finally {
            monEntity.localUsageStatsLock.unlock();
        }
    }

    protected BytesAndMessagesCount getLocalUsageStats(ResourceGroupMonitoringClass monClass)
                                                                                        throws PulsarAdminException {
        this.checkMonitoringClass(monClass);
        BytesAndMessagesCount retval = new BytesAndMessagesCount();
        final PerMonitoringClassFields monEntity = this.monitoringClassFields[monClass.ordinal()];
        monEntity.localUsageStatsLock.lock();
        try {
            retval.bytes = monEntity.usedLocallySinceLastReport.bytes;
            retval.messages = monEntity.usedLocallySinceLastReport.messages;
        } finally {
            monEntity.localUsageStatsLock.unlock();
        }

        return retval;
    }

    protected BytesAndMessagesCount getLocalUsageStatsCumulative(ResourceGroupMonitoringClass monClass)
            throws PulsarAdminException {
        this.checkMonitoringClass(monClass);
        BytesAndMessagesCount retval = new BytesAndMessagesCount();
        final PerMonitoringClassFields monEntity = this.monitoringClassFields[monClass.ordinal()];
        monEntity.localUsageStatsLock.lock();
        try {
            // If the total wasn't accumulated yet (i.e., a report wasn't sent yet), just return the
            // partial accumulation in usedLocallySinceLastReport.
            if (monEntity.totalUsedLocally.messages == 0) {
                retval.bytes = monEntity.usedLocallySinceLastReport.bytes;
                retval.messages = monEntity.usedLocallySinceLastReport.messages;
            } else {
                retval.bytes = monEntity.totalUsedLocally.bytes;
                retval.messages = monEntity.totalUsedLocally.messages;
            }
        } finally {
            monEntity.localUsageStatsLock.unlock();
        }

        return retval;
    }

    protected BytesAndMessagesCount getLocalUsageStatsFromBrokerReports(ResourceGroupMonitoringClass monClass)
            throws PulsarAdminException {
        this.checkMonitoringClass(monClass);
        val retval = new BytesAndMessagesCount();
        final PerMonitoringClassFields monEntity = this.monitoringClassFields[monClass.ordinal()];
        String myBrokerId = this.rgs.getPulsar().getBrokerServiceUrl();
        PerBrokerUsageStats pbus;

        monEntity.usageFromOtherBrokersLock.lock();
        try {
            // TODO: 2/14/23 通过消费内部topic而获得的该RG在所有broker上面的网络使用信息，这里只是获取到该broker的上面RG的网络使用情况
            pbus = monEntity.usageFromOtherBrokers.get(myBrokerId);
        } finally {
            monEntity.usageFromOtherBrokersLock.unlock();
        }

        if (pbus != null) {
            retval.bytes = pbus.usedValues.bytes;
            retval.messages = pbus.usedValues.messages;
        } else {
            log.info("getLocalUsageStatsFromBrokerReports: no usage report found for broker={} and monClass={}",
                    myBrokerId, monClass);
        }

        return retval;
    }

    protected BytesAndMessagesCount getGlobalUsageStats(ResourceGroupMonitoringClass monClass)
                                                                                        throws PulsarAdminException {
        this.checkMonitoringClass(monClass);

        final PerMonitoringClassFields monEntity = this.monitoringClassFields[monClass.ordinal()];
        // todo 获取锁
        monEntity.usageFromOtherBrokersLock.lock();
        BytesAndMessagesCount retStats = new BytesAndMessagesCount();
        try {
            // TODO: 2/14/23 通过消费内部topic而获得的该RG在所有broker上面的网络使用信息，所以，这里是该RG在集群里面的网络总使用量
            monEntity.usageFromOtherBrokers.forEach((broker, brokerUsage) -> {
                retStats.bytes += brokerUsage.usedValues.bytes;
                retStats.messages += brokerUsage.usedValues.messages;
            });
        } finally {
            // todo 释放锁
            monEntity.usageFromOtherBrokersLock.unlock();
        }

        return retStats;
    }

    // TODO: 2/14/23 这里没有更新dispatch的限流信息
    protected BytesAndMessagesCount updateLocalQuota(ResourceGroupMonitoringClass monClass,
                                                     BytesAndMessagesCount newQuota) throws PulsarAdminException {
        // Only the Publish side is functional now; add the Dispatch side code when the consume side is ready.
        // TODO: 2/10/23 这里指的是 Dispatch 没有ready吗？因为即便是Dispatch，这里也不会进行处理，这里也说明了为什么消费的quota不生效的原因，
        //  即PerMonitoringClassFields的Dispatch一直不更新导致的
        if (!ResourceGroupMonitoringClass.Publish.equals(monClass)) {
            if (log.isDebugEnabled()) {
                log.debug("Doing nothing for monClass={}; only Publish is functional", monClass);
            }
            return null;
        }

        this.checkMonitoringClass(monClass);
        // todo 当前的quota值
        BytesAndMessagesCount oldBMCount;

        final PerMonitoringClassFields monEntity = this.monitoringClassFields[monClass.ordinal()];
        monEntity.localUsageStatsLock.lock();
        oldBMCount = monEntity.quotaForNextPeriod;
        try {
            // todo 更新为最新的quota值
            // TODO: 2/10/23 第一是更新resourcegroup里面quotaForNextPeriod 的值，
            //  其次是更新resourceGroupPublishLimiter，这个是真正做流量限制的
            monEntity.quotaForNextPeriod = newQuota;
            // TODO: 2/14/23 之前所有的动作都是为这里所准备的
            this.resourceGroupPublishLimiter.update(newQuota);
        } finally {
            monEntity.localUsageStatsLock.unlock();
        }
        if (log.isDebugEnabled()) {
            log.debug("updateLocalQuota for RG={}: set local {} quota to bytes={}, messages={}",
                    this.resourceGroupName, monClass, newQuota.bytes, newQuota.messages);
        }

        // todo 返回当前的quota值
        return oldBMCount;
    }

    protected BytesAndMessagesCount getRgPublishRateLimiterValues() {
        BytesAndMessagesCount retVal;
        final PerMonitoringClassFields monEntity =
                                        this.monitoringClassFields[ResourceGroupMonitoringClass.Publish.ordinal()];
        monEntity.localUsageStatsLock.lock();
        try {
            retVal = this.resourceGroupPublishLimiter.getResourceGroupPublishValues();
        } finally {
            monEntity.localUsageStatsLock.unlock();
        }

        return retVal;
    }

    // Visibility for unit testing
    protected static double getRgRemoteUsageByteCount (String rgName, String monClassName, String brokerName) {
        return rgRemoteUsageReportsBytes.labels(rgName, monClassName, brokerName).get();
    }

    // Visibility for unit testing
    protected static double getRgRemoteUsageMessageCount (String rgName, String monClassName, String brokerName) {
        return rgRemoteUsageReportsMessages.labels(rgName, monClassName, brokerName).get();
    }

    // Visibility for unit testing
    protected static double getRgUsageReportedCount (String rgName, String monClassName) {
        return rgLocalUsageReportCount.labels(rgName, monClassName).get();
    }

    // Visibility for unit testing
    protected static BytesAndMessagesCount accumulateBMCount(BytesAndMessagesCount ... bmCounts) {
        BytesAndMessagesCount retBMCount = new BytesAndMessagesCount();
        for (int ix = 0; ix < bmCounts.length; ix++) {
            retBMCount.messages += bmCounts[ix].messages;
            retBMCount.bytes += bmCounts[ix].bytes;
        }
        return retBMCount;
    }

    private void checkMonitoringClass(ResourceGroupMonitoringClass monClass) throws PulsarAdminException {
        if (monClass != ResourceGroupMonitoringClass.Publish && monClass != ResourceGroupMonitoringClass.Dispatch) {
            String errMesg = "Unexpected monitoring class: " + monClass;
            throw new PulsarAdminException(errMesg);
        }
    }

    // TODO: 2/14/23 主要功能是：
    //  1。 判断是否需要上报该RG的网络信息
    //  2。 重置usedLocallySinceLastReport=0
    //  3。 统计totalUsedLocally
    //  4.  重置lastReportedValues如果需要上报
    // Fill usage about a particular monitoring class in the transport-manager callback
    // for reporting local stats to other brokers.
    // Returns true if something was filled.
    // Visibility for unit testing.
    protected boolean setUsageInMonitoredEntity(ResourceGroupMonitoringClass monClass, NetworkUsage p) {
        long bytesUsed, messagesUsed;
        boolean sendReport;
        int numSuppressions = 0;
        PerMonitoringClassFields monEntity;

        final int idx = monClass.ordinal();
        // TODO: 2/14/23 获取到给RG内存中的 PerMonitoringClassFields
        monEntity = this.monitoringClassFields[idx];

        monEntity.localUsageStatsLock.lock();
        try {
            // todo 判断是否需要上报
            sendReport = this.rgs.quotaCalculator.needToReportLocalUsage(
                    monEntity.usedLocallySinceLastReport.bytes,
                    monEntity.lastReportedValues.bytes,
                    monEntity.usedLocallySinceLastReport.messages,
                    monEntity.lastReportedValues.messages,
                    monEntity.lastResourceUsageFillTimeMSecsSinceEpoch);

            // TODO: 2/14/23 已经被使用的网络情况
            bytesUsed = monEntity.usedLocallySinceLastReport.bytes;
            messagesUsed = monEntity.usedLocallySinceLastReport.messages;
            // TODO: 2/14/23 把该broker上面的该RG网上使用的情况重置为0，因为后面一直是做增量操作，即聚合的时候，会把数据量累积起来作为该RG的网络使用情况。
            //  然后当把本地数据上报后，需要重置该值
            monEntity.usedLocallySinceLastReport.bytes = monEntity.usedLocallySinceLastReport.messages = 0;

            // TODO: 2/14/23 统计到总用量里面
            monEntity.totalUsedLocally.bytes += bytesUsed;
            monEntity.totalUsedLocally.messages += messagesUsed;

            monEntity.lastResourceUsageFillTimeMSecsSinceEpoch = System.currentTimeMillis();

            if (sendReport) {
                // TODO: 2/14/23 如果是为上报
                p.setBytesPerPeriod(bytesUsed);
                p.setMessagesPerPeriod(messagesUsed);
                // TODO: 2/14/23 更新上报信息，所以说，上报值不一定为已使用值
                monEntity.lastReportedValues.bytes = bytesUsed;
                monEntity.lastReportedValues.messages = messagesUsed;
                monEntity.numSuppressedUsageReports = 0; // todo 如果需要上报，则numSuppressedUsageReports=0
            } else {
                // todo 否则 numSuppressedUsageReports会自增1
                numSuppressions = monEntity.numSuppressedUsageReports++;
            }

        } finally {
            monEntity.localUsageStatsLock.unlock();
        }

        final String rgName = this.ruPublisher != null ? this.ruPublisher.getID() : this.resourceGroupName;
        double sentCount = sendReport ? 1 : 0;
        // todo 监控里面更新信息，上报了多少次统计采集
        rgLocalUsageReportCount.labels(rgName, monClass.name()).inc(sentCount);
        if (sendReport) {
            if (log.isDebugEnabled()) {
                log.debug("fillResourceUsage for RG={}: filled a {} update; bytes={}, messages={}",
                        rgName, monClass, bytesUsed, messagesUsed);
            }
        } else {
            if (log.isDebugEnabled()) {
                log.debug("fillResourceUsage for RG={}: report for {} suppressed "
                    + "(suppressions={} since last sent report)",
                        rgName, monClass, numSuppressions);
            }
        }

        return sendReport;
    }

    // Update fields in a particular monitoring class from a given broker in the
    // transport-manager callback for listening to usage reports.
    private void getUsageFromMonitoredEntity(ResourceGroupMonitoringClass monClass, NetworkUsage p, String broker) {
        // TODO: 2/14/23 更新broker的 publish 的网络使用情况 
        final int idx = monClass.ordinal();
        PerMonitoringClassFields monEntity;
        // TODO: 2/14/23 网络使用状态 
        PerBrokerUsageStats usageStats, oldUsageStats;
        long oldByteCount, oldMessageCount;
        long newByteCount, newMessageCount;

        // TODO: 2/14/23 这个是内存中的一个数组，里面只有publish， dispatch两个。
        //  当这里是publish的时候，对应的是 PerMonitoringClassFields的数据结构
        monEntity = this.monitoringClassFields[idx];
        // TODO: 2/14/23 根据broker，获取到broker在内存中的网络使用情况
        usageStats = monEntity.usageFromOtherBrokers.get(broker);
        // TODO: 2/14/23 第一次的时候，为null
        if (usageStats == null) {
            // TODO: 2/14/23 创建一个 PerBrokerUsageStats实例
            usageStats = new PerBrokerUsageStats();
            // TODO: 2/14/23 设置  usedValues
            usageStats.usedValues = new BytesAndMessagesCount();
        }
        monEntity.usageFromOtherBrokersLock.lock();
        try {
            // TODO: 2/14/23 把bytes , messages 更新到对应的broker上面
            newByteCount = p.getBytesPerPeriod();
            usageStats.usedValues.bytes = newByteCount;
            newMessageCount = p.getMessagesPerPeriod();
            usageStats.usedValues.messages = newMessageCount;
            usageStats.lastResourceUsageReadTimeMSecsSinceEpoch = System.currentTimeMillis();
            // TODO: 2/14/23  把broker新数据进行更新
            oldUsageStats = monEntity.usageFromOtherBrokers.put(broker, usageStats);
        } finally {
            monEntity.usageFromOtherBrokersLock.unlock();
        }
        // TODO: 2/14/23 metric信息采集
        rgRemoteUsageReportsBytes.labels(this.ruConsumer.getID(), monClass.name(), broker).inc(newByteCount);
        rgRemoteUsageReportsMessages.labels(this.ruConsumer.getID(), monClass.name(), broker).inc(newMessageCount);

        oldByteCount = oldMessageCount = -1;
        if (oldUsageStats != null) {
            oldByteCount = oldUsageStats.usedValues.bytes;
            oldMessageCount = oldUsageStats.usedValues.messages;
        }

        if (log.isDebugEnabled()) {
            log.debug("resourceUsageListener for RG={}: updated {} stats for broker={} "
                            + "with bytes={} (old ={}), messages={} (old={})",
                    this.resourceGroupName, monClass, broker,
                    newByteCount, oldByteCount,
                    newMessageCount, oldMessageCount);
        }
    }

    // TODO: 2/10/23 每一个RG都在内存中维护一个下面的数据结构，里面存放这broker的quota，流量等信息
    //  这些信息会在calculateQuotaForAllResourceGroups()方法里面被使用到
    private void setResourceGroupMonitoringClassFields() {
        // todo 每一个RG都有两个指标， publish， dispatch
        PerMonitoringClassFields monClassFields;
        for (int idx = 0; idx < ResourceGroupMonitoringClass.values().length; idx++) {
            this.monitoringClassFields[idx] = new PerMonitoringClassFields();

            // TODO: 2/14/23 初始化 monitoringClassFields，
            monClassFields = this.monitoringClassFields[idx];
            // TODO: 2/10/23 我们设置的quota值
            monClassFields.configValuesPerPeriod = new BytesAndMessagesCount();
            monClassFields.usedLocallySinceLastReport = new BytesAndMessagesCount();
            monClassFields.lastReportedValues = new BytesAndMessagesCount();
            // TODO: 2/10/23 这些信息会在calculateQuotaForAllResourceGroups()方法里面更新这个值
            monClassFields.quotaForNextPeriod = new BytesAndMessagesCount();
            monClassFields.totalUsedLocally = new BytesAndMessagesCount();
            monClassFields.usageFromOtherBrokers = new HashMap<>();

            monClassFields.usageFromOtherBrokersLock = new ReentrantLock();
            // ToDo: Change the following to a ReadWrite lock if needed.
            monClassFields.localUsageStatsLock = new ReentrantLock();
        }
    }

    private void setResourceGroupConfigParameters(org.apache.pulsar.common.policies.data.ResourceGroup rgConfig) {
        int idx;

        // todo 设置我们配置的quota值，默认值为-1，如果我们没有配置
        idx = ResourceGroupMonitoringClass.Publish.ordinal();
        this.monitoringClassFields[idx].configValuesPerPeriod.bytes = rgConfig.getPublishRateInBytes() == null
                ? -1 : rgConfig.getPublishRateInBytes();
        this.monitoringClassFields[idx].configValuesPerPeriod.messages = rgConfig.getPublishRateInMsgs() == null
                ? -1 : rgConfig.getPublishRateInMsgs();

        idx = ResourceGroupMonitoringClass.Dispatch.ordinal();
        this.monitoringClassFields[idx].configValuesPerPeriod.bytes = rgConfig.getDispatchRateInBytes() == null
                ? -1 : rgConfig.getDispatchRateInBytes();
        this.monitoringClassFields[idx].configValuesPerPeriod.messages = rgConfig.getDispatchRateInMsgs() == null
                ? -1 : rgConfig.getDispatchRateInMsgs();
    }

    private void setDefaultResourceUsageTransportHandlers() {
        // todo 会把自己的资源汇报到内部topic里面，然后再消费该topic，获取其他broker汇报的信息
        this.ruPublisher = new ResourceUsagePublisher() {
            // TODO: 2/14/23 ID 即为RG name
            @Override
            public String getID() {
                return ResourceGroup.this.getID();
            }

            // TODO: 2/14/23 收集该RG的网络情况信息
            @Override
            public void fillResourceUsage(ResourceUsage resourceUsage) {
                // todo 汇报这个RG的资源情况到内部topic里面
                ResourceGroup.this.rgFillResourceUsage(resourceUsage);
            }
        };

        this.ruConsumer = new ResourceUsageConsumer() {
            // TODO: 2/14/23 这个ID为RG名词，即 ResourceUsage里面的owner
            @Override
            public String getID() {
                return ResourceGroup.this.getID();
            }

            @Override
            public void acceptResourceUsage(String broker, ResourceUsage resourceUsage) {
                // todo 消费内部topic，获取其他broker的资源情况
                ResourceGroup.this.rgResourceUsageListener(broker, resourceUsage);
            }
        };
    }

    public final String resourceGroupName;

    // TODO: 2/14/23 该broker内存中的数组，记录所有broker上面对该RG的publish和dispatch的网络使用情况，
    //  里面包含两个PerMonitoringClassFields[publish=new PerMonitoringClassFields(), dispatch=new PerMonitoringClassFields()]
    //  即 publish = this.monitoringClassFields[0]，
    //    dispatch = this.monitoringClassFields[1]
    public PerMonitoringClassFields[] monitoringClassFields =
            new PerMonitoringClassFields[ResourceGroupMonitoringClass.values().length];

    private static final Logger log = LoggerFactory.getLogger(ResourceGroupService.class);

    // Tenants and namespaces that are directly referencing this resource-group.
    // These are not kept on a per-monitoring class basis, since a given NS (for example) references the same RG
    // across all of its usage classes (publish/dispatch/...).
    private Set<String> resourceGroupTenantRefs = ConcurrentHashMap.newKeySet();
    private Set<String> resourceGroupNamespaceRefs = ConcurrentHashMap.newKeySet();

    // Blobs required for transport manager's resource-usage register/unregister ops.
    ResourceUsageConsumer ruConsumer;
    ResourceUsagePublisher ruPublisher;

    // The creator resource-group-service [ToDo: remove later with a strict singleton ResourceGroupService]
    ResourceGroupService rgs;

    // Labels for the various counters used here.
    private static final String[] resourceGroupMontoringclassLabels = {"ResourceGroup", "MonitoringClass"};
    private static final String[] resourceGroupMontoringclassRemotebrokerLabels =
                                                    {"ResourceGroup", "MonitoringClass", "RemoteBroker"};

    private static final Counter rgRemoteUsageReportsBytes = Counter.build()
            .name("pulsar_resource_group_remote_usage_bytes_used")
            .help("Bytes used reported about this <RG, monitoring class> from a remote broker")
            .labelNames(resourceGroupMontoringclassRemotebrokerLabels)
            .register();
    private static final Counter rgRemoteUsageReportsMessages = Counter.build()
            .name("pulsar_resource_group_remote_usage_messages_used")
            .help("Messages used reported about this <RG, monitoring class> from a remote broker")
            .labelNames(resourceGroupMontoringclassRemotebrokerLabels)
            .register();

    private static final Counter rgLocalUsageReportCount = Counter.build()
            .name("pulsar_resource_group_local_usage_reported")
            .help("Number of times local usage was reported (vs. suppressed due to negligible change)")
            .labelNames(resourceGroupMontoringclassLabels)
            .register();

    // Publish rate limiter for the resource group
    @Getter
    protected ResourceGroupPublishLimiter resourceGroupPublishLimiter;

    // TODO: 2/14/23 这个数据结构非常重要，它记录着该RG的网络使用情况
    protected static class PerMonitoringClassFields {
        // This lock covers all the "local" counts (i.e., except for the per-broker usage stats).
        Lock localUsageStatsLock;

        // TODO: 2/14/23 配置的 quota值
        BytesAndMessagesCount configValuesPerPeriod;

        // TODO: 2/14/23 调整后的quota值
        // Target quotas set (after reading info from other brokers) for local usage.
        BytesAndMessagesCount quotaForNextPeriod;

        // TODO: 2/14/23 已使用的网络情况，这里一直做增量操作 因为后面一直是做增量操作，即聚合的时候，会把数据量累积起来作为该RG的网络使用情况。
        //            //  然后当把本地数据上报后，需要重置该值为0
        // Running statistics about local usage.
        BytesAndMessagesCount usedLocallySinceLastReport;

        // TODO: 2/14/23 上一次上报的网络情况
        // Statistics about local usage that were reported in the last round.
        // We will suppress reports (as optimization) if usedLocallySinceLastReport is not
        // substantially different from lastReportedValues.
        BytesAndMessagesCount lastReportedValues;

        // Time when we last attempted to fill up in fillResourceUsage; for debugging.
        long lastResourceUsageFillTimeMSecsSinceEpoch;

        // TODO: 2/14/23 计数器
        // Number of rounds of suppressed usage reports (due to not enough change) since last we reported usage.
        int numSuppressedUsageReports;

        // TODO: 2/14/23 网络总使用情况
        // Accumulated stats of local usage.
        BytesAndMessagesCount totalUsedLocally;

        // This lock covers all the non-local usage counts, received from other brokers.
        Lock usageFromOtherBrokersLock;
        // TODO: 2/14/23 其他broker的网络使用情况，<broker, PerBrokerUsageStats>
        public HashMap<String, PerBrokerUsageStats> usageFromOtherBrokers;
    }

    // Usage stats for this RG obtained from other brokers.
    protected static class PerBrokerUsageStats {
        // Time when we last read about the <RG, broker> in resourceUsageListener; for debugging.
        public long lastResourceUsageReadTimeMSecsSinceEpoch;

        // Usage stats about the <RG, broker>.
        BytesAndMessagesCount usedValues;
    }
}
