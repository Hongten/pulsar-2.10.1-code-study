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

import static org.apache.pulsar.common.util.Runnables.catchingAndLoggingThrowables;
import io.prometheus.client.Counter;
import io.prometheus.client.Summary;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.val;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.resourcegroup.ResourceGroup.BytesAndMessagesCount;
import org.apache.pulsar.broker.resourcegroup.ResourceGroup.ResourceGroupMonitoringClass;
import org.apache.pulsar.broker.resourcegroup.ResourceGroup.ResourceGroupRefTypes;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.policies.data.stats.TopicStatsImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The <code>ResourceGroupService</code> contains APIs to manipulate resource groups. It is assumed that there is a
 * single instance of the ResourceGroupService on each broker.
 * The control plane operations are somewhat stringent, and throw exceptions if (for instance) the op refers to a
 * resource group which does not exist.
 * The data plane operations (e.g., increment stats) throw exceptions as a last resort; if (e.g.) increment stats
 * refers to a non-existent resource group, the stats are quietly not incremented.
 *
 * @see PulsarService
 */
// TODO: 2/9/23 在broker启动的时候，会创建 ResourceGroupService 一个实例。ResourceGroupService在初始化的时候，会启动定期任务
//  aggregateLocalUsagePeriodicTask: 负责定期聚合所有已知BrokerService的流量使用情况，会周期性的执行aggregateResourceGroupLocalUsages方法，
//                                   针对生产消费两种行为。aggregateResourceGroupLocalUsages会调用updateStatsWithDiff方法，
//                                   找出最新更新的统计数据，若有差异则更新统计数据
//  calculateQuotaPeriodicTask: 负责定期计算所有ResourceGroup的更新的流量配额，周期性地执行calculateQuotaForAllResourceGroups方法。
//                              其内部的computeLocalQuota方法会根据配置值和实际值计算本地的流量配额。
public class ResourceGroupService {
    /**
     * Default constructor.
     */
    public ResourceGroupService(PulsarService pulsar) {
        this.pulsar = pulsar;
        this.timeUnitScale = TimeUnit.SECONDS;
        // todo 计算quota的实现
        this.quotaCalculator = new ResourceQuotaCalculatorImpl();
        // todo resourceUsageTransportManager 初始化。在broker启动的时候初始化的
        this.resourceUsageTransportManagerMgr = pulsar.getResourceUsageTransportManager();
        // todo 创建resourcegroup listener。加载所有的resourcegroup，并实例化到缓存。然后把namespace和resourcegroup绑定在一起
        this.rgConfigListener = new ResourceGroupConfigListener(this, pulsar);
        this.initialize();
    }

    // For testing only.
    public ResourceGroupService(PulsarService pulsar, TimeUnit timescale,
                                ResourceUsageTopicTransportManager transportMgr,
                                ResourceQuotaCalculator quotaCalc) {
        this.pulsar = pulsar;
        this.timeUnitScale = timescale;
        this.resourceUsageTransportManagerMgr = transportMgr;
        this.quotaCalculator = quotaCalc;
        this.rgConfigListener = new ResourceGroupConfigListener(this, pulsar);
        this.initialize();
    }

    protected enum ResourceGroupOpStatus {
        OK,
        Exists,
        DoesNotExist,
        NotSupported
    }

    // Types of RG-usage stats that UTs may ask for
    protected enum ResourceGroupUsageStatsType {
        Cumulative,   // Cumulative usage, since the RG was instantiated
        LocalSinceLastReported,  // Incremental usage since last report to the transport-manager
        ReportFromTransportMgr   // Last reported stats for this broker, as provided by transport-manager
    }

    /**
     * Create RG.
     *
     * @throws if RG with that name already exists.
     */
    public void resourceGroupCreate(String rgName, org.apache.pulsar.common.policies.data.ResourceGroup rgConfig)
      throws PulsarAdminException {
        this.checkRGCreateParams(rgName, rgConfig);
        // TODO: 2/10/23 创建一个resourcegroup对象
        ResourceGroup rg = new ResourceGroup(this, rgName, rgConfig);
        // todo 放入到cache 里面
        resourceGroupsMap.put(rgName, rg);
    }

    /**
     * Create RG, with non-default functions for resource-usage transport-manager.
     *
     * @throws if RG with that name already exists (even if the resource usage handlers are different).
     */
    public void resourceGroupCreate(String rgName,
                                    org.apache.pulsar.common.policies.data.ResourceGroup rgConfig,
                                    ResourceUsagePublisher rgPublisher,
                                    ResourceUsageConsumer rgConsumer) throws PulsarAdminException {
        this.checkRGCreateParams(rgName, rgConfig);
        ResourceGroup rg = new ResourceGroup(this, rgName, rgConfig, rgPublisher, rgConsumer);
        resourceGroupsMap.put(rgName, rg);
    }

    /**
     * Get a copy of the RG with the given name.
     */
    public ResourceGroup resourceGroupGet(String resourceGroupName) {
        ResourceGroup retrievedRG = this.getResourceGroupInternal(resourceGroupName);
        if (retrievedRG == null) {
            return null;
        }

        // TODO: 2/9/23 这里为啥要不直接返回呢？
        // Return a copy.
        return new ResourceGroup(retrievedRG);
    }

    /**
     * Update RG.
     *
     * @throws if RG with that name does not exist.
     */
    public void resourceGroupUpdate(String rgName, org.apache.pulsar.common.policies.data.ResourceGroup rgConfig)
      throws PulsarAdminException {
        if (rgConfig == null) {
            throw new IllegalArgumentException("ResourceGroupUpdate: Invalid null ResourceGroup config");
        }

        ResourceGroup rg = this.getResourceGroupInternal(rgName);
        if (rg == null) {
            throw new PulsarAdminException("Resource group does not exist: " + rgName);
        }
        rg.updateResourceGroup(rgConfig);
        rgUpdates.labels(rgName).inc();
    }

    public Set<String> resourceGroupGetAll() {
        return resourceGroupsMap.keySet();
    }

    /**
     * Delete RG.
     *
     * @throws if RG with that name does not exist, or if the RG exists but is still in use.
     */
    public void resourceGroupDelete(String name) throws PulsarAdminException {
        ResourceGroup rg = this.getResourceGroupInternal(name);
        if (rg == null) {
            throw new PulsarAdminException("Resource group does not exist: " + name);
        }

        long tenantRefCount = rg.getResourceGroupNumOfTenantRefs();
        long nsRefCount = rg.getResourceGroupNumOfNSRefs();
        // todo 如果被删除的RG还存在引用，则抛出异常，
        //  是否可以直接删除引用呢？
        if ((tenantRefCount + nsRefCount) > 0) {
            String errMesg = "Resource group " + name + " still has " + tenantRefCount + " tenant refs";
            errMesg += " and " + nsRefCount + " namespace refs on it";
            throw new PulsarAdminException(errMesg);
        }

        rg.resourceGroupPublishLimiter.close();
        rg.resourceGroupPublishLimiter = null;
        // todo 移除操作
        resourceGroupsMap.remove(name);
    }

    /**
     * Get the current number of RGs. For testing.
     */
    protected long getNumResourceGroups() {
        return resourceGroupsMap.mappingCount();
    }

    // TODO: 2/9/23 没有任何地方使用这个方法 
    /**
     * Registers a tenant as a user of a resource group.
     *
     * @param resourceGroupName
     * @param tenantName
     * @throws if the RG does not exist, or if the NS already references the RG.
     */
    public void registerTenant(String resourceGroupName, String tenantName) throws PulsarAdminException {
        ResourceGroup rg = checkResourceGroupExists(resourceGroupName);

        // Check that the tenant-name doesn't already have a RG association.
        // [If it does, that should be unregistered before putting a different association.]
        ResourceGroup oldRG = this.tenantToRGsMap.get(tenantName);
        if (oldRG != null) {
            String errMesg = "Tenant " + tenantName + " already references a resource group: " + oldRG.getID();
            throw new PulsarAdminException(errMesg);
        }

        ResourceGroupOpStatus status = rg.registerUsage(tenantName, ResourceGroupRefTypes.Tenants, true,
                                                        this.resourceUsageTransportManagerMgr);
        if (status == ResourceGroupOpStatus.Exists) {
            String errMesg = "Tenant " + tenantName + " already references the resource group " + resourceGroupName;
            errMesg += "; this is unexpected";
            throw new PulsarAdminException(errMesg);
        }

        // Associate this tenant name with the RG.
        this.tenantToRGsMap.put(tenantName, rg);
        rgTenantRegisters.labels(resourceGroupName).inc();
    }

    /**
     * UnRegisters a tenant from a resource group.
     *
     * @param resourceGroupName
     * @param tenantName
     * @throws if the RG does not exist, or if the tenant does not references the RG yet.
     */
    public void unRegisterTenant(String resourceGroupName, String tenantName) throws PulsarAdminException {
        ResourceGroup rg = checkResourceGroupExists(resourceGroupName);

        ResourceGroupOpStatus status = rg.registerUsage(tenantName, ResourceGroupRefTypes.Tenants, false,
                                                        this.resourceUsageTransportManagerMgr);
        if (status == ResourceGroupOpStatus.DoesNotExist) {
            String errMesg = "Tenant " + tenantName + " does not yet reference resource group " + resourceGroupName;
            throw new PulsarAdminException(errMesg);
        }

        // Dissociate this tenant name from the RG.
        this.tenantToRGsMap.remove(tenantName, rg);
        rgTenantUnRegisters.labels(resourceGroupName).inc();
    }

    /**
     * Registers a namespace as a user of a resource group.
     *
     * @param resourceGroupName
     * @param fqNamespaceName (i.e., in "tenant/Namespace" format)
     * @throws if the RG does not exist, or if the NS already references the RG.
     */
    public void registerNameSpace(String resourceGroupName, NamespaceName fqNamespaceName) throws PulsarAdminException {
        ResourceGroup rg = checkResourceGroupExists(resourceGroupName);

        // Check that the NS-name doesn't already have a RG association.
        // [If it does, that should be unregistered before putting a different association.]
        ResourceGroup oldRG = this.namespaceToRGsMap.get(fqNamespaceName);
        if (oldRG != null) {
            String errMesg = "Namespace " + fqNamespaceName + " already references a resource group: " + oldRG.getID();
            throw new PulsarAdminException(errMesg);
        }

        ResourceGroupOpStatus status = rg.registerUsage(fqNamespaceName.toString(), ResourceGroupRefTypes.Namespaces,
                true, this.resourceUsageTransportManagerMgr);
        if (status == ResourceGroupOpStatus.Exists) {
            String errMesg = String.format("Namespace %s already references the target resource group %s",
                    fqNamespaceName, resourceGroupName);
            throw new PulsarAdminException(errMesg);
        }

        // todo 注册成功后，把namespace，resourcegroup放入到缓存
        // Associate this NS-name with the RG.
        this.namespaceToRGsMap.put(fqNamespaceName, rg);
        rgNamespaceRegisters.labels(resourceGroupName).inc();
    }

    /**
     * UnRegisters a namespace from a resource group.
     *
     * @param resourceGroupName
     * @param fqNamespaceName i.e., in "tenant/Namespace" format)
     * @throws if the RG does not exist, or if the NS does not references the RG yet.
     */
    public void unRegisterNameSpace(String resourceGroupName, NamespaceName fqNamespaceName)
            throws PulsarAdminException {
        ResourceGroup rg = checkResourceGroupExists(resourceGroupName);

        // todo 移除和添加操作刚刚相反
        ResourceGroupOpStatus status = rg.registerUsage(fqNamespaceName.toString(), ResourceGroupRefTypes.Namespaces,
                false, this.resourceUsageTransportManagerMgr);
        if (status == ResourceGroupOpStatus.DoesNotExist) {
            String errMesg = String.format("Namespace %s does not yet reference resource group %s",
                    fqNamespaceName, resourceGroupName);
            throw new PulsarAdminException(errMesg);
        }

        // Dissociate this NS-name from the RG.
        this.namespaceToRGsMap.remove(fqNamespaceName, rg);
        rgNamespaceUnRegisters.labels(resourceGroupName).inc();
    }

    /**
     * Return the resource group associated with a namespace.
     *
     * @param namespaceName
     * @throws if the RG does not exist, or if the NS already references the RG.
     */
    public ResourceGroup getNamespaceResourceGroup(NamespaceName namespaceName) {
        return this.namespaceToRGsMap.get(namespaceName);
    }

    /**
     * Increments usage stats for the resource groups associated with the given namespace and tenant.
     * Expected to be called when a message is produced or consumed on a topic, or when we calculate
     * usage periodically in the background by going through broker-service stats. [Not yet decided
     * which model we will follow.] Broker-service stats will be cumulative, while calls from the
     * topic produce/consume code will be per-produce/consume.
     *
     * If the tenant and NS are associated with different RGs, the statistics on both RGs are updated.
     * If the tenant and NS are associated with the same RG, the stats on the RG are updated only once
     * (to avoid a direct double-counting).
     * ToDo: will this distinction result in "expected semantics", or shock from users?
     * For now, the only caller is internal to this class.
     *
     * @param tenantName
     * @param nsName
     * @param monClass
     * @param incStats
     * @returns true if the stats were updated; false if nothing was updated.
     */
    protected boolean incrementUsage(String tenantName, String nsName,
                                  ResourceGroupMonitoringClass monClass,
                                  BytesAndMessagesCount incStats) throws PulsarAdminException {
        final ResourceGroup nsRG = this.namespaceToRGsMap.get(NamespaceName.get(tenantName, nsName));
        // TODO: 2/9/23 没有使用这个resourcegroup，因此为null
        final ResourceGroup tenantRG = this.tenantToRGsMap.get(tenantName);
        if (tenantRG == null && nsRG == null) {
            return false;
        }

        // Expect stats to increase monotonically.
        if (incStats.bytes < 0 || incStats.messages < 0) {
            String errMesg = String.format("incrementUsage on tenant=%s, NS=%s: bytes (%s) or mesgs (%s) is negative",
                    tenantName, nsName, incStats.bytes, incStats.messages);
            throw new PulsarAdminException(errMesg);
        }

        if (nsRG == tenantRG) {
            // Update only once in this case.
            // Note that we will update both tenant and namespace RGs in other cases.
            nsRG.incrementLocalUsageStats(monClass, incStats);
            rgLocalUsageMessages.labels(nsRG.resourceGroupName, monClass.name()).inc(incStats.messages);
            rgLocalUsageBytes.labels(nsRG.resourceGroupName, monClass.name()).inc(incStats.bytes);
            return true;
        }

        if (tenantRG != null) {
            tenantRG.incrementLocalUsageStats(monClass, incStats);
            rgLocalUsageMessages.labels(tenantRG.resourceGroupName, monClass.name()).inc(incStats.messages);
            rgLocalUsageBytes.labels(tenantRG.resourceGroupName, monClass.name()).inc(incStats.bytes);
        }
        if (nsRG != null) {
            // TODO: 2/14/23 对namespace 的RG进行更新该broker的网络使用情况
            nsRG.incrementLocalUsageStats(monClass, incStats);
            // TODO: 2/14/23 metrics采集
            rgLocalUsageMessages.labels(nsRG.resourceGroupName, monClass.name()).inc(incStats.messages);
            rgLocalUsageBytes.labels(nsRG.resourceGroupName, monClass.name()).inc(incStats.bytes);
        }

        return true;
    }

    // Visibility for testing.
    protected BytesAndMessagesCount getRGUsage(String rgName, ResourceGroupMonitoringClass monClass,
                                               ResourceGroupUsageStatsType statsType) throws PulsarAdminException {
        final ResourceGroup rg = this.getResourceGroupInternal(rgName);
        if (rg != null) {
            switch (statsType) {
                default:
                    String errStr = "Unsupported statsType: " + statsType;
                    throw new PulsarAdminException(errStr);
                case Cumulative:
                    return rg.getLocalUsageStatsCumulative(monClass);
                case LocalSinceLastReported:
                    return rg.getLocalUsageStats(monClass);
                case ReportFromTransportMgr:
                    return rg.getLocalUsageStatsFromBrokerReports(monClass);
            }
        }

        BytesAndMessagesCount retCount = new BytesAndMessagesCount();
        retCount.bytes = -1;
        retCount.messages = -1;
        return retCount;
    }

    /**
     * Get the RG with the given name. For internal operations only.
     */
    private ResourceGroup getResourceGroupInternal(String resourceGroupName) {
        if (resourceGroupName == null) {
            throw new IllegalArgumentException("Invalid null resource group name: " + resourceGroupName);
        }

        return resourceGroupsMap.get(resourceGroupName);
    }

    private ResourceGroup checkResourceGroupExists(String rgName) throws PulsarAdminException {
        ResourceGroup rg = this.getResourceGroupInternal(rgName);
        if (rg == null) {
            throw new PulsarAdminException("Resource group does not exist: " + rgName);
        }
        return rg;
    }

    // TODO: 2/9/23 找出最新更新的统计数据，若有差异则更新统计信息
    // Find the difference between the last time stats were updated for this topic, and the current
    // time. If the difference is positive, update the stats.
    private void updateStatsWithDiff(String topicName, String tenantString, String nsString,
                                     long accByteCount, long accMesgCount, ResourceGroupMonitoringClass monClass) {
        ConcurrentHashMap<String, BytesAndMessagesCount> hm;
        switch (monClass) {
            default:
                log.error("updateStatsWithDiff: Unknown monitoring class={}; ignoring", monClass);
                return;

            case Publish:
                hm = this.topicProduceStats;
                break;

            case Dispatch:
                hm = this.topicConsumeStats;
                break;
        }

        BytesAndMessagesCount bmDiff = new BytesAndMessagesCount();
        BytesAndMessagesCount bmOldCount;
        BytesAndMessagesCount bmNewCount = new BytesAndMessagesCount();

        bmNewCount.bytes = accByteCount;
        bmNewCount.messages = accMesgCount;

        bmOldCount = hm.get(topicName);
        // TODO: 2/9/23 第一次，由于hm里面为空，差值即为新值。如果已经存在数据，差值即为旧数据与新数据之差
        if (bmOldCount == null) {
            bmDiff.bytes = bmNewCount.bytes;
            bmDiff.messages = bmNewCount.messages;
        } else {
            bmDiff.bytes = bmNewCount.bytes - bmOldCount.bytes;
            bmDiff.messages = bmNewCount.messages - bmOldCount.messages;
        }

        // todo 如果是大于0的差值，才会更新。也意味着只要新数据一直小于旧数据，一直不会更新
        // TODO: 2/9/23 这里可以优化为最大值查找，第一次：max(0, bmNewCount). 以后：max(bmNewCount, bmOldCount),然后和bmOldCount进行比较即可
        if (bmDiff.bytes <= 0 || bmDiff.messages <= 0) {
            return;
        }

        try {
            // TODO: 2/14/23 这里为增量操作
            boolean statsUpdated = this.incrementUsage(tenantString, nsString, monClass, bmDiff);
            if (log.isDebugEnabled()) {
                log.debug("updateStatsWithDiff for topic={}: monclass={} statsUpdated={} for tenant={}, namespace={}; "
                                + "by {} bytes, {} mesgs",
                        topicName, monClass, statsUpdated, tenantString, nsString,
                        bmDiff.bytes, bmDiff.messages);
            }
            // TODO: 2/9/23 如果是第一次，会把数据添加进入hm。如果是后面，则满足条件后进行更新
            hm.put(topicName, bmNewCount);
        } catch (Throwable t) {
            log.error("updateStatsWithDiff: got ex={} while aggregating for {} side",
                    t.getMessage(), monClass);
        }
    }

    // Visibility for testing.
    protected BytesAndMessagesCount getPublishRateLimiters (String rgName) throws PulsarAdminException {
        ResourceGroup rg = this.getResourceGroupInternal(rgName);
        if (rg == null) {
            throw new PulsarAdminException("Resource group does not exist: " + rgName);
        }

        return rg.getRgPublishRateLimiterValues();
    }

    // Visibility for testing.
    protected static double getRgQuotaByteCount (String rgName, String monClassName) {
        return rgCalculatedQuotaBytes.labels(rgName, monClassName).get();
    }

    // Visibility for testing.
    protected static double getRgQuotaMessageCount (String rgName, String monClassName) {
        return rgCalculatedQuotaMessages.labels(rgName, monClassName).get();
    }

    // Visibility for testing.
    protected static double getRgLocalUsageByteCount (String rgName, String monClassName) {
        return rgLocalUsageBytes.labels(rgName, monClassName).get();
    }

    // Visibility for testing.
    protected static double getRgLocalUsageMessageCount (String rgName, String monClassName) {
        return rgLocalUsageMessages.labels(rgName, monClassName).get();
    }

    // Visibility for testing.
    protected static double getRgUpdatesCount (String rgName) {
        return rgUpdates.labels(rgName).get();
    }

    // Visibility for testing.
    protected static double getRgTenantRegistersCount (String rgName) {
        return rgTenantRegisters.labels(rgName).get();
    }

    // Visibility for testing.
    protected static double getRgTenantUnRegistersCount (String rgName) {
        return rgTenantUnRegisters.labels(rgName).get();
    }

    // Visibility for testing.
    protected static double getRgNamespaceRegistersCount (String rgName) {
        return rgNamespaceRegisters.labels(rgName).get();
    }

    // Visibility for testing.
    protected static double getRgNamespaceUnRegistersCount (String rgName) {
        return rgNamespaceUnRegisters.labels(rgName).get();
    }

    // Visibility for testing.
    protected static Summary.Child.Value getRgUsageAggregationLatency() {
        return rgUsageAggregationLatency.get();
    }

    // Visibility for testing.
    protected static Summary.Child.Value getRgQuotaCalculationTime() {
        return rgQuotaCalculationLatency.get();
    }

    // TODO: 2/9/23 aggregateLocalUsagePeriodicTask: 负责定期聚合所有已知BrokerService的流量使用情况，会周期性的执行aggregateResourceGroupLocalUsages方法，
    //        //                                   针对生产消费两种行为。aggregateResourceGroupLocalUsages会调用updateStatsWithDiff方法，
    //        //                                   找出最新更新的统计数据，若有差异则更新统计数据
    // Periodically aggregate the usage from all topics known to the BrokerService.
    // Visibility for unit testing.
    protected void aggregateResourceGroupLocalUsages() { // todo 聚合操作
        // todo 获取起始时间
        final Summary.Timer aggrUsageTimer = rgUsageAggregationLatency.startTimer();
        BrokerService bs = this.pulsar.getBrokerService();
        // todo <topicName, TopicStatsImpl> TopicStatsImpl包含topic所有的状态信息
        Map<String, TopicStatsImpl> topicStatsMap = bs.getTopicStats();

        for (Map.Entry<String, TopicStatsImpl> entry : topicStatsMap.entrySet()) {
            // TODO: 2/9/23 topic名称
            final String topicName = entry.getKey();
            // TODO: 2/9/23 topic的状态信息
            final TopicStats topicStats = entry.getValue();
            final TopicName topic = TopicName.get(topicName); // todo 获取topicName对象
            final String tenantString = topic.getTenant(); // todo 获取租户
            final String nsString = topic.getNamespacePortion();
            // final String nsString1 = topic.getNamespace(); // todo 为什么不适用这个呢？
            final NamespaceName fqNamespace = topic.getNamespaceObject(); // todo 获取命名空间对象

            // Can't use containsKey here, as that checks for exact equality
            // (we need a check for string-comparison).
            val tenantRG = this.tenantToRGsMap.get(tenantString); // todo 目前在pulsar-admin 里面没有租户关于设置RG的选项
            // TODO: 2/9/23 目前我们配置的resourcegroup都是在namespace上面
            val namespaceRG = this.namespaceToRGsMap.get(fqNamespace);
            if (tenantRG == null && namespaceRG == null) {
                // This topic's NS/tenant are not registered to any RG.
                continue;
            }

            // TODO: 2/9/23 生产行为统计
            this.updateStatsWithDiff(topicName, tenantString, nsString,
                    topicStats.getBytesInCounter(), topicStats.getMsgInCounter(),
                    ResourceGroupMonitoringClass.Publish);
            // TODO: 2/9/23 消费行为统计
            this.updateStatsWithDiff(topicName, tenantString, nsString,
                    topicStats.getBytesOutCounter(), topicStats.getMsgOutCounter(),
                    ResourceGroupMonitoringClass.Dispatch);
        }
        double diffTimeSeconds = aggrUsageTimer.observeDuration();
        if (log.isDebugEnabled()) {
            log.debug("aggregateResourceGroupLocalUsages took {} milliseconds", diffTimeSeconds * 1000);
        }

        // TODO: 2/9/23 由于我们可以动态配置resourceUsageTransportPublishIntervalInSecs参数，所以，我们需要查看是否需要根据新值来处理数据
        // Check any re-scheduling requirements for next time.
        // Use the same period as getResourceUsagePublishIntervalInSecs;
        // cancel and re-schedule this task if the period of execution has changed.
        ServiceConfiguration config = pulsar.getConfiguration();
        long newPeriodInSeconds = config.getResourceUsageTransportPublishIntervalInSecs();
        // TODO: 2/9/23 只要前后值不一样，就需要使用新值进行处理 
        if (newPeriodInSeconds != this.aggregateLocalUsagePeriodInSeconds) {
            if (this.aggreagteLocalUsagePeriodicTask == null) {
                log.error("aggregateResourceGroupLocalUsages: Unable to find running task to cancel when "
                                + "publish period changed from {} to {} {}",
                        this.aggregateLocalUsagePeriodInSeconds, newPeriodInSeconds, timeUnitScale);
            } else {
                // TODO: 2/9/23 需要把之前的task cancel
                boolean cancelStatus = this.aggreagteLocalUsagePeriodicTask.cancel(true);
                log.info("aggregateResourceGroupLocalUsages: Got status={} in cancel of periodic "
                                + "when publish period changed from {} to {} {}",
                        cancelStatus, this.aggregateLocalUsagePeriodInSeconds, newPeriodInSeconds, timeUnitScale);
            }
            this.aggreagteLocalUsagePeriodicTask = pulsar.getExecutor().scheduleAtFixedRate(
                    catchingAndLoggingThrowables(this::aggregateResourceGroupLocalUsages),
                    newPeriodInSeconds,
                    newPeriodInSeconds,
                    timeUnitScale);
            this.aggregateLocalUsagePeriodInSeconds = newPeriodInSeconds;
        }
    }


    // todo 计算所有RG的 quota
    //  calculateQuotaPeriodicTask: 负责定期计算所有ResourceGroup的更新的流量配额，周期性地执行calculateQuotaForAllResourceGroups方法。
    //                              其内部的computeLocalQuota方法会根据配置值和实际值计算本地的流量配额。
    //                              注意：这里没有对Dispatch进行流量更新
    // Periodically calculate the updated quota for all RGs in the background,
    // from the reports received from other brokers.
    // [Visibility for unit testing.]
    protected void calculateQuotaForAllResourceGroups() {
        // todo 获取到起始时间
        // Calculate the quota for the next window for this RG, based on the observed usage.
        final Summary.Timer quotaCalcTimer = rgQuotaCalculationLatency.startTimer();
        BytesAndMessagesCount updatedQuota = new BytesAndMessagesCount();
        // todo 遍历所有的RG
        this.resourceGroupsMap.forEach((rgName, resourceGroup) -> {
            BytesAndMessagesCount globalUsageStats;
            BytesAndMessagesCount localUsageStats;
            BytesAndMessagesCount confCounts;
            // todo 目前只有生产和消费 publish， dispatch，需要注意的是，Dispatch是不进行更新操作的！
            for (ResourceGroupMonitoringClass monClass : ResourceGroupMonitoringClass.values()) {
                try {
                    // TODO: 2/10/23 获取所有broker的流量信息
                    globalUsageStats = resourceGroup.getGlobalUsageStats(monClass);
                    // TODO: 2/10/23 获取该broker本身的流量信息
                    localUsageStats = resourceGroup.getLocalUsageStatsFromBrokerReports(monClass);
                    // TODO: 2/10/23 配置的quota值
                    confCounts = resourceGroup.getConfLimits(monClass);

                    long[] globUsageBytesArray = new long[] { globalUsageStats.bytes };
                    // TODO: 2/10/23 计算出调整后的broker的bytes值
                    updatedQuota.bytes = this.quotaCalculator.computeLocalQuota(
                            confCounts.bytes,
                            localUsageStats.bytes,
                            globUsageBytesArray);

                    long[] globUsageMessagesArray = new long[] {globalUsageStats.messages };
                    // TODO: 2/10/23 计算出调整后的broker的messages值
                    updatedQuota.messages = this.quotaCalculator.computeLocalQuota(
                            confCounts.messages,
                            localUsageStats.messages,
                            globUsageMessagesArray);

                    // todo 更新quota值，主要作用是为了和最新的quota值进行比较，查看增加的msg，bytes， updatedQuota=最新的quota值
                    // TODO: 2/10/23 这里不会对Dispatch进行更新操作
                    BytesAndMessagesCount oldBMCount = resourceGroup.updateLocalQuota(monClass, updatedQuota);
                    // Guard against unconfigured quota settings, for which computeLocalQuota will return negative.
                    if (updatedQuota.messages >= 0) {
                        // TODO: 2/14/23 当前RG调整的后Quota值
                        rgCalculatedQuotaMessages.labels(rgName, monClass.name()).inc(updatedQuota.messages);
                    }
                    if (updatedQuota.bytes >= 0) {
                        rgCalculatedQuotaBytes.labels(rgName, monClass.name()).inc(updatedQuota.bytes);
                    }
                    if (oldBMCount != null) {
                        // todo 增加的msg，bytes
                        long messagesIncrement = updatedQuota.messages - oldBMCount.messages;
                        long bytesIncrement = updatedQuota.bytes - oldBMCount.bytes;
                        if (log.isDebugEnabled()) {
                            log.debug("calculateQuota for RG={} [class {}]: "
                                            + "updatedlocalBytes={}, updatedlocalMesgs={}; "
                                            + "old bytes={}, old mesgs={};  incremented bytes by {}, messages by {}",
                                    rgName, monClass, updatedQuota.bytes, updatedQuota.messages,
                                    oldBMCount.bytes, oldBMCount.messages,
                                    bytesIncrement, messagesIncrement);
                        }
                    } else {
                        if (log.isDebugEnabled()) {
                            log.debug("calculateQuota for RG={} [class {}]: got back null from updateLocalQuota",
                                    rgName, monClass);
                        }
                    }
                } catch (Throwable t) {
                    log.error("Got exception={} while calculating new quota for monitoring-class={} of RG={}",
                            t.getMessage(), monClass, rgName);
                }
            }
        });
        double diffTimeSeconds = quotaCalcTimer.observeDuration();
        if (log.isDebugEnabled()) {
            log.debug("calculateQuotaForAllResourceGroups took {} milliseconds", diffTimeSeconds * 1000);
        }

        // Check any re-scheduling requirements for next time.
        // Use the same period as getResourceUsagePublishIntervalInSecs;
        // cancel and re-schedule this task if the period of execution has changed.
        ServiceConfiguration config = pulsar.getConfiguration();
        // todo 默认为60s
        long newPeriodInSeconds = config.getResourceUsageTransportPublishIntervalInSecs();
        // todo 初始化的时候， resourceUsagePublishPeriodInSeconds=60s
        if (newPeriodInSeconds != this.resourceUsagePublishPeriodInSeconds) {
            if (this.calculateQuotaPeriodicTask == null) {
                log.error("calculateQuotaForAllResourceGroups: Unable to find running task to cancel when "
                                + "publish period changed from {} to {} {}",
                        this.resourceUsagePublishPeriodInSeconds, newPeriodInSeconds, timeUnitScale);
            } else {
                boolean cancelStatus = this.calculateQuotaPeriodicTask.cancel(true);
                log.info("calculateQuotaForAllResourceGroups: Got status={} in cancel of periodic "
                        + " when publish period changed from {} to {} {}",
                        cancelStatus, this.resourceUsagePublishPeriodInSeconds, newPeriodInSeconds, timeUnitScale);
            }
            // todo 如果刚开始时，都设置了newPeriodInSeconds=resourceUsagePublishPeriodInSeconds=60s，
            //  现在通过动态配置重新设置了newPeriodInSeconds，那么此时，需要更新一下task的执行时间
            this.calculateQuotaPeriodicTask = pulsar.getExecutor().scheduleAtFixedRate(
                        catchingAndLoggingThrowables(this::calculateQuotaForAllResourceGroups),
                        newPeriodInSeconds,
                        newPeriodInSeconds,
                        timeUnitScale);
            // todo 避免每次都更新，所以，更新完task执行时间后，需要更新resourceUsagePublishPeriodInSeconds，这样第二次就不会再更新了
            this.resourceUsagePublishPeriodInSeconds = newPeriodInSeconds;
            maxIntervalForSuppressingReportsMSecs =
                    this.resourceUsagePublishPeriodInSeconds * MaxUsageReportSuppressRounds;
        }
    }

    private void initialize() {
        ServiceConfiguration config = this.pulsar.getConfiguration();
        // todo 默认为60s
        long periodInSecs = config.getResourceUsageTransportPublishIntervalInSecs();
        // todo aggregateLocalUsagePeriodInSeconds = 60s, resourceUsagePublishPeriodInSeconds=60s
        this.aggregateLocalUsagePeriodInSeconds = this.resourceUsagePublishPeriodInSeconds = periodInSecs;
        // todo 定时任务
        //  aggregateLocalUsagePeriodicTask: 负责定期聚合所有已知BrokerService的流量使用情况，会周期性的执行aggregateResourceGroupLocalUsages方法，
        //                                   针对生产消费两种行为。aggregateResourceGroupLocalUsages会调用updateStatsWithDiff方法，
        //                                   找出最新更新的统计数据，若有差异则更新统计数据
        this.aggreagteLocalUsagePeriodicTask = this.pulsar.getExecutor().scheduleAtFixedRate(
                    catchingAndLoggingThrowables(this::aggregateResourceGroupLocalUsages),
                    periodInSecs,
                    periodInSecs,
                    this.timeUnitScale);
        // todo 定时任务
        //  calculateQuotaPeriodicTask: 负责定期计算所有ResourceGroup的更新的流量配额，周期性地执行calculateQuotaForAllResourceGroups方法。
        //                              其内部的computeLocalQuota方法会根据配置值和实际值计算本地的流量配额。
        this.calculateQuotaPeriodicTask = this.pulsar.getExecutor().scheduleAtFixedRate(
                    catchingAndLoggingThrowables(this::calculateQuotaForAllResourceGroups),
                    periodInSecs,
                    periodInSecs,
                    this.timeUnitScale);
        // todo 默认为300s
        maxIntervalForSuppressingReportsMSecs =
                this.resourceUsagePublishPeriodInSeconds * MaxUsageReportSuppressRounds;

    }

    private void checkRGCreateParams(String rgName, org.apache.pulsar.common.policies.data.ResourceGroup rgConfig)
      throws PulsarAdminException {
        if (rgConfig == null) {
            throw new IllegalArgumentException("ResourceGroupCreate: Invalid null ResourceGroup config");
        }

        if (rgName.isEmpty()) {
            throw new IllegalArgumentException("ResourceGroupCreate: can't create resource group with an empty name");
        }

        ResourceGroup rg = getResourceGroupInternal(rgName);
        if (rg != null) {
            throw new PulsarAdminException("Resource group already exists:" + rgName);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(ResourceGroupService.class);

    // todo pulsarService
    @Getter
    private final PulsarService pulsar;

    // todo pulsar resource quota计算器
    protected final ResourceQuotaCalculator quotaCalculator;
    private ResourceUsageTransportManager resourceUsageTransportManagerMgr;

    // rgConfigListener is used only through its side effects in the ctors, to set up RG/NS loading in config-listeners.
    private final ResourceGroupConfigListener rgConfigListener;

    // TODO: 2/9/23 缓存中的resourcegroup map <RG-name, ResourceGroup>
    // Given a RG-name, get the resource-group
    private ConcurrentHashMap<String, ResourceGroup> resourceGroupsMap = new ConcurrentHashMap<>();

    // Given a tenant-name, record its associated resource-group
    private ConcurrentHashMap<String, ResourceGroup> tenantToRGsMap = new ConcurrentHashMap<>();

    // todo 这个是 namespace和resourcegroup 的map，因为一个namespace只对应一个rc
    // Given a qualified NS-name (i.e., in "tenant/namespace" format), record its associated resource-group
    private ConcurrentHashMap<NamespaceName, ResourceGroup> namespaceToRGsMap = new ConcurrentHashMap<>();

    // TODO: 2/9/23 在缓存中保留一份所有topic的流量数据
    // Maps to maintain the usage per topic, in produce/consume directions.
    private ConcurrentHashMap<String, BytesAndMessagesCount> topicProduceStats = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, BytesAndMessagesCount> topicConsumeStats = new ConcurrentHashMap<>();


    // The task that periodically re-calculates the quota budget for local usage.
    private ScheduledFuture<?> aggreagteLocalUsagePeriodicTask;
    private long aggregateLocalUsagePeriodInSeconds;

    // The task that periodically re-calculates the quota budget for local usage.
    private ScheduledFuture<?> calculateQuotaPeriodicTask;
    private long resourceUsagePublishPeriodInSeconds;

    // Allow a pluggable scale on time units; for testing periodic functionality.
    private TimeUnit timeUnitScale;

    // The maximum number of successive rounds that we can suppress reporting local usage, because there was no
    // substantial change from the prior round. This is to ensure the reporting does not become too chatty.
    // Set this value to one more than the cadence of sending reports; e.g., if you want to send every 3rd report,
    // set the value to 4.
    // Setting this to 0 will make us report in every round.
    // Don't set to negative values; behavior will be "undefined".
    protected static final int MaxUsageReportSuppressRounds = 5;

    // Convenient shorthand, for MaxUsageReportSuppressRounds converted to a time interval in milliseconds.
    protected static long maxIntervalForSuppressingReportsMSecs;

    // The percentage difference that is considered "within limits" to suppress usage reporting.
    // Setting this to 0 will also make us report in every round.
    // Don't set it to negative values; behavior will be "undefined".
    protected static final float UsageReportSuppressionTolerancePercentage = 5;

    // Labels for the various counters used here.
    private static final String[] resourceGroupLabel = {"ResourceGroup"};
    private static final String[] resourceGroupMonitoringclassLabels = {"ResourceGroup", "MonitoringClass"};

    private static final Counter rgCalculatedQuotaBytes = Counter.build()
            .name("pulsar_resource_group_calculated_bytes_quota")
            .help("Bytes quota calculated for resource group")
            .labelNames(resourceGroupMonitoringclassLabels)
            .register();
    private static final Counter rgCalculatedQuotaMessages = Counter.build()
            .name("pulsar_resource_group_calculated_messages_quota")
            .help("Messages quota calculated for resource group")
            .labelNames(resourceGroupMonitoringclassLabels)
            .register();

    private static final Counter rgLocalUsageBytes = Counter.build()
            .name("pulsar_resource_group_bytes_used")
            .help("Bytes locally used within this resource group during the last aggregation interval")
            .labelNames(resourceGroupMonitoringclassLabels)
            .register();
    private static final Counter rgLocalUsageMessages = Counter.build()
            .name("pulsar_resource_group_messages_used")
            .help("Messages locally used within this resource group during the last aggregation interval")
            .labelNames(resourceGroupMonitoringclassLabels)
            .register();

    private static final Counter rgUpdates = Counter.build()
            .name("pulsar_resource_group_updates")
            .help("Number of update operations on the given resource group")
            .labelNames(resourceGroupLabel)
            .register();

    private static final Counter rgTenantRegisters = Counter.build()
            .name("pulsar_resource_group_tenant_registers")
            .help("Number of registrations of tenants")
            .labelNames(resourceGroupLabel)
            .register();
    private static final Counter rgTenantUnRegisters = Counter.build()
            .name("pulsar_resource_group_tenant_unregisters")
            .help("Number of un-registrations of tenants")
            .labelNames(resourceGroupLabel)
            .register();

    private static final Counter rgNamespaceRegisters = Counter.build()
            .name("pulsar_resource_group_namespace_registers")
            .help("Number of registrations of namespaces")
            .labelNames(resourceGroupLabel)
            .register();
    private static final Counter rgNamespaceUnRegisters = Counter.build()
            .name("pulsar_resource_group_namespace_unregisters")
            .help("Number of un-registrations of namespaces")
            .labelNames(resourceGroupLabel)
            .register();

    private static final Summary rgUsageAggregationLatency = Summary.build()
            .quantile(0.5, 0.05)
            .quantile(0.9, 0.01)
            .name("pulsar_resource_group_aggregate_usage_secs")
            .help("Time required to aggregate usage of all resource groups, in seconds.")
            .register();

    private static final Summary rgQuotaCalculationLatency = Summary.build()
            .quantile(0.5, 0.05)
            .quantile(0.9, 0.01)
            .name("pulsar_resource_group_calculate_quota_secs")
            .help("Time required to calculate quota of all resource groups, in seconds.")
            .register();
}
