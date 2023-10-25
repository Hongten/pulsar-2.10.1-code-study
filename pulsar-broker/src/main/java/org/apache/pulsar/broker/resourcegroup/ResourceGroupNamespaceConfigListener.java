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

import java.util.function.Consumer;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.resources.NamespaceResources;
import org.apache.pulsar.broker.resources.TenantResources;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resource Group Namespace Config Listener
 *
 * <P>Meta data store listener of updates to namespace attachment to resource groups.
 * <P>Listens to namespace(policy) config changes and updates internal data structures.
 *
 * @see <a href="https://github.com/apache/pulsar/wiki/PIP-82%3A-Tenant-and-namespace-level-rate-limiting">Global-quotas</a>
 *
 */
public class ResourceGroupNamespaceConfigListener implements Consumer<Notification> {

    private static final Logger LOG = LoggerFactory.getLogger(ResourceGroupNamespaceConfigListener.class);
    private final ResourceGroupService rgService;
    private final PulsarService pulsarService;
    private final NamespaceResources namespaceResources;
    private final TenantResources tenantResources;
    private final ResourceGroupConfigListener rgConfigListener;

    public ResourceGroupNamespaceConfigListener(ResourceGroupService rgService, PulsarService pulsarService,
            ResourceGroupConfigListener rgConfigListener) {
        this.rgService = rgService;
        this.pulsarService = pulsarService;
        // TODO: 2/9/23 因为resourcegroup需要绑定到租户或者namespace上面
        this.namespaceResources = pulsarService.getPulsarResources().getNamespaceResources();
        this.tenantResources = pulsarService.getPulsarResources().getTenantResources();
        this.rgConfigListener = rgConfigListener;
        // todo 加载所有的resourcegroups， 在初始化ResourceGroupConfigListener的时候，已经加载了所有的resourcegroup实例，这里需要把rc实例和namespace进行绑定 
        loadAllNamespaceResourceGroups();
        this.namespaceResources.getStore().registerListener(this);
    }

    private void updateNamespaceResourceGroup(NamespaceName nsName) {
        // TODO: 2/9/23 从zk中获取该namespace的策略信息
        namespaceResources.getPoliciesAsync(nsName).whenCompleteAsync((optionalPolicies, ex) -> {
            if (ex != null) {
                LOG.error("Exception when getting namespace {}", nsName, ex);
                return;
            }
            Policies policy = optionalPolicies.get();
            // TODO: 2/9/23 更新namespace的rc
            reconcileNamespaceResourceGroup(nsName, policy);
        });
    }

    // TODO: 2/9/23 在初始化ResourceGroupConfigListener的时候，已经加载了所有的resourcegroup实例，这里需要把rc实例和namespace进行绑定
    private void loadAllNamespaceResourceGroups() {
        // todo 遍历所有的tenants
        tenantResources.listTenantsAsync().whenComplete((tenantList, ex) -> {
            if (ex != null) {
                LOG.error("Exception when fetching tenants", ex);
                return;
            }
            for (String ts: tenantList) {
                // todo 遍历所有的namespace
                namespaceResources.listNamespacesAsync(ts).whenComplete((nsList, ex1) -> {
                    if (ex1 != null) {
                        LOG.error("Exception when fetching namespaces", ex1);
                    } else {
                        for (String ns : nsList) {
                            NamespaceName nsn = NamespaceName.get(ts, ns);
                            // todo 找到resourcegroup,
                            namespaceResources.namespaceExistsAsync(nsn)
                                    .thenAccept(exists -> {
                                        if (exists) {
                                            // todo 更新到对应的namespace下面
                                            updateNamespaceResourceGroup(NamespaceName.get(ts, ns));
                                        }
                                    });
                        }
                    }
                });
            }
        });
    }

    public void reloadAllNamespaceResourceGroups() {
        loadAllNamespaceResourceGroups();
    }

    // TODO: 2/9/23 更新namespace上面的rc策略，从缓存中直接更新
    public void reconcileNamespaceResourceGroup(NamespaceName ns, Policies policy) {
        boolean delete = false, add = false;
        org.apache.pulsar.broker.resourcegroup.ResourceGroup current = rgService
                .getNamespaceResourceGroup(ns);

        if (policy == null || policy.resource_group_name == null) {
            // TODO: 2/9/23 本来有，现在为空，即需要删除操作
            if (current != null) {
                delete = true;
            }
        } else {
            if (current == null) {
                // TODO: 2/9/23 本来没有，现在有，需要添加
                add = true;
            }
            if (current != null && !policy.resource_group_name.equals(current.resourceGroupName)) {
                // TODO: 2/9/23 本来有，现在也有，但是名字不一样，需要删除本来的
                delete = true;
            }
        }
        try {
            if (delete) {
                LOG.info("Unregistering namespace {} from resource group {}", ns, current.resourceGroupName);
                // todo 移除操作
                rgService.unRegisterNameSpace(current.resourceGroupName, ns);
            }
            if (add) {
                LOG.info("Registering namespace {} with resource group {}", ns, policy.resource_group_name);
                // todo 添加操作
                rgService.registerNameSpace(policy.resource_group_name, ns);
            }
        } catch (PulsarAdminException e) {
            LOG.error("Failed to {} namespace {} with resource group {}",
                    delete ? "unregister" : "register", ns, policy.resource_group_name, e);
        }
    }

    @Override
    public void accept(Notification notification) {
        String notifyPath = notification.getPath();

        if (!NamespaceResources.pathIsFromNamespace(notifyPath)) {
            return;
        }
        String[] parts = notifyPath.split("/");
        if (parts.length < 4) {
            // We are only interested in the tenant and namespace level notifications
            return;
        }
        LOG.info("Metadata store notification: Path {}, Type {}", notifyPath, notification.getType());

        if (parts.length == 4 && notification.getType() == NotificationType.ChildrenChanged) {
            // Namespace add/delete
            reloadAllNamespaceResourceGroups();
        } else if (parts.length == 5) {
            switch (notification.getType()) {
                case Modified: {
                    NamespaceName nsName = NamespaceResources.namespaceFromPath(notifyPath);
                    updateNamespaceResourceGroup(nsName);
                    break;
                }
                case Deleted: {
                    NamespaceName nsName = NamespaceResources.namespaceFromPath(notifyPath);
                    ResourceGroup rg = rgService
                            .getNamespaceResourceGroup(nsName);
                    if (rg != null) {
                        try {
                            rgService.unRegisterNameSpace(rg.resourceGroupName, nsName);
                        } catch (PulsarAdminException e) {
                            LOG.error("Failed to unregister namespace", e);
                        }
                    }
                    break;
                }
            default:
                break;
            }
        }
    }
}