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

import static org.apache.pulsar.client.api.CompressionType.LZ4;
import static org.apache.pulsar.common.util.Runnables.catchingAndLoggingThrowables;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.resource.usage.ResourceUsageInfo;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderListener;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resource Usage Transport Manager
 *
 * <P>Module to exchange usage information with other brokers. Implements a task to periodically.
 * <P>publish the usage as well as handlers to process the usage info from other brokers.
 *
 * @see <a href="https://github.com/apache/pulsar/wiki/PIP-82%3A-Tenant-and-namespace-level-rate-limiting">Global-quotas</a>
 *
 */
// TODO: 2/9/23 这个类实现了 ResourceUsageTransportManager接口，并且重写了该接口里面的所有方法。
//  为了实现流量限制，首先就要知道各个Broker在此范围内的流量配额使用情况，
//  内部实现机理就是建立一个non-persistent模式的topic，non-persistent://pulsar/system/resource-usage,
//  Broker将流量配额使用相关数据传到这个topic内以便相互通信。
//  ResourceUsageWriteTask：负责把broker的流量配额数据写入到该topic里面。
//  ResourceUsageReader：从该topic里面读取所有broker的流量信息，并将存储在一个Hasp Map中，之后Broker内部就会进行相应的处理。根据Resourcegroup的流量配额使用情况，调整自己的现在流量
public class ResourceUsageTopicTransportManager implements ResourceUsageTransportManager {

    // TODO: 2/9/23 负责把broker的流量配额数据写入到该topic里面。
    private class ResourceUsageWriterTask implements Runnable, AutoCloseable {
        // todo 资源使用生产者
        private final Producer<ByteBuffer> producer;
        private final ScheduledFuture<?> resourceUsagePublishTask;

        private Producer<ByteBuffer> createProducer() throws PulsarClientException {
            // TODO: 2/9/23 这两个变量为什么在这里定义呢？为何不直接在下面填写对一个的值？ 
            final int publishDelayMilliSecs = 10;
            final int sendTimeoutSecs = 10;

            // TODO: 2/9/23 使用pulsarClient创建一个producer 
            return pulsarClient.newProducer(Schema.BYTEBUFFER)
                    .topic(RESOURCE_USAGE_TOPIC_NAME) // todo 内部构建一个topic non-persistent://pulsar/system/resource-usage
                    .batchingMaxPublishDelay(publishDelayMilliSecs, TimeUnit.MILLISECONDS)
                    .sendTimeout(sendTimeoutSecs, TimeUnit.SECONDS)
                    .blockIfQueueFull(false)
                    .compressionType(LZ4) // todo 压缩方式为LZ4
                    .create();
        }

        // TODO: 2/9/23 把broker的流量信息写入到内部topic
        public ResourceUsageWriterTask() throws PulsarClientException {
            // TODO: 2/9/23 初始化producer 
            producer = createProducer();
            // todo 以固定的时间(默认60s） 执行 catchingAndLoggingThrowables()
            resourceUsagePublishTask = pulsarService.getExecutor().scheduleAtFixedRate(
                    catchingAndLoggingThrowables(this), // todo this就是class本生,因为其实现了Runnable，所以会执行run方法
                    pulsarService.getConfig().getResourceUsageTransportPublishIntervalInSecs(), // TODO: 2/9/23 这里可以通过调节上报时间, e.g 5s
                    pulsarService.getConfig().getResourceUsageTransportPublishIntervalInSecs(),
                    TimeUnit.SECONDS);
        }

        @Override
        public synchronized void run() {
            if (resourceUsagePublishTask.isCancelled()) {
                return;
            }
            // TODO: 2/14/23 broker上报的流程为：
            /**
             * 1. 在RG和namespace绑定好以后，会把RG放入到 publisherMap 里面
             * 2. 每个RG都有 monitoringClassFields 数组，里面记录了RG的publish，dispatch的网络信息
             * 3. 聚合函数会收集所有topic的流量信息，把流量信息更新到对应的RG的usedLocallySinceLastReport里面
             * 4. 该task里面的producer会收集该broker上面所有的RG以及RG对应的网络使用信息，进行上报到内部topic，
             *    同时会重置usedLocallySinceLastReport=0
             */
            if (!publisherMap.isEmpty()) {
                // TODO: 2/14/23 这里是包含该broker上面的所有RG的网络使用情况信息
                /**
                 * {
                 *    broker : “broker-1.example.com”,
                 *    usage : {
                 *       “resource-group-1” : {
                 *          topics: 1,
                 *          publishedMsg : 100,
                 *          publishedBytes : 100000,
                 *       },
                 *       “resource-group-2” : {
                 *          topics: 1,
                 *          publishedMsg : 1000,
                 *          publishedBytes : 500000,
                 *       },
                 *       “resource-group-3” : {
                 *          topics: 1,
                 *          publishedMsg : 80000,
                 *          publishedBytes : 9999999,
                 *       },
                 *    }
                 * }
                 */
                ResourceUsageInfo rUsageInfo = new ResourceUsageInfo();
                // TODO: 2/9/23 获取broker的url
                rUsageInfo.setBroker(pulsarService.getBrokerServiceUrl());

                // todo 如果这个map里面有值，则把所有的信息加入到rUsageInfo里面，然后通过producer写入到内部topic里. key, value 例子。
                //  key： resourcegroup 名称
                //  value: 对应的流量信息
                /**
                 * “resource-group-1” : {
                 *          topics: 1,
                 *          publishedMsg : 100,
                 *          publishedBytes : 100000,
                 *       }
                 */
                publisherMap.forEach((key, item) -> item.fillResourceUsage(rUsageInfo.addUsageMap()));

                ByteBuf buf = PulsarByteBufAllocator.DEFAULT.heapBuffer(rUsageInfo.getSerializedSize());
                rUsageInfo.writeTo(buf);

                // TODO: 2/9/23 把该broker的流量信息写入到内部topic里面（broker流量数据上报）
                producer.sendAsync(buf.nioBuffer()).whenComplete((id, ex) -> {
                    if (null != ex) {
                        LOG.error("Resource usage publisher: error sending message ID {}", id, ex);
                    }
                    buf.release();
                });
            }
        }

        @Override
        public synchronized void close() throws Exception {
            resourceUsagePublishTask.cancel(true);
            producer.close();
        }
    }

    // TODO: 2/9/23 从该topic里面读取所有broker的流量信息，并将存储在一个Hasp Map中，之后Broker内部就会进行相应的处理。根据Resourcegroup的流量配额使用情况，调整自己的现在流量
    private class ResourceUsageReader implements ReaderListener<byte[]>, AutoCloseable {
        // TODO: 2/14/23 每个broker上报到内部topic的流量使用情况，其数据为
        /**
         * {
         *    broker : “brokerIP”,
         *    usage : {
         *       “resource-group-1” : {
         *          topics: 1,
         *          publishedMsg : 100,
         *          publishedBytes : 100000,
         *       },
         *       “resource-group-2” : {
         *          topics: 1,
         *          publishedMsg : 1000,
         *          publishedBytes : 500000,
         *       },
         *       “resource-group-3” : {
         *          topics: 1,
         *          publishedMsg : 80000,
         *          publishedBytes : 9999999,
         *       },
         *    }
         * }
         */
        private final ResourceUsageInfo recdUsageInfo = new ResourceUsageInfo();

        // reader定义
        private final Reader<byte[]> consumer;

        public ResourceUsageReader() throws PulsarClientException {
            // TODO: 2/9/23 初始化consumer，目的就是读取内部topic的数据
            consumer =  pulsarClient.newReader()
                    .topic(RESOURCE_USAGE_TOPIC_NAME) // todo 读取内部topic
                    .startMessageId(MessageId.latest) // todo 从最后的message开始读取
                    .readerListener(this)
                    .create();
            }

        @Override
        public void close() throws Exception {
            consumer.close();
        }

        // TODO: 2/13/23 当生产者生产一条消息到内部topic的时候，消费者可以自动调用该方法进行消费
        @Override
        public void received(Reader<byte[]> reader, Message<byte[]> msg) {
            /**
             * 消费内部topic的流程为：
             * 1. 内部topic只要有其他broker上报的一条消息，每个broker的这个consumer就会消费这条消息
             * 2. 该消息只是包含一个broker上报的RG的网络信息
             * 3. 把对应的RG的网络信息更新到该broker内部的RG的 usageFromOtherBrokersLock 里面。对于一个RG来说，就知道了该RG在所有broker的网络使用信息了
             */
            long publishTime = msg.getPublishTime(); // todo 消息发布时间
            long currentTime = System.currentTimeMillis();
            long timeDelta = currentTime - publishTime;

            // TODO: 2/14/23 这里每次收到的是一条消息，然后处理。该消息为一个broker写入到内部topic的
            recdUsageInfo.parseFrom(Unpooled.wrappedBuffer(msg.getData()), msg.getData().length);
            // TODO: 2/13/23 判断生产消息和消费消息的时间间隔， 默认为120s
            if (timeDelta > TimeUnit.SECONDS.toMillis(
            2 * pulsarService.getConfig().getResourceUsageTransportPublishIntervalInSecs())) {
                LOG.error("Stale resource usage msg from broker {} publish time {} current time{}",
                recdUsageInfo.getBroker(), publishTime, currentTime);
                return;
            }
            try {
                recdUsageInfo.getUsageMapsList().forEach(ru -> {
                    // TODO: 2/14/23 owner=resourceGroup Name。
                    ResourceUsageConsumer owner = consumerMap.get(ru.getOwner());
                    if (owner != null) {
                        // TODO: 2/14/23 这里只是更新该resource group网络使用情况
                        owner.acceptResourceUsage(recdUsageInfo.getBroker(), ru);
                    }
                });

            } catch (IllegalStateException exception) {
                LOG.error("Resource usage reader: Error parsing incoming message", exception);
            } catch (Exception exception) {
                LOG.error("Resource usage reader: Unknown exception while parsing message", exception);
            }
        }

    }

    private static final Logger LOG = LoggerFactory.getLogger(ResourceUsageTopicTransportManager.class);
    // TODO: 2/9/23 使用内部建立一个non-persistent模式的topic进行broker之间的流量通信。所有的broker都会把自己的流量信息上报到该topic，
    //  每一个broker又都会读取该topic，进行broker流量分析处理
    public  static final String RESOURCE_USAGE_TOPIC_NAME = "non-persistent://pulsar/system/resource-usage";
    private final PulsarService pulsarService;
    private final PulsarClient pulsarClient;
    // todo 生产者，broker流量数据写入到内部topic
    private final ResourceUsageWriterTask pTask;
    // todo 消费者，broker读取内部topic的中各个broker的流量数据信息
    private final ResourceUsageReader consumer;
    // TODO: 2/9/23 broker的流量使用情况map。数据传输格式：
    /**
     * {
     *    broker : “broker-1.example.com”,
     *    usage : {
     *       “resource-group-1” : {
     *          topics: 1,
     *          publishedMsg : 100,
     *          publishedBytes : 100000,
     *       },
     *       “resource-group-2” : {
     *          topics: 1,
     *          publishedMsg : 1000,
     *          publishedBytes : 500000,
     *       },
     *       “resource-group-3” : {
     *          topics: 1,
     *          publishedMsg : 80000,
     *          publishedBytes : 9999999,
     *       },
     *    }
     * }
     */
    private final Map<String, ResourceUsagePublisher>
            publisherMap = new ConcurrentHashMap<String, ResourceUsagePublisher>();
    private final Map<String, ResourceUsageConsumer>
            consumerMap = new ConcurrentHashMap<String, ResourceUsageConsumer>();

    // TODO: 2/9/23 创建租户和namespace
    private void createTenantAndNamespace() throws PulsarServerException, PulsarAdminException {
        // TODO: 2/9/23 因为是系统的内部topic，因此在public/default下面
        // Create a public tenant and default namespace
        TopicName topicName = TopicName.get(RESOURCE_USAGE_TOPIC_NAME);

        // TODO: 2/14/23 获取admin客户端
        PulsarAdmin admin = pulsarService.getAdminClient();
        ServiceConfiguration config = pulsarService.getConfig();
        String cluster = config.getClusterName();

        // TODO: 2/9/23 获取到租户和namespace
        final String tenant = topicName.getTenant();
        final String namespace = topicName.getNamespace();

        List<String> tenantList =  admin.tenants().getTenants();
        // todo 如果不存在，则创建tenant
        if (!tenantList.contains(tenant)) {
            try {
                admin.tenants().createTenant(tenant,
                  new TenantInfoImpl(Sets.newHashSet(config.getSuperUserRoles()), Sets.newHashSet(cluster)));
            } catch (PulsarAdminException ex1) {
                if (!(ex1 instanceof PulsarAdminException.ConflictException)) {
                    LOG.error("Unexpected exception {} when creating tenant {}", ex1, tenant);
                    throw ex1;
                }
            }
        }
        List<String> nsList = admin.namespaces().getNamespaces(tenant);
        // todo 如果不存在，则创建namespace
        if (!nsList.contains(namespace)) {
            try {
                admin.namespaces().createNamespace(namespace);
            } catch (PulsarAdminException ex1) {
                if (!(ex1 instanceof PulsarAdminException.ConflictException)) {
                    LOG.error("Unexpected exception {} when creating namespace {}", ex1, namespace);
                    throw ex1;
                }
            }
        }
    }

    // TODO: 2/9/23 初始化ResourceUsageTopicTransportManager实例
    public ResourceUsageTopicTransportManager(PulsarService pulsarService)
        throws PulsarServerException, PulsarAdminException, PulsarClientException {
        this.pulsarService = pulsarService;
        // TODO: 2/14/23 生产者会使用到这个客户端
        this.pulsarClient = pulsarService.getClient();

        try {
            // TODO: 2/9/23  因为是系统的内部topic，因此在public/default下面，所以，先要检查租户和namespace，如果不存在，则创建
            createTenantAndNamespace();
            consumer = new ResourceUsageReader();
            pTask = new ResourceUsageWriterTask();
        } catch (Exception ex) {
            LOG.error("Error initializing resource usage transport manager", ex);
            throw ex;
        }
    }

    /*
     * Register a resource owner (resource-group, tenant, namespace, topic etc).
     *
     * @param resource usage publisher
     */
    public void registerResourceUsagePublisher(ResourceUsagePublisher r) {
        publisherMap.put(r.getID(), r);
    }

    /*
     * Unregister a resource owner (resource-group, tenant, namespace, topic etc).
     *
     * @param resource usage publisher
     */
    public void unregisterResourceUsagePublisher(ResourceUsagePublisher r) {
        publisherMap.remove(r.getID());
    }

    /*
     * Register a resource owner (resource-group, tenant, namespace, topic etc).
     *
     * @param resource usage consumer
     */
    public void registerResourceUsageConsumer(ResourceUsageConsumer r) {
        consumerMap.put(r.getID(), r);
    }

    /*
     * Unregister a resource owner (resource-group, tenant, namespace, topic etc).
     *
     * @param resource usage consumer
     */
    public void unregisterResourceUsageConsumer(ResourceUsageConsumer r) {
        consumerMap.remove(r.getID());
    }

    @Override
    public void close() throws Exception {
        try {
            pTask.close();
            consumer.close();
        } catch (Exception ex1) {
            LOG.error("Error closing producer/consumer for resource-usage topic", ex1);
        }
    }
}
