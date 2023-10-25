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

// TODO: 2/9/23 对于接口 ResourceUsageTransportManager，其实现这个接口的类共两个
//  1. ResourceUsageDisabledTransportManager，这个类重载了接口的所有方法，但是都没有具体的实现，因此属于空壳
//  2. ResourceUsageTopicTransportManager， 该类实现了接口的所有方法，并且提供了具体的实现，因此，在我们的配置文件中，需要配置这个类来开启transport manager
public interface ResourceUsageTransportManager extends AutoCloseable {

  // TODO: 2/9/23 该方法registerResourceUsagePublisher主要是提供ResourceUsagePublisher注册
  /*
   * Register a resource owner (resource-group, tenant, namespace, topic etc).
   *
   * @param resource usage publisher
   */
  void registerResourceUsagePublisher(ResourceUsagePublisher r);

  /*
   * Unregister a resource owner (resource-group, tenant, namespace, topic etc).
   *
   * @param resource usage publisher
   */
  void unregisterResourceUsagePublisher(ResourceUsagePublisher r);

  /*
   * Register a resource owner (resource-group, tenant, namespace, topic etc).
   *
   * @param resource usage consumer
   */
  void registerResourceUsageConsumer(ResourceUsageConsumer r);

  /*
   * Unregister a resource owner (resource-group, tenant, namespace, topic etc).
   *
   * @param resource usage consumer
   */
  void unregisterResourceUsageConsumer(ResourceUsageConsumer r);

  // todo 这里的ResourceUsageDisabledTransportManager虽然实现了ResourceUsageTransportManager，但是里面的方法却并没有具体实现
  ResourceUsageTransportManager DISABLE_RESOURCE_USAGE_TRANSPORT_MANAGER = new ResourceUsageDisabledTransportManager();

  class ResourceUsageDisabledTransportManager implements ResourceUsageTransportManager {
    @Override
    public void registerResourceUsagePublisher(ResourceUsagePublisher r) {
    }

    @Override
    public void unregisterResourceUsagePublisher(ResourceUsagePublisher r) {
    }

    @Override
    public void registerResourceUsageConsumer(ResourceUsageConsumer r) {
    }

    public void unregisterResourceUsageConsumer(ResourceUsageConsumer r) {
    }

    @Override
    public void close() throws Exception {

    }
  }

}
