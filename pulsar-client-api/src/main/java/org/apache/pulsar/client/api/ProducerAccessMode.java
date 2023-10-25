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

/**
 * The type of access to the topic that the producer requires.
 */
public enum ProducerAccessMode {
    // TODO: 10/23/23 producer对topic的访问模式 ，默认为shared，即多个producer可以发送消息到一个topic
    /**
     * By default multiple producers can publish on a topic.
     */
    Shared,


    /**
     * todo 独占模式，即只有一个producer向topic发送数据，其他的producer创建的时候，会立刻失败
     * Require exclusive access for producer. Fail immediately if there's already a producer connected.
     */
    Exclusive,

    /**
     * todo 等待独占，即只有一个producer向topic发送数据，其他的producer可以被创建，但是都是在等待中
     * Producer creation is pending until it can acquire exclusive access.
     */
    WaitForExclusive,
}
