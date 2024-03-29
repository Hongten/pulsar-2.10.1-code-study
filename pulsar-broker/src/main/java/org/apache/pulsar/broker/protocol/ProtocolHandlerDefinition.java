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
package org.apache.pulsar.broker.protocol;

import lombok.Data;
import lombok.NoArgsConstructor;

// TODO: 2/15/23 协议里面的yml文件内容
/**
 * name: kafka
 * description: Kafka Protocol Handler
 * handlerClass: io.streamnative.pulsar.handlers.kop.KafkaProtocolHandler
 */

/**
 * Metadata information about a Pulsar protocol handler.
 */
@Data
@NoArgsConstructor
public class ProtocolHandlerDefinition {

    /**
     * The name of the protocol.
     */
    private String name;

    /**
     * The description of the protocol handler to be used for user help.
     */
    private String description;

    /**
     * The class name for the protocol handler.
     */
    private String handlerClass;

}
