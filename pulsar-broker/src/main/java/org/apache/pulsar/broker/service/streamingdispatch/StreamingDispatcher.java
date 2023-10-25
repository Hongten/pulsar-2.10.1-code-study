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
package org.apache.pulsar.broker.service.streamingdispatch;

import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.pulsar.broker.service.Dispatcher;
import org.apache.pulsar.common.classification.InterfaceStability;
// TODO: 2/22/23 streamingDispatcher和其他Dispatcher的区别在于，每读到一条消息就立即返回给Consumer，而不会等一批消息都读完后才返回。
//  其内部使用了StreamingEntryReader来读取数据。e.g要读10个Entry，普通的Dispatcher会使用bookie client，让bookie返回10个entry，
//  bookie读取完后会在一次响应中返回10个entry。而StreamingEntryReader会封装10个读取请求并分别发送，最终所有请求都在Client的同一个
//  线程中排队读取数据，但每个请求读完数据后都会触发一次读取完成事件，在事件中StreamingEntryReader把消息发给Consumer，而不需要等待
//  所有的数据都读完再返回。这样消息能更加及时地返回给Consumer
/**
 * A {@link Dispatcher} that'll use {@link StreamingEntryReader} to read entries from {@link ManagedLedger}.
 */
@InterfaceStability.Unstable
public interface StreamingDispatcher extends Dispatcher {

    /**
     * Notify dispatcher issued read entry request has complete.
     * @param entry Entry read.
     * @param ctx   Context passed in when issuing read entries request.
     */
    void readEntryComplete(Entry entry, PendingReadEntryRequest ctx);

    /**
     * Notify dispatcher can issue next read request.
     */
    void canReadMoreEntries(boolean withBackoff);

    /**
     * Notify dispatcher to inform consumers reached end of topic.
     */
    void notifyConsumersEndOfTopic();

    /**
     * @return Name of the dispatcher.
     */
    String getName();
}
