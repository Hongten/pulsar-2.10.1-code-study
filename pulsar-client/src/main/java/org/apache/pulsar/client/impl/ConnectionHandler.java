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
package org.apache.pulsar.client.impl;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.HandlerState.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionHandler {
    // TODO: 10/18/23 用于原子更新ClientCnx
    private static final AtomicReferenceFieldUpdater<ConnectionHandler, ClientCnx> CLIENT_CNX_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(ConnectionHandler.class, ClientCnx.class, "clientCnx");
    // TODO: 10/18/23 封装底层的客户端控制上下文，是实际的处理网络连接的
    @SuppressWarnings("unused")
    private volatile ClientCnx clientCnx = null;

    // TODO: 10/18/23 客户端句柄状态
    protected final HandlerState state;
    // TODO: 10/18/23 用于自动重连的时间组件
    protected final Backoff backoff;
    private static final AtomicLongFieldUpdater<ConnectionHandler> EPOCH_UPDATER = AtomicLongFieldUpdater
            .newUpdater(ConnectionHandler.class, "epoch");
    // Start with -1L because it gets incremented before sending on the first connection
    private volatile long epoch = -1L;
    protected volatile long lastConnectionClosedTimestamp = 0L;

    // TODO: 10/18/23 connection 用于回调 
    interface Connection {
        void connectionFailed(PulsarClientException exception);
        void connectionOpened(ClientCnx cnx);
    }

    protected Connection connection;

    protected ConnectionHandler(HandlerState state, Backoff backoff, Connection connection) {
        // TODO: 2/24/23 由于ProducerImpl实现了ConnectionHandler.Connection，并且继承了HandlerState，
        //  因此this本身就有HandlerState，Connection功能
        this.state = state;
        this.connection = connection;
        this.backoff = backoff;
        CLIENT_CNX_UPDATER.set(this, null);
    }

    // TODO: 2/24/23 查找topic所在的broker地址，然后建立到broker的连接
    protected void grabCnx() {
        // TODO: 10/18/23 如果ClientCnx不为空，则直接返回，因为已经设置过了，这时候就忽略重连请求
        if (CLIENT_CNX_UPDATER.get(this) != null) {
            log.warn("[{}] [{}] Client cnx already set, ignoring reconnection request",
                    state.topic, state.getHandlerName());
            return;
        }

        // TODO: 10/18/23 判定下是否能重连，只有Uninitialized、Connecting、Ready才能重连
        if (!isValidStateForReconnection()) {
            // Ignore connection closed when we are shutting down
            log.info("[{}] [{}] Ignoring reconnection request (state: {})",
                    state.topic, state.getHandlerName(), state.getState());
            return;
        }

        // TODO: 10/18/23 这时候，客户端开始获取连接 ， 如果连接发送异常，则延后重连
        try {
            state.client.getConnection(state.topic) // TODO: 2/24/23 和broker创建连接
                    // TODO: 10/18/23 如果连接成功，则执行 Connection 接口中的 connectionOpened 方法
                    .thenAccept(cnx -> connection.connectionOpened(cnx)) // TODO: 2/24/23  发送PRODUCER请求到broker，进行broker端Producer的初始化
                    .exceptionally(this::handleConnectionError);
        } catch (Throwable t) {
            log.warn("[{}] [{}] Exception thrown while getting connection: ", state.topic, state.getHandlerName(), t);
            reconnectLater(t);
        }
    }

    private Void handleConnectionError(Throwable exception) {
        log.warn("[{}] [{}] Error connecting to broker: {}",
                state.topic, state.getHandlerName(), exception.getMessage());
        if (exception instanceof PulsarClientException) {
            connection.connectionFailed((PulsarClientException) exception);
        } else if (exception.getCause() instanceof  PulsarClientException) {
            connection.connectionFailed((PulsarClientException) exception.getCause());
        } else {
            connection.connectionFailed(new PulsarClientException(exception));
        }

        State state = this.state.getState();
        if (state == State.Uninitialized || state == State.Connecting || state == State.Ready) {
            reconnectLater(exception);
        }

        return null;
    }

    protected void reconnectLater(Throwable exception) {
        CLIENT_CNX_UPDATER.set(this, null);
        if (!isValidStateForReconnection()) {
            log.info("[{}] [{}] Ignoring reconnection request (state: {})",
                    state.topic, state.getHandlerName(), state.getState());
            return;
        }
        long delayMs = backoff.next();
        log.warn("[{}] [{}] Could not get connection to broker: {} -- Will try again in {} s",
                state.topic, state.getHandlerName(),
                exception.getMessage(), delayMs / 1000.0);
        if (state.changeToConnecting()) {
            state.client.timer().newTimeout(timeout -> {
                log.info("[{}] [{}] Reconnecting after connection was closed", state.topic, state.getHandlerName());
                grabCnx();
            }, delayMs, TimeUnit.MILLISECONDS);
        } else {
            log.info("[{}] [{}] Ignoring reconnection request (state: {})",
                    state.topic, state.getHandlerName(), state.getState());
        }
    }

    public void connectionClosed(ClientCnx cnx) {
        lastConnectionClosedTimestamp = System.currentTimeMillis();
        state.client.getCnxPool().releaseConnection(cnx);
        if (CLIENT_CNX_UPDATER.compareAndSet(this, cnx, null)) {
            if (!isValidStateForReconnection()) {
                log.info("[{}] [{}] Ignoring reconnection request (state: {})",
                        state.topic, state.getHandlerName(), state.getState());
                return;
            }
            long delayMs = backoff.next();
            state.setState(State.Connecting);
            log.info("[{}] [{}] Closed connection {} -- Will try again in {} s",
                    state.topic, state.getHandlerName(), cnx.channel(),
                    delayMs / 1000.0);
            state.client.timer().newTimeout(timeout -> {
                log.info("[{}] [{}] Reconnecting after timeout", state.topic, state.getHandlerName());
                grabCnx();
            }, delayMs, TimeUnit.MILLISECONDS);
        }
    }

    protected void resetBackoff() {
        backoff.reset();
    }

    public ClientCnx cnx() {
        return CLIENT_CNX_UPDATER.get(this);
    }

    protected void setClientCnx(ClientCnx clientCnx) {
        CLIENT_CNX_UPDATER.set(this, clientCnx);
    }

    /**
     * Update the {@link ClientCnx} for the class, then increment and get the epoch value. Note that the epoch value is
     * currently only used by the {@link ProducerImpl}.
     * @param clientCnx - the new {@link ClientCnx}
     * @return the epoch value to use for this pair of {@link ClientCnx} and {@link ProducerImpl}
     */
    protected long switchClientCnx(ClientCnx clientCnx) {
        setClientCnx(clientCnx);
        return EPOCH_UPDATER.incrementAndGet(this);
    }

    private boolean isValidStateForReconnection() {
        State state = this.state.getState();
        switch (state) {
            case Uninitialized:
            case Connecting:
            case RegisteringSchema:
            case Ready:
                // Ok
                return true;

            case Closing:
            case Closed:
            case Failed:
            case ProducerFenced:
            case Terminated:
                return false;
        }
        return false;
    }

    public long getEpoch() {
        return epoch;
    }

    private static final Logger log = LoggerFactory.getLogger(ConnectionHandler.class);
}
