/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.proxy.service.relay;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.service.channel.SimpleChannel;
import org.apache.rocketmq.proxy.service.transaction.TransactionService;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.netty.NettyRemotingAbstract;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.remoting.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.remoting.protocol.header.ConsumeMessageDirectlyResultRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerRunningInfoRequestHeader;

public class LocalProxyRelayService extends AbstractProxyRelayService {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    private final BrokerController brokerController;

    public LocalProxyRelayService(BrokerController brokerController, TransactionService transactionService) {
        super(transactionService);
        this.brokerController = brokerController;
    }

    @Override
    public CompletableFuture<ProxyRelayResult<ConsumerRunningInfo>> processGetConsumerRunningInfo(
        ProxyContext context, RemotingCommand command, GetConsumerRunningInfoRequestHeader header) {
        return buildRelayFuture(context, command, result -> result.encode());
    }

    @Override
    public CompletableFuture<ProxyRelayResult<ConsumeMessageDirectlyResult>> processConsumeMessageDirectly(
        ProxyContext context, RemotingCommand command,
        ConsumeMessageDirectlyResultRequestHeader header) {
        return buildRelayFuture(context, command, result -> result.encode());
    }

    private <T> CompletableFuture<ProxyRelayResult<T>> buildRelayFuture(
        ProxyContext context, RemotingCommand command, Function<T, byte[]> encoder) {
        CompletableFuture<ProxyRelayResult<T>> future = new CompletableFuture<>();
        future.whenComplete((proxyOutResult, throwable) -> {
            try {
                if (throwable != null) {
                    log.error("Relay future completed exceptionally, remoteAddress={}", context.getRemoteAddress(), throwable);
                    return;
                }
                RemotingServer remotingServer = this.brokerController.getRemotingServer();
                if (!(remotingServer instanceof NettyRemotingAbstract)) {
                    log.warn("RemotingServer is not instance of NettyRemotingAbstract, skip relay response");
                    return;
                }
                NettyRemotingAbstract nettyRemotingAbstract = (NettyRemotingAbstract) remotingServer;
                RemotingCommand remotingCommand = RemotingCommand.createResponseCommand(null);
                remotingCommand.setOpaque(command.getOpaque());
                remotingCommand.setCode(proxyOutResult.getCode());
                remotingCommand.setRemark(proxyOutResult.getRemark());
                if (proxyOutResult.getCode() == ResponseCode.SUCCESS && proxyOutResult.getResult() != null) {
                    remotingCommand.setBody(encoder.apply(proxyOutResult.getResult()));
                }
                SimpleChannel simpleChannel = new SimpleChannel(context.getRemoteAddress(), context.getLocalAddress());
                nettyRemotingAbstract.processResponseCommand(simpleChannel.getChannelHandlerContext(), remotingCommand);
            } catch (Exception e) {
                log.error("Error processing relay result, remoteAddress={}", context.getRemoteAddress(), e);
            }
        });
        return future;
    }
}
