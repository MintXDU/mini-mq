package com.github.houbb.mq.broker.handler;

import com.alibaba.fastjson.JSON;
import com.github.houbb.log.integration.core.Log;
import com.github.houbb.log.integration.core.LogFactory;
import com.github.houbb.mq.broker.dto.persist.MqMessagePersistPut;
import com.github.houbb.mq.broker.support.persist.IMqBrokerPersist;
import com.github.houbb.mq.broker.support.persist.LocalMqBrokerPersist;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MqBrokerClientHandler extends SimpleChannelInboundHandler {
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf byteBuf = (ByteBuf)msg;
        byte[] bytes = new byte[byteBuf.readableBytes()];
        byteBuf.readBytes(bytes);

        Map<String, List<MqMessagePersistPut>> mqMap = JSON.parseObject(bytes, ConcurrentHashMap.class);

        // 把从主 Broker 拉取的 mqMap 同步到本副本 Broker
        IMqBrokerPersist mqBrokerPersist = new LocalMqBrokerPersist();
        mqBrokerPersist.SynchronizeMqMap(mqMap);
    }
}
