package com.github.houbb.mq.consumer.support.broker;

import com.github.houbb.mq.common.api.Destroyable;
import com.github.houbb.mq.common.dto.req.MqCommonReq;
import com.github.houbb.mq.common.dto.resp.MqCommonResp;
import io.netty.channel.Channel;

/**
 * @author binbin.hou
 * @since 0.0.5
 */
public interface IConsumerBrokerService extends Destroyable {

    /**
     * 初始化列表
     * @param config 配置
     * @since 0.0.5
     */
    void initChannelFutureList(final ConsumerBrokerConfig config);

    /**
     * 注册到服务端
     * @since 0.0.5
     */
    void registerToBroker();

    /**
     * 调用服务端
     * @param channel 调用通道
     * @param commonReq 通用请求
     * @param respClass 类
     * @param <T> 泛型
     * @param <R> 结果
     * @return 结果
     * @since 0.0.5
     */
    <T extends MqCommonReq, R extends MqCommonResp> R callServer(Channel channel,
                                                                 T commonReq,
                                                                 Class<R> respClass);

    /**
     * 获取请求通道
     * @param key 标识
     * @return 结果
     * @since 0.0.5
     */
    Channel getChannel(String key);

    /**
     * 订阅
     * @param topicName topic 名称
     * @param tagRegex 标签正则
     */
    void subscribe(String topicName, String tagRegex);

    /**
     * 取消订阅
     * @param topicName topic 名称
     * @param tagRegex 标签正则
     */
    void unSubscribe(String topicName, String tagRegex);

}