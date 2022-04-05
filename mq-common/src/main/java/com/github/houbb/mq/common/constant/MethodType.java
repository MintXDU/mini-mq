package com.github.houbb.mq.common.constant;

/**
 * @author binbin.hou
 * @since 1.0.0
 */
public class MethodType {

    /**
     * 生产者发送消息
     */
    public static final String P_SEND_MSG = "P_SEND_MSG";

    /**
     * 生产者发送消息
     * @since 0.0.3
     */
    public static final String P_SEND_MSG_ONE_WAY = "P_SEND_MESSAGE_ONE_WAY";

    /**
     * 生产者注册
     * @since 0.0.3
     */
    public static final String P_REGISTER = "P_REGISTER";

    /**
     * 生产者取消注册
     * @since 0.0.3
     */
    public static final String P_UN_REGISTER = "P_UN_REGISTER";


    /**
     * 消费者注册
     * @since 0.0.3
     */
    public static final String C_REGISTER = "C_REGISTER";

    /**
     * 消费者取消注册
     * @since 0.0.3
     */
    public static final String C_UN_REGISTER = "C_UN_REGISTER";

    /**
     * 消费者订阅
     * @since 0.0.3
     */
    public static final String C_SUBSCRIBE = "C_SUBSCRIBE";

    /**
     * 消费者取消订阅
     * @since 0.0.3
     */
    public static final String C_UN_SUBSCRIBE = "C_UN_SUBSCRIBE";

    /**
     * 消费者消息主动拉取
     * @since 0.0.3
     */
    public static final String C_MESSAGE_PULL = "C_MESSAGE_PULL";

    /**
     * 中间人消息推送
     * @since 0.0.3
     */
    public static final String B_MESSAGE_PUSH = "B_MESSAGE_PUSH";

}