package com.github.houbb.mq.consumer;

import com.alibaba.fastjson.JSON;
import com.github.houbb.mq.common.dto.req.MqMessage;
import com.github.houbb.mq.common.resp.ConsumerStatus;
import com.github.houbb.mq.consumer.api.IMqConsumerListener;
import com.github.houbb.mq.consumer.api.IMqConsumerListenerContext;
import com.github.houbb.mq.consumer.core.MqConsumerPush;

public class Main {
    public static void main(String[] args) {
        final MqConsumerPush mqConsumerPush = new MqConsumerPush();
        mqConsumerPush.start();

        mqConsumerPush.subscribe("TOPIC", "TAGA");
        mqConsumerPush.registerListener(new IMqConsumerListener() {
            @Override
            public ConsumerStatus consumer(MqMessage mqMessage, IMqConsumerListenerContext context) {
                System.out.println("----------- 自定义" + JSON.toJSONString(mqMessage));
                return ConsumerStatus.SUCCESS;
            }
        });
    }
}
