package com.github.houbb.mq.producer;

import com.alibaba.fastjson.JSON;
import com.github.houbb.mq.common.dto.req.MqMessage;
import com.github.houbb.mq.producer.core.MqProducer;
import com.github.houbb.mq.producer.dto.SendResult;

import java.util.Arrays;

public class Main {
    public static void main(String[] args) {
        MqProducer mqProducer = new MqProducer();
        mqProducer.start();
        String message = "HELLO MQ2!";
        MqMessage mqMessage = new MqMessage();
        mqMessage.setTopic("TOPIC");
        mqMessage.setTags(Arrays.asList("TAGA", "TAGB"));
        mqMessage.setPayload(message);

        SendResult sendResult = mqProducer.send(mqMessage);
        System.out.println(JSON.toJSON(sendResult));
    }
}
