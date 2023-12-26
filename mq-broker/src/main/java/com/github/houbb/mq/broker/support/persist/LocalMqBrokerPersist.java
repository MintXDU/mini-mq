package com.github.houbb.mq.broker.support.persist;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.github.houbb.heaven.util.util.CollectionUtil;
import com.github.houbb.heaven.util.util.MapUtil;
import com.github.houbb.heaven.util.util.regex.RegexUtil;
import com.github.houbb.log.integration.core.Log;
import com.github.houbb.log.integration.core.LogFactory;
import com.github.houbb.mq.broker.dto.persist.MqMessagePersistPut;
import com.github.houbb.mq.broker.dto.persist.MqMessagePersistPutBatch;
import com.github.houbb.mq.common.constant.MessageStatusConst;
import com.github.houbb.mq.common.dto.req.MqConsumerPullReq;
import com.github.houbb.mq.common.dto.req.MqMessage;
import com.github.houbb.mq.common.dto.req.component.MqConsumerUpdateStatusDto;
import com.github.houbb.mq.common.dto.resp.MqCommonResp;
import com.github.houbb.mq.common.dto.resp.MqConsumerPullResp;
import com.github.houbb.mq.common.resp.MqCommonRespCode;
import io.netty.channel.Channel;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 本地持久化策略
 * @author binbin.hou
 * @since 1.0.0
 */
public class LocalMqBrokerPersist implements IMqBrokerPersist {

    private static final Log log = LogFactory.getLog(LocalMqBrokerPersist.class);

    /**
     * 队列
     * ps: 这里只是简化实现，暂时不考虑并发等问题。
     */
    private final Map<String, List<MqMessagePersistPut>> map = new ConcurrentHashMap<>();

    public LocalMqBrokerPersist() {
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader("mq-data.json"))) {
            String stringMqMap = bufferedReader.readLine();
            Map<String, List<MqMessagePersistPut>> mqMap = JSON.parseObject(stringMqMap, Map.class);

            // 清空本地的消息队列
            map.clear();

            // 将磁盘里持久化过的 mq 赋值给本地消息队列
            for (Map.Entry<String, List<MqMessagePersistPut>> entry: mqMap.entrySet()) {
                String key = entry.getKey();
                List<MqMessagePersistPut> value = entry.getValue();
                map.put(key, value);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Map<String, List<MqMessagePersistPut>> getMap() {
        return this.map;
    }

    //1. 接收
    //2. 持久化
    //3. 通知消费
    @Override
    public synchronized MqCommonResp put(MqMessagePersistPut put) {
        this.doPut(put);

        MqCommonResp commonResp = new MqCommonResp();
        commonResp.setRespCode(MqCommonRespCode.SUCCESS.getCode());
        commonResp.setRespMessage(MqCommonRespCode.SUCCESS.getMsg());
        return commonResp;
    }

    private void doPut(MqMessagePersistPut put) {
        log.info("put elem: {}", JSON.toJSON(put));

        MqMessage mqMessage = put.getMqMessage();
        final String topic = mqMessage.getTopic();

        // 放入元素
        MapUtil.putToListMap(map, topic, put);

        String mqMapJson = JSON.toJSONString(map);

        // 存储 mqMap 到本地文件中
        try (FileWriter fileWriter = new FileWriter("mq-data.json")) {
            fileWriter.write(mqMapJson);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public MqCommonResp putBatch(List<MqMessagePersistPut> putList) {
        // 构建列表
        for(MqMessagePersistPut put : putList) {
            this.doPut(put);
        }

        MqCommonResp commonResp = new MqCommonResp();
        commonResp.setRespCode(MqCommonRespCode.SUCCESS.getCode());
        commonResp.setRespMessage(MqCommonRespCode.SUCCESS.getMsg());
        return commonResp;
    }

    @Override
    public MqCommonResp updateStatus(String messageId,
                                     String consumerGroupName,
                                     String status) {
        // 这里性能比较差，所以不可以用于生产。仅作为测试验证
        this.doUpdateStatus(messageId, consumerGroupName, status);

        MqCommonResp commonResp = new MqCommonResp();
        commonResp.setRespCode(MqCommonRespCode.SUCCESS.getCode());
        commonResp.setRespMessage(MqCommonRespCode.SUCCESS.getMsg());
        return commonResp;
    }


    private void doUpdateStatus(String messageId,
                                String consumerGroupName,
                                String status) {
        // 这里性能比较差，所以不可以用于生产。仅作为测试验证
        for(List<MqMessagePersistPut> list : map.values()) {
            for(MqMessagePersistPut put : list) {
                MqMessage mqMessage = put.getMqMessage();
                if(mqMessage.getTraceId().equals(messageId)) {
                    put.setMessageStatus(status);

                    break;
                }
            }
        }
    }

    @Override
    public MqCommonResp updateStatusBatch(List<MqConsumerUpdateStatusDto> statusDtoList) {
        for(MqConsumerUpdateStatusDto statusDto : statusDtoList) {
            this.doUpdateStatus(statusDto.getMessageId(), statusDto.getConsumerGroupName(),
                    statusDto.getMessageStatus());
        }

        MqCommonResp commonResp = new MqCommonResp();
        commonResp.setRespCode(MqCommonRespCode.SUCCESS.getCode());
        commonResp.setRespMessage(MqCommonRespCode.SUCCESS.getMsg());
        return commonResp;
    }

    @Override
    public MqConsumerPullResp pull(MqConsumerPullReq pullReq, Channel channel) {
        //1. 拉取匹配的信息
        //2. 状态更新为代理中
        //3. 如何更新对应的消费状态呢？

        // 获取状态为 W 的订单
        final int fetchSize = pullReq.getSize();
        final String topic = pullReq.getTopicName();
        final String tagRegex = pullReq.getTagRegex();

        List<MqMessage> resultList = new ArrayList<>(fetchSize);
        List<MqMessagePersistPut> putList = map.get(topic);
        // 性能比较差
        if(CollectionUtil.isNotEmpty(putList)) {
            for(MqMessagePersistPut put : putList) {
                if(!isEnableStatus(put)) {
                    continue;
                }

                final MqMessage mqMessage = put.getMqMessage();
                List<String> tagList = mqMessage.getTags();
                if(RegexUtil.hasMatch(tagList, tagRegex)) {
                    // 设置为处理中
                    // TODO： 消息的最终状态什么时候更新呢？
                    // 可以给 broker 一个 ACK
                    put.setMessageStatus(MessageStatusConst.TO_CONSUMER_PROCESS);
                    resultList.add(mqMessage);
                }

                if(resultList.size() >= fetchSize) {
                    break;
                }
            }
        }

        MqConsumerPullResp resp = new MqConsumerPullResp();
        resp.setRespCode(MqCommonRespCode.SUCCESS.getCode());
        resp.setRespMessage(MqCommonRespCode.SUCCESS.getMsg());
        resp.setList(resultList);
        return resp;
    }

    @Override
    public void SynchronizeMqMap(Map<String, List<MqMessagePersistPut>> masterMqMap) {
        // 清空本地的消息队列
        map.clear();

        // 将主 broker 的队列赋值给本地消息队列
        for (Map.Entry<String, List<MqMessagePersistPut>> entry: masterMqMap.entrySet()) {
            String key = entry.getKey();
            List<MqMessagePersistPut> value = entry.getValue();
            map.put(key, value);
        }

        log.info("[BROKER CLIENT] 同步消息队列 {}", map);
    }

    private boolean isEnableStatus(final MqMessagePersistPut persistPut) {
        final String status = persistPut.getMessageStatus();
        // 数据库可以设计一个字段，比如待消费时间，进行排序。
        // 这里只是简化实现，仅用于测试。
        List<String> statusList = Arrays.asList(MessageStatusConst.WAIT_CONSUMER,
                MessageStatusConst.CONSUMER_LATER);
        return statusList.contains(status);
    }

}
