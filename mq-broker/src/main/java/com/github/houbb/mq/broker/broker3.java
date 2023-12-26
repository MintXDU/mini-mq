package com.github.houbb.mq.broker;

import com.github.houbb.mq.broker.core.MqBroker;
import com.github.houbb.mq.common.util.CuratorUtils;
import org.apache.curator.framework.CuratorFramework;

public class broker3 {
    public static void main(String[] args) {
        CuratorFramework zkClient = CuratorUtils.getZkClient();
        //CuratorUtils.deleteNode(zkClient, "/my-mq/master-broker-port");
        MqBroker mqBroker3 = new MqBroker("broker3", 9997);
        mqBroker3.start();
    }
}
