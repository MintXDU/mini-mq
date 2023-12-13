package com.github.houbb.mq.broker;

import com.github.houbb.mq.broker.core.MqBroker;
import com.github.houbb.mq.common.util.CuratorUtils;
import org.apache.curator.framework.CuratorFramework;

public class Main {
    public static void main(String[] args) {
        CuratorFramework zkClient = CuratorUtils.getZkClient();
        CuratorUtils.deleteNode(zkClient, "/my-mq/master-broker-port");
        MqBroker mqBroker1 = new MqBroker("broker1", 9999);
        MqBroker mqBroker2 = new MqBroker("broker2", 9998);
        MqBroker mqBroker3 = new MqBroker("broker3", 9997);
        mqBroker1.start();
        mqBroker2.start();
        mqBroker3.start();
    }
}
