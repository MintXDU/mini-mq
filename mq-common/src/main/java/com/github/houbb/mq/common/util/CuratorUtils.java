package com.github.houbb.mq.common.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
public final class CuratorUtils {
    private static final String ZK_REGISTER_ROOT_PATH = "/my-mq";
    private static final String DEFAULT_ZOOKEEPER_ADDRESS = "127.0.0.1:2181";
    private static final int BASE_SLEEP_TIME = 1000;
    private static final int MAX_RETRIES = 3;
    private static CuratorFramework zkClient;

    public static CuratorFramework getZkClient() {
        // if zkClient has been started, return directly
        if (zkClient != null && zkClient.getState() == CuratorFrameworkState.STARTED) {
            return zkClient;
        }

        // Retry strategy. Retry 3 times, and will increase the sleep time between retries.
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(BASE_SLEEP_TIME, MAX_RETRIES);
        zkClient = CuratorFrameworkFactory.builder()
                .connectString(DEFAULT_ZOOKEEPER_ADDRESS)
                .retryPolicy(retryPolicy)
                .build();
        zkClient.start();
        try {
            // wait 30s util connect to the zookeeper
            if (!zkClient.blockUntilConnected(30, TimeUnit.SECONDS)) {
                throw new RuntimeException("Time out waiting to connect to ZK!");
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return zkClient;
    }

    public static boolean createEphemeralNode(CuratorFramework zkClient, String path, String brokerName) {
        try {
            if (zkClient.checkExists().forPath(path) != null) {
                log.info("The node already exists. The node is: [{}/{}]", path, CuratorUtils.getChildNodes(zkClient, "master-broker-port"));
                return false;
            } else {
                zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(path, brokerName.getBytes());
                log.info("The node was created successfully. The node is: [{}/{}]", path, CuratorUtils.getChildNodes(zkClient, "master-broker-port"));
                return true;
            }
        } catch (Exception e) {
            log.error("create ephemeral node for path [{}] fail", path);
        }
        return false;
    }

    public static void deleteNode(CuratorFramework zkClient, String path) {
        try {
            zkClient.delete().deletingChildrenIfNeeded().forPath(path);
            log.info("The node was deleted successfully. The node is: [{}]", path);
        } catch (Exception e) {
            log.error("delete node for path [{}] fail", path);
        }
    }

    public static String getChildNodes(CuratorFramework zkClient, String brokerName) {
        String result = null;
        String brokerPath = ZK_REGISTER_ROOT_PATH + "/" + brokerName;
        try {
            result = new String(zkClient.getData().forPath(brokerPath));
            registerWatcher(brokerName, zkClient);
            log.info("get children nodes for path [{}] successfully", brokerPath);
        } catch (Exception e) {
            log.error("get children nodes for path [{}] fail", brokerPath);
        }
        return result;
    }

    private static void registerWatcher(String brokerName, CuratorFramework zkClient) throws Exception {
        final String brokerPath = ZK_REGISTER_ROOT_PATH + "/" + brokerName;
        PathChildrenCache pathChildrenCache = new PathChildrenCache(zkClient, brokerPath, true);
        PathChildrenCacheListener pathChildrenCacheListener = (curatorFramework, pathChildrenCacheEvent) -> {
            List<String> serviceAddresses = curatorFramework.getChildren().forPath(brokerPath);
        };
        pathChildrenCache.getListenable().addListener(pathChildrenCacheListener);
        pathChildrenCache.start();
    }
}
