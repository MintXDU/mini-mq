package com.github.houbb.mq.broker.core;

import com.alibaba.fastjson.JSON;
import com.github.houbb.id.core.util.IdHelper;
import com.github.houbb.load.balance.api.ILoadBalance;
import com.github.houbb.load.balance.api.impl.LoadBalances;
import com.github.houbb.log.integration.core.Log;
import com.github.houbb.log.integration.core.LogFactory;
import com.github.houbb.mq.broker.api.IBrokerConsumerService;
import com.github.houbb.mq.broker.api.IBrokerProducerService;
import com.github.houbb.mq.broker.api.IMqBroker;
import com.github.houbb.mq.broker.constant.BrokerConst;
import com.github.houbb.mq.broker.constant.BrokerRespCode;
import com.github.houbb.mq.broker.dto.consumer.ConsumerSubscribeBo;
import com.github.houbb.mq.broker.handler.MqBrokerClientHandler;
import com.github.houbb.mq.broker.handler.MqBrokerHandler;
import com.github.houbb.mq.broker.support.api.LocalBrokerConsumerService;
import com.github.houbb.mq.broker.support.api.LocalBrokerProducerService;
import com.github.houbb.mq.broker.support.persist.IMqBrokerPersist;
import com.github.houbb.mq.broker.support.persist.LocalMqBrokerPersist;
import com.github.houbb.mq.broker.support.push.BrokerPushService;
import com.github.houbb.mq.broker.support.push.IBrokerPushService;
import com.github.houbb.mq.broker.support.valid.BrokerRegisterValidService;
import com.github.houbb.mq.broker.support.valid.IBrokerRegisterValidService;
import com.github.houbb.mq.common.constant.MethodType;
import com.github.houbb.mq.common.dto.req.MqCommonReq;
import com.github.houbb.mq.common.dto.req.MqPullReq;
import com.github.houbb.mq.common.dto.resp.MqCommonResp;
import com.github.houbb.mq.common.resp.MqException;
import com.github.houbb.mq.common.rpc.RpcChannelFuture;
import com.github.houbb.mq.common.rpc.RpcMessageDto;
import com.github.houbb.mq.common.support.invoke.IInvokeService;
import com.github.houbb.mq.common.support.invoke.impl.InvokeService;
import com.github.houbb.mq.common.util.ChannelUtil;
import com.github.houbb.mq.common.util.CuratorUtils;
import com.github.houbb.mq.common.util.DelimiterUtil;
import com.github.houbb.heaven.util.net.NetUtil;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * @author binbin.hou
 * @since 1.0.0
 */
public class MqBroker extends Thread implements IMqBroker {

    private static final Log log = LogFactory.getLog(MqBroker.class);

    /**
     * 端口号
     */
    private int port = BrokerConst.DEFAULT_PORT;

    /**
     * 调用管理类
     *
     * @since 1.0.0
     */
    private final IInvokeService invokeService = new InvokeService();

    /**
     * 消费者管理
     *
     * @since 0.0.3
     */
    private IBrokerConsumerService registerConsumerService = new LocalBrokerConsumerService();

    /**
     * 生产者管理
     *
     * @since 0.0.3
     */
    private IBrokerProducerService registerProducerService = new LocalBrokerProducerService();

    /**
     * 持久化类
     *
     * @since 0.0.3
     */
    private IMqBrokerPersist mqBrokerPersist = new LocalMqBrokerPersist();

    /**
     * 推送服务
     *
     * @since 0.0.3
     */
    private IBrokerPushService brokerPushService = new BrokerPushService();

    /**
     * 获取响应超时时间
     * @since 0.0.3
     */
    private long respTimeoutMills = 5000;

    /**
     * 拉取 MQ 定时任务
     */
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    /**
     * 负载均衡
     * @since 0.0.7
     */
    private ILoadBalance<ConsumerSubscribeBo> loadBalance = LoadBalances.weightRoundRobbin();

    /**
     * 推送最大尝试次数
     * @since 0.0.8
     */
    private int pushMaxAttempt = 3;

    /**
     * 注册验证服务类
     * @since 0.1.4
     */
    private IBrokerRegisterValidService brokerRegisterValidService = new BrokerRegisterValidService();

    /**
     * Broker 名称
     */
    private String brokerName = "broker1";

    /**
     * 副本 broker 的 channelFuture
     */
    private RpcChannelFuture rpcChannelFuture;

    /**
     * true 表示本 Broker 为主 broker
     */
    private boolean isMaster;

    public MqBroker () {

    }

    public MqBroker (String brokerName, int port) {
        this.brokerName = brokerName;
        this.port = port;

        // 向 zk 注册 master-broker
        CuratorFramework zkClient = CuratorUtils.getZkClient();
        this.isMaster = CuratorUtils.createEphemeralNode(zkClient, "/my-mq/master-broker-port", String.valueOf(port));
        if (!this.isMaster) {
            this.registerWatcher(zkClient);
        }
    }

    public MqBroker port(int port) {
        this.port = port;
        return this;
    }

    public MqBroker registerConsumerService(IBrokerConsumerService registerConsumerService) {
        this.registerConsumerService = registerConsumerService;
        return this;
    }

    public MqBroker registerProducerService(IBrokerProducerService registerProducerService) {
        this.registerProducerService = registerProducerService;
        return this;
    }

    public MqBroker mqBrokerPersist(IMqBrokerPersist mqBrokerPersist) {
        this.mqBrokerPersist = mqBrokerPersist;
        return this;
    }

    public MqBroker brokerPushService(IBrokerPushService brokerPushService) {
        this.brokerPushService = brokerPushService;
        return this;
    }

    public MqBroker respTimeoutMills(long respTimeoutMills) {
        this.respTimeoutMills = respTimeoutMills;
        return this;
    }

    public MqBroker loadBalance(ILoadBalance<ConsumerSubscribeBo> loadBalance) {
        this.loadBalance = loadBalance;
        return this;
    }

    public MqBroker pushMaxAttempt(int pushMaxAttempt) {
        this.pushMaxAttempt = pushMaxAttempt;
        return this;
    }

    public MqBroker brokerRegisterValidService(IBrokerRegisterValidService brokerRegisterValidService) {
        this.brokerRegisterValidService = brokerRegisterValidService;
        return this;
    }

    private ChannelHandler initServerChannelHandler() {
        registerConsumerService.loadBalance(this.loadBalance);

        MqBrokerHandler handler = new MqBrokerHandler();
        handler.invokeService(invokeService)
                .respTimeoutMills(respTimeoutMills)
                .registerConsumerService(registerConsumerService)
                .registerProducerService(registerProducerService)
                .mqBrokerPersist(mqBrokerPersist)
                .brokerPushService(brokerPushService)
                .respTimeoutMills(respTimeoutMills)
                .pushMaxAttempt(pushMaxAttempt)
                .brokerRegisterValidService(brokerRegisterValidService);

        return handler;
    }

    private ChannelHandler initClientChannelHandler() {
        final ByteBuf delimiterBuf = DelimiterUtil.getByteBuf(DelimiterUtil.DELIMITER);

        final MqBrokerClientHandler mqBrokerClientHandler = new MqBrokerClientHandler();

        ChannelHandler handler = new ChannelInitializer<Channel>() {

            @Override
            protected void initChannel(Channel channel) throws Exception {
                channel.pipeline()
                        .addLast(new DelimiterBasedFrameDecoder(DelimiterUtil.LENGTH, delimiterBuf))
                        .addLast(mqBrokerClientHandler);
            }
        };

        return handler;
    }

    @Override
    public void run() {
        CuratorFramework zkClient = CuratorUtils.getZkClient();
        String masterBrokerPort = CuratorUtils.getChildNodes(zkClient, "master-broker-port");

        if (isMaster) {
            // broker 为主 broker，启动为服务端。
            log.info("主 MQ 中间人 {} 开始启动，监听 port: {}", brokerName, port);

            startNettyServer();
        } else {
            // broker 为 broker 副本，启动为客户端。
            log.info("副本 MQ 中间人 {} 开始启动，连接主 broker 服务器端口 port: {}", brokerName, masterBrokerPort);

            startNettyClient(masterBrokerPort);

            // 初始化拉取
            this.initPullMQ();
        }

    }

    private void startNettyServer() {
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        try {
            final ByteBuf delimiterBuf = DelimiterUtil.getByteBuf(DelimiterUtil.DELIMITER);
            ServerBootstrap serverBootstrap = new ServerBootstrap();
            serverBootstrap.group(workerGroup, bossGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<Channel>() {
                        @Override
                        protected void initChannel(Channel ch) throws Exception {
                            ch.pipeline()
                                    .addLast(new DelimiterBasedFrameDecoder(DelimiterUtil.LENGTH, delimiterBuf))
                                    .addLast(initServerChannelHandler());
                        }
                    })
                    // 这个参数影响的是还没有被accept 取出的连接
                    .option(ChannelOption.SO_BACKLOG, 128)
                    // 这个参数只是过一段时间内客户端没有响应，服务端会发送一个 ack 包，以判断客户端是否还活着。
                    .childOption(ChannelOption.SO_KEEPALIVE, true);

            // 绑定端口，开始接收进来的链接
            ChannelFuture channelFuture = serverBootstrap.bind(port).syncUninterruptibly();
            log.info("MQ 中间人" + brokerName + "启动完成，监听【" + port + "】端口");

            channelFuture.channel().closeFuture().syncUninterruptibly();
            log.info("MQ 中间人" + brokerName + "关闭完成");
        } catch (Exception e) {
            log.error("MQ 中间人" + brokerName + "启动异常", e);
            throw new MqException(BrokerRespCode.RPC_INIT_FAILED);
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }

    private void startNettyClient(String masterBrokerPort) {
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        try {
            final String address = "127.0.0.1";
            final int port = Integer.parseInt(masterBrokerPort);
            Bootstrap bootstrap = new Bootstrap();
            ChannelFuture channelFuture = bootstrap.group(workerGroup)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .handler(new ChannelInitializer<Channel>() {
                        @Override
                        protected void initChannel(Channel ch) throws Exception {
                            ch.pipeline()
                                    .addLast(new LoggingHandler(LogLevel.INFO))
                                    .addLast(initClientChannelHandler());
                        }
                    })
                    .connect(address, port)
                    .syncUninterruptibly();
            log.info("启动副本 broker 完成，监听主 broker 的 address: {}, port: {}", address, port);
            RpcChannelFuture rpcChannelFuture = new RpcChannelFuture();
            rpcChannelFuture.setChannelFuture(channelFuture);
            rpcChannelFuture.setAddress(address);
            rpcChannelFuture.setPort(port);
            rpcChannelFuture.setWeight(1);

            this.rpcChannelFuture = rpcChannelFuture;
        } catch (Exception exception) {
            log.error("注册到 broker 服务端异常", exception);
        }
    }

    /**
     * 初始化拉取 MQ 操作
     */
    private void initPullMQ() {
        // 5S 拉取一次
        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                pullMq();
            }
        }, 5, 5, TimeUnit.SECONDS);
    }

    public void pullMq() {
        final MqPullReq req = new MqPullReq();
        final String traceId = IdHelper.uuid32();

        req.setTraceId(traceId);
        req.setMethodType(MethodType.B_PULL);
        req.setAddress(NetUtil.getLocalHost());
        req.setPort(0);
        req.setTime(System.currentTimeMillis());
        log.info("[BROKER_PULL] BROKER 副本同步队列消息", JSON.toJSON(req));

        try {
            Channel channel = rpcChannelFuture.getChannelFuture().channel();
            callServer(channel, req, null);
        } catch (Exception exception) {
            log.error("[BROKER_PULL] BROKER 副本同步队列消息请求异常", exception);
        }
    }

    public <T extends MqCommonReq, R extends MqCommonResp> R callServer(Channel channel, T commonReq, Class<R> respClass) {
        final String traceId = commonReq.getTraceId();
        final long requestTime = System.currentTimeMillis();

        RpcMessageDto rpcMessageDto = new RpcMessageDto();
        rpcMessageDto.setTraceId(traceId);
        rpcMessageDto.setRequestTime(requestTime);
        rpcMessageDto.setJson(JSON.toJSONString(commonReq));
        rpcMessageDto.setMethodType(commonReq.getMethodType());
        rpcMessageDto.setRequest(true);

        // 添加调用服务
        invokeService.addRequest(traceId, respTimeoutMills);

        // 遍历 channel
        // 关闭当前线程，以获取对应的信息
        // 使用序列化的方式
        ByteBuf byteBuf = DelimiterUtil.getMessageDelimiterBuffer(rpcMessageDto);

        //负载均衡获取 channel
        channel.writeAndFlush(byteBuf);

        String channelId = ChannelUtil.getChannelId(channel);
        log.info("[BROKER_PULL] channelId {} 发送消息 {}", channelId, JSON.toJSON(rpcMessageDto));

        return null;
    }

    private void registerWatcher(CuratorFramework zkClient) {
        final String brokerPath = "/my-mq";
        PathChildrenCache pathChildrenCache = new PathChildrenCache(zkClient, brokerPath, true);
        try {
            pathChildrenCache.start();
        } catch (Exception e) {
            e.printStackTrace();
        }

        PathChildrenCacheListener pathChildrenCacheListener = (curatorFramework, pathChildrenCacheEvent) -> {
            if (pathChildrenCacheEvent.getType() == PathChildrenCacheEvent.Type.CHILD_REMOVED) {
                String deletedPath = pathChildrenCacheEvent.getData().getPath();
                System.out.println("节点被删除：" + deletedPath);

                // 向 zk 注册 master-broker
                isMaster = CuratorUtils.createEphemeralNode(zkClient, "/my-mq/master-broker-port", String.valueOf(port));

                if (isMaster) {
                    // 如果本 broker 成功注册为 master-broker
                    Channel channel = rpcChannelFuture.getChannelFuture().channel();
                    channel.close();

                    // 停止心跳拉取消息
                    scheduledExecutorService.shutdown();

                    startNettyServer();
                } else {
                    // 如果本 broker 没注册为 master-broker
                    Channel channel = rpcChannelFuture.getChannelFuture().channel();
                    channel.close();

                    String masterBrokerPort = CuratorUtils.getChildNodes(zkClient, "master-broker-port");
                    startNettyClient(masterBrokerPort);
                }
            }
        };
        pathChildrenCache.getListenable().addListener(pathChildrenCacheListener);
    }
}
