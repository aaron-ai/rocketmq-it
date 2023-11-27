package org.example;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.lang3.RandomUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.namesrv.NamesrvController;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.example.clients.Consumers;
import org.example.clients.Producers;

import static java.util.Collections.emptyMap;

public class ServerStartup {
    public static final InternalLogger logger = InternalLoggerFactory.getLogger(ServerStartup.class);

    private static String createTempDir() {
        String path = null;
        try {
            File file = Files.createTempDirectory("rocketmq-benchmark").toFile();
            path = file.getCanonicalPath();
        } catch (IOException e) {
            logger.warn("Error creating temporary directory.", e);
        }
        return path;
    }

    public static void startBroker(String namesrvAddr, String clusterName, String brokerName)
        throws Exception {
        BrokerConfig brokerConfig = new BrokerConfig();
        MessageStoreConfig storeConfig = new MessageStoreConfig();
        brokerConfig.setBrokerClusterName(clusterName);
        brokerConfig.setBrokerName(brokerName);
        brokerConfig.setBrokerIP1(Constants.LOCALHOST_STR);
        brokerConfig.setNamesrvAddr(namesrvAddr);
        brokerConfig.setEnablePropertyFilter(true);
        String directory = createTempDir();
        storeConfig.setStorePathRootDir(directory);
        storeConfig.setStorePathCommitLog(directory + File.separator + Constants.COMMIT_LOG_DIR_PREFIX);
        storeConfig.setMappedFileSizeCommitLog(Constants.COMMIT_LOG_SIZE);
        storeConfig.setMaxIndexNum(Constants.INDEX_NUM);
        storeConfig.setMaxHashSlotNum(Constants.INDEX_NUM * 4);
        NettyServerConfig nettyServerConfig = new NettyServerConfig();
        NettyClientConfig nettyClientConfig = new NettyClientConfig();
        int port = PortUtils.findBrokerPort();
        nettyServerConfig.setListenPort(port);
        storeConfig.setHaListenPort(port + 1);
        BrokerController brokerController = new BrokerController(brokerConfig, nettyServerConfig, nettyClientConfig,
            storeConfig);
        brokerController.initialize();
        brokerController.start();
    }

    public static void startNamesrv() throws Exception {
        String baseDir = createTempDir();
        Path kvConfigPath = Paths.get(baseDir, "namesrv", "kvConfig.json");
        Path namesrvPath = Paths.get(baseDir, "namesrv", "namesrv.properties");

        NamesrvConfig namesrvConfig = new NamesrvConfig();
        NettyServerConfig nameServerNettyServerConfig = new NettyServerConfig();

        namesrvConfig.setKvConfigPath(kvConfigPath.toString());
        namesrvConfig.setConfigStorePath(namesrvPath.toString());

        nameServerNettyServerConfig.setListenPort(Constants.NAMESRV_PORT);
        NamesrvController namesrvController =
            new NamesrvController(namesrvConfig, nameServerNettyServerConfig);
        namesrvController.initialize();
        namesrvController.start();
    }

    public static void createTopic(String topic, String nsAddr, String clusterName) {
        createTopic(topic, nsAddr, clusterName, emptyMap());
    }

    public static void createTopic(String topic, String nsAddr, String clusterName, Map<String, String> attributes) {
        try {
            // RocketMQ 4.x
            Class<?> mqAdmin = Class.forName("org.apache.rocketmq.test.util.MQAdmin");
            Method createTopic =
                mqAdmin.getMethod("createTopic", String.class, String.class, String.class, int.class);
            createTopic.invoke(null, nsAddr, clusterName, topic, 20);
        } catch (ClassNotFoundException | InvocationTargetException | NoSuchMethodException |
                 IllegalAccessException e) {

            // RocketMQ 5.x
            try {
                Class<?> mqAdmin = Class.forName("org.apache.rocketmq.test.util.MQAdminTestUtils");
                Method createTopic =
                    mqAdmin.getMethod(
                        "createTopic", String.class, String.class, String.class, int.class, Map.class);
                createTopic.invoke(null, nsAddr, clusterName, topic, 20, attributes);
            } catch (ClassNotFoundException
                     | InvocationTargetException
                     | NoSuchMethodException
                     | IllegalAccessException ex) {
                throw new LinkageError("Could not initialize topic", ex);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));
        startNamesrv();

        // 创建一个线程池，其核心线程数等于 2 倍 CPU 数
        ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 4);

        char clusterSuffix = 'a';
        for (int i = 0; i < Constants.CLUSTER_NUM; i++) {
            String clusterName = Constants.CLUSTER_NAME_PREFIX + clusterSuffix;
            for (int j = 0; j < Constants.BROKERS_PER_CLUSTER; j++) {
                String brokerName = clusterName + Constants.BROKER_PREFIX + j; // 在 broker 的名称中包含集群的名称
                startBroker(Constants.LOCALHOST_STR + Constants.IP_PORT_SEPARATOR + Constants.NAMESRV_PORT, clusterName, brokerName);
            }
            Thread.sleep(8000);
            for (int k = 0; k < Constants.TOPICS_PER_BROKER; k++) {
                String topicName = clusterName + Constants.TOPIC_PREFIX + k; // 在 topic 的名称中包含集群的名称和 broker 的名称
                // 将创建 topic 的任务提交到线程池中
                executorService.submit(() -> {
                    try {
                        createTopic(topicName, Constants.LOCALHOST_STR + Constants.IP_PORT_SEPARATOR + Constants.NAMESRV_PORT, clusterName);
                    } catch (Exception e) {
                        logger.error("Failed to create topic {}", topicName, e);
                    }
                });
            }
            clusterSuffix++;
        }
        Producers.start();
        Consumers.start();
    }

    public static String getRandomTopicName() {
        int clusterIndex = RandomUtils.nextInt(0, Constants.CLUSTER_NUM);
        int topicIndex = RandomUtils.nextInt(0, Constants.TOPICS_PER_BROKER);
        return Constants.CLUSTER_NAME_PREFIX + (char) ('a' + clusterIndex) + Constants.TOPIC_PREFIX + topicIndex;
    }
}
