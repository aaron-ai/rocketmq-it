package org.example;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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

public class ServerStartup {
    public static final InternalLogger logger = InternalLoggerFactory.getLogger(ServerStartup.class);

    private static final String COMMIT_LOG_DIR_PREFIX = "commitlog";
    private static final String LOCALHOST_STR = "127.0.0.1";
    private static final String IP_PORT_SEPARATOR = ":";

    private static final int COMMIT_LOG_SIZE = 1024 * 1024 * 100;
    private static final int INDEX_NUM = 1000;
    private static final int NAMESRV_PORT = 9876;

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
        brokerConfig.setBrokerIP1(LOCALHOST_STR);
        brokerConfig.setNamesrvAddr(namesrvAddr);
        brokerConfig.setEnablePropertyFilter(true);
        String directory = createTempDir();
        storeConfig.setStorePathRootDir(directory);
        storeConfig.setStorePathCommitLog(directory + File.separator + COMMIT_LOG_DIR_PREFIX);
        storeConfig.setMappedFileSizeCommitLog(COMMIT_LOG_SIZE);
        storeConfig.setMaxIndexNum(INDEX_NUM);
        storeConfig.setMaxHashSlotNum(INDEX_NUM * 4);
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

        nameServerNettyServerConfig.setListenPort(NAMESRV_PORT);
        NamesrvController namesrvController =
            new NamesrvController(namesrvConfig, nameServerNettyServerConfig);
        namesrvController.initialize();
        namesrvController.start();
    }

    public static void main(String[] args) throws Exception {
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));
        startNamesrv();

        int numClusters = 3; // 定义集群的数量
        int numBrokersPerCluster = 2; // 定义每个集群中 broker 的数量

        char clusterSuffix = 'a';
        for (int i = 0; i < numClusters; i++) {
            for (int j = 0; j < numBrokersPerCluster; j++) {
                String clusterName = "benchmark-cluster-" + clusterSuffix;
                String brokerName = clusterName + "-broker-" + j; // 在 broker 的名称中包含集群的名称
                startBroker(LOCALHOST_STR + IP_PORT_SEPARATOR + NAMESRV_PORT, clusterName, brokerName);
            }
            clusterSuffix++;
        }
    }
}
