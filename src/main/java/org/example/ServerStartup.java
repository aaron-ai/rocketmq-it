package org.example;

import java.util.HashMap;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.namesrv.NamesrvController;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class ServerStartup {
    public final String nsAddr;
    public final String broker1Addr;
    final String broker1Name;
    final String clusterName;
    final NamesrvController namesrvController;
    final BrokerController brokerControllerA;
    final BrokerController brokerControllerB;

    public ServerStartup() {
        System.setProperty(
            RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));
        namesrvController = Utility.createAndStartNamesrv();
        nsAddr = "localhost:" + namesrvController.getNettyServerConfig().getListenPort();

        brokerControllerA = Utility.createAndStartBrokerA(nsAddr);
        clusterName = brokerControllerA.getBrokerConfig().getBrokerClusterName();

        broker1Name = brokerControllerA.getBrokerConfig().getBrokerName();
        broker1Addr = "localhost:" + brokerControllerA.getNettyServerConfig().getListenPort();

        brokerControllerB = Utility.createAndStartBrokerB(nsAddr);
    }

    public static void main(String[] args) {
        final ServerStartup serverStartup = new ServerStartup();
        Utility.initTopic("normal-topic-0", serverStartup.nsAddr, serverStartup.clusterName);
        Utility.initTopic("normal-topic-1", serverStartup.nsAddr, serverStartup.clusterName);
        Utility.initTopic("normal-topic-2", serverStartup.nsAddr, serverStartup.clusterName);

        final HashMap<String, String> fifoAttr = new HashMap<>();
        Utility.initTopic("fifo-topic-0", serverStartup.nsAddr, serverStartup.clusterName, fifoAttr);
        Utility.initTopic("fifo-topic-1", serverStartup.nsAddr, serverStartup.clusterName, fifoAttr);
        Utility.initTopic("fifo-topic-2", serverStartup.nsAddr, serverStartup.clusterName, fifoAttr);

        final HashMap<String, String> delayAttr = new HashMap<>();
        Utility.initTopic("delay-topic-0", serverStartup.nsAddr, serverStartup.clusterName, delayAttr);
        Utility.initTopic("delay-topic-1", serverStartup.nsAddr, serverStartup.clusterName, delayAttr);
        Utility.initTopic("delay-topic-2", serverStartup.nsAddr, serverStartup.clusterName, delayAttr);

        final HashMap<String, String> transactionAttr = new HashMap<>();
        Utility.initTopic("transaction-topic-0", serverStartup.nsAddr, serverStartup.clusterName, transactionAttr);
        Utility.initTopic("transaction-topic-1", serverStartup.nsAddr, serverStartup.clusterName, transactionAttr);
        Utility.initTopic("transaction-topic-2", serverStartup.nsAddr, serverStartup.clusterName, transactionAttr);
    }
}