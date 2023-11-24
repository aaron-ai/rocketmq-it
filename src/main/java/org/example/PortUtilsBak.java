package org.example;

public class PortUtilsBak {
    private PortUtilsBak() {
    }

    public static int getBrokerAPort() {
        String port = System.getenv("rocketmq.broker-a.port");
        if (null == port) {
            port = "10911";
        }
        return Integer.parseInt(port);
    }

    public static int getBrokerBPort() {
        String port = System.getenv("rocketmq.broker-b.port");
        if (null == port) {
            port = "11911";
        }
        return Integer.parseInt(port);
    }

    public static int getBrokerHAAServicePort() {
        String port = System.getenv("rocketmq.broker-a.ha.port");
        if (null == port) {
            port = "10912";
        }
        return Integer.parseInt(port);
    }

    public static int getBrokerHABServicePort() {
        String port = System.getenv("rocketmq.broker-b.ha.port");
        if (null == port) {
            port = "11912";
        }
        return Integer.parseInt(port);
    }

    public static int getNamesrvPort() {
        String port = System.getenv("rocketmq.namesrv.port");
        if (null == port) {
            port = "9876";
        }
        return Integer.parseInt(port);
    }
}
