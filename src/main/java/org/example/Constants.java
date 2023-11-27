package org.example;

public class Constants {
    private Constants() {
    }

    public static final int CLUSTER_NUM = 3;
    public static final int BROKERS_PER_CLUSTER = 2;
    public static final int TOPICS_PER_BROKER = 500;

    public static final int MESSAGE_SEND_PER_SECOND = 1000;

    public static final int PRODUCER_NUM = 8;
    public static final int CONSUMER_NUM = 2;
    public static final String CLUSTER_NAME_PREFIX = "benchmark-cluster-";
    public static final String BROKER_PREFIX = "-broker-";
    public static final String TOPIC_PREFIX = "-topic-";
    public static final String COMMIT_LOG_DIR_PREFIX = "commitlog";
    public static final String LOCALHOST_STR = "127.0.0.1";
    public static final String IP_PORT_SEPARATOR = ":";
    public static final int NAMESRV_PORT = 9876;
    public static final int COMMIT_LOG_SIZE = 1024 * 1024 * 100;
    public static final int INDEX_NUM = 1000;
}
