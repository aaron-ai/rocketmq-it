package org.example.clients;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.example.Constants;
import org.example.ServerStartup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Consumers {
    public static final Logger logger = LoggerFactory.getLogger(Consumers.class);
    // Limit message max cache size in MiB to prevent OOM.
    public static final int CONSUMER_QUEUE_MESSAGE_MAX_CACHE_SIZE_IN_MIB = 8;
    // Limit message max cache count to prevent OOM.
    private static final int CONSUMER_QUEUE_MESSAGE_MAX_CACHE_COUNT = 128;
    // Limit message consumption thread num to save resources.
    private static final int CONSUME_THREAD_NUM = 2;

    private static void startAll() {
        for (int i = 0; i < Constants.CONSUMER_NUM; i++) {
            try {
                DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("test-consumer-group-" + i);
                consumer.setNamesrvAddr(Constants.LOCALHOST_STR + Constants.IP_PORT_SEPARATOR + Constants.NAMESRV_PORT);
                consumer.setPullThresholdSizeForQueue(CONSUMER_QUEUE_MESSAGE_MAX_CACHE_SIZE_IN_MIB);
                consumer.setPullThresholdForQueue(CONSUMER_QUEUE_MESSAGE_MAX_CACHE_COUNT);
                consumer.setConsumeThreadMin(CONSUME_THREAD_NUM);
                consumer.setConsumeThreadMax(CONSUME_THREAD_NUM);
                consumer.setInstanceName("Instance-" + System.nanoTime());
                consumer.subscribe(ServerStartup.getRandomTopicName(), "*");
                consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> ConsumeConcurrentlyStatus.CONSUME_SUCCESS);
                consumer.start();
            } catch (MQClientException e) {
                logger.error("Failed to start consumer", e);
            }
        }
    }

    public static void start() {
        startAll();
    }
}
