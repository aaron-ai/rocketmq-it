package org.example.clients;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.SettableFuture;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.RandomUtils;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.example.Constants;
import org.example.ServerStartup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("UnstableApiUsage")
public class Producers {
    public static final Logger logger = LoggerFactory.getLogger(Producers.class);
    private static final ThreadPoolExecutor EXECUTOR = new ThreadPoolExecutor(2, 2,
        10, TimeUnit.MINUTES, new LinkedBlockingQueue<>());

    private static final List<DefaultMQProducer> PRODUCER_LIST = new ArrayList<>();

    public static void startAll() {
        for (int i = 0; i < Constants.PRODUCER_NUM; i++) {
            long nanoTime = System.nanoTime();
            DefaultMQProducer producer = new DefaultMQProducer("test-producer-group-" + nanoTime);
            producer.setNamesrvAddr(Constants.LOCALHOST_STR + Constants.IP_PORT_SEPARATOR + Constants.NAMESRV_PORT);
            producer.setInstanceName("Instance-" + nanoTime);
            try {
                producer.start();
            } catch (MQClientException e) {
                logger.error("Failed to start producer", e);
            }
            PRODUCER_LIST.add(producer);
        }
    }

    public static DefaultMQProducer selectProducer() {
        int index = RandomUtils.nextInt(0, Constants.PRODUCER_NUM);
        return PRODUCER_LIST.get(index);
    }

    public static void start() {
        startAll();
        RateLimiter rateLimiter = RateLimiter.create(Constants.MESSAGE_SEND_PER_SECOND);
        EXECUTOR.submit((Runnable) () -> {
            while (true) {
                rateLimiter.acquire();
                String topicName = ServerStartup.getRandomTopicName();
                DefaultMQProducer producer = selectProducer();
                sendAsync(producer, new Message(topicName, "test-message".getBytes()));
            }
        });
    }

    public static ListenableFuture<SendResult> sendAsync(DefaultMQProducer producer, Message message) {
        SettableFuture<SendResult> future = SettableFuture.create();
        try {
            producer.send(message, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    future.set(sendResult);
                }

                @Override
                public void onException(Throwable e) {
                    future.setException(e);
                    logger.error("Failed to send message", e);
                }
            });
        } catch (Throwable t) {
            future.setException(t);
        }
        return future;
    }
}
