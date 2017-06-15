package edu.vanderbilt.chuilian.clients.subscriber;

/**
 * Created by Killian on 5/31/17.
 */

import edu.vanderbilt.chuilian.util.MsgBufferMap;
import edu.vanderbilt.chuilian.util.ZkConnect;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * waiter will periodically check if message channel with given topic is opened on broker.
 */
public class Waiter {
    private String ip;
    private String topic;
    private MsgBufferMap msgBufferMap;
    private ExecutorService waiterExecutor;
    private ExecutorService receiverExecutor;
    private TopicWaiterMap topicWaiterMap;
    private TopicReceiverMap topicReceiverMap;
    private ZkConnect zkConnect;
    private Future<?> future;
    private static final Logger logger = LogManager.getLogger(Waiter.class.getName());

    public Waiter(String topic, MsgBufferMap msgBufferMap, ExecutorService waiterExecutor, ExecutorService receiverExecutor, TopicWaiterMap topicWaiterMap, TopicReceiverMap topicReceiverMap, ZkConnect zkConnect, String ip) {
        this.ip = ip;
        this.topic = topic;
        this.msgBufferMap = msgBufferMap;
        this.waiterExecutor = waiterExecutor;
        this.receiverExecutor = receiverExecutor;
        this.topicWaiterMap = topicWaiterMap;
        this.topicReceiverMap = topicReceiverMap;
        this.zkConnect = zkConnect;
    }

    void start() throws Exception {
        future = waiterExecutor.submit(() -> {
            logger.info("Waiter thread Started, topic: {}", topic);
            String address;
            while (true) {
                try {
                    Thread.sleep(500);
                    address = getAddress(topic);
                    if (address != null) break;
                } catch (Exception e) {
                    topicWaiterMap.unregister(topic);
                    return;
                }
            }
            logger.info("Waiter ({}): newly opened channel detected, creating a receiverFromLB for it", topic);
            // here we successfully get the broker sender address for this topic, create a data receiverFromLB for it.
            DataReceiver newReceiver = topicReceiverMap.register(topic, address, this.msgBufferMap, this.receiverExecutor, this.zkConnect, this.ip);
            try {
                newReceiver.start();
            } catch (Exception e) {
            }
            topicWaiterMap.unregister(topic);
            logger.info("Waiter stopped, topic: {}", topic);
        });
    }

    void stop() throws Exception {
        logger.info("Stopping waiter, topic: {}", topic);
        future.cancel(false);
        // unregister itself from topic-waiter map
        topicWaiterMap.unregister(topic);
        logger.info("Waiter stopped, topic: {}", topic);
    }

    /**
     * get the specific receiverFromLB address from zookeeper server
     *
     * @param topic
     * @return null if can not get it
     */
    private String getAddress(String topic) throws Exception {
        String address = zkConnect.getNodeData("/topics/" + topic + "/sub");
        return address;
    }

}
