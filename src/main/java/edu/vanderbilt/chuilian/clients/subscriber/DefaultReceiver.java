package edu.vanderbilt.chuilian.clients.subscriber;

import edu.vanderbilt.chuilian.util.MsgBuffer;
import edu.vanderbilt.chuilian.util.MsgBufferMap;
import edu.vanderbilt.chuilian.util.ZkConnect;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMsg;

import java.util.concurrent.ExecutorService;

/**
 * Created by Killian on 5/24/17.
 */
public class DefaultReceiver extends DataReceiver {
    private static final Logger logger = LogManager.getLogger(DefaultReceiver.class.getName());
    public DefaultReceiver(String address, MsgBufferMap msgBufferMap, ExecutorService executor, ZkConnect zkConnect) {
        super("", address, msgBufferMap, executor, zkConnect);
    }

    @Override
    // default receiver will not register itself to any topics on zookeeper
    public void start() throws Exception {
        // connect to the sender address
        recSocket.connect("tcp://" + address);
        // subscribe topic, by default, the default receiver will subscribe nothing.
        // recSocket.subscribe(topic.getBytes());
        // register message buffer for this topic
        msgBuffer = msgBufferMap.register(topic);
        if (msgBuffer == null) {
            throw new IllegalStateException("message buffer with the topic name " + topic + " already exist!");
        }
        // execute receiver thread for this topic
        future = executor.submit(() -> {
            logger.info("New default receiver thread started.");
            while (true) {
                receiver();
            }
        });
    }

    @Override
    // all message received by default receiver are stored in topic "", may need to change in the future
    public void receiver() {
        ZMsg receivedMsg = ZMsg.recvMsg(recSocket);
        String msgTopic = new String(receivedMsg.getFirst().getData());
        String msgContent = new String(receivedMsg.getLast().getData());
        msgBuffer.add(receivedMsg);
        logger.info("Message Received at Default Receiver. Topic: {} Content: {}", msgTopic, msgContent);
    }

    @Override
    // will not unregister itself from zookeeper server since it never does
    public MsgBuffer stop() throws Exception {
        logger.info("Stopping default receiver.");
        // stop the receiver thread
        future.cancel(false);
        // shutdown zmq socket and context
        recSocket.close();
        recContext.term();
        logger.info("Default receiver stopped.");
        // unregister the message buffer, the return value is the old buffer, which may have some old message left
        // return them to subscriber for properly handling.
        return msgBufferMap.unregister(topic);
    }

    public void subscribe(String topic) throws Exception {
        // subscribe topic
        recSocket.subscribe(topic.getBytes());
        logger.info("Subscribe topic {} at default receiver.", topic);
    }

    public void unsubscribe(String topic) throws Exception {
        // unsubscribe topic
        recSocket.unsubscribe(topic.getBytes());
        logger.info("Unsubscribe topic {} at default receiver.", topic);
    }
}
