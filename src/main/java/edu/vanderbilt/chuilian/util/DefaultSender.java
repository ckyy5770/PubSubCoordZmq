package edu.vanderbilt.chuilian.util;

import org.zeromq.ZMsg;

import java.util.concurrent.ExecutorService;

/**
 * Created by Killian on 5/24/17.
 */
public class DefaultSender extends DataSender {
    public DefaultSender(String address, MsgBufferMap msgBufferMap, ExecutorService executor, ZkConnect zkConnect) {
        super("", address, msgBufferMap, executor, zkConnect);
    }

    @Override
    // default sender will not register itself to any topics on zookeeper
    public void start() throws Exception {
        // connect to the receiver address
        this.sendSocket.connect("tcp://" + this.address);
        // register message buffer for this topic
        this.msgBuffer = this.msgBufferMap.register(this.topic);
        if (this.msgBuffer == null) {
            throw new IllegalStateException("message buffer with the topic name " + this.topic + " already exist!");
        }
        // execute sender thread for this topic
        this.future = executor.submit(() -> {
            while (true) {
                this.sender();
            }
        });
    }

    // user should send messages only through this method
    public void send(String topic, String message) {
        // wrap the message to ZMsg and push it to the message buffer, waiting to be sent
        ZMsg newMsg = ZMsg.newStringMsg();
        newMsg.addFirst(topic.getBytes());
        newMsg.addLast(message.getBytes());
        this.msgBuffer.add(newMsg);
    }

    @Override
    public void send(String message) {
        throw new UnsupportedOperationException("you must specify message topic when sending message through default data sender!");
    }
}
