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
        sendSocket.connect("tcp://" + address);
        // register message buffer for this topic
        msgBuffer = msgBufferMap.register(topic);
        if (msgBuffer == null) {
            throw new IllegalStateException("message buffer with the topic name " + topic + " already exist!");
        }
        // execute sender thread for this topic
        future = executor.submit(() -> {
            while (true) {
                sender();
            }
        });
    }

    @Override
    public void stop() throws Exception {
        {
            //debug
            System.out.println("stopping default sender: " + topic);
        }
        // stop the sender thread first,
        // otherwise there could be interruption between who ever invoked this method and the sender thread.
        future.cancel(false);
        // stop logic should be different than receivers, since here in sender, we should make sure every messages
        // in the old sending buffer are sent before we shut down the sender.
        // unregister the message buffer, the return value is the old buffer, which may have some old message left
        // return them to publisher for properly handling.
        MsgBuffer oldBuffer = msgBufferMap.unregister(topic);
        // send messages in old buffer
        processBuffer(oldBuffer);
        // shutdown zmq socket and context
        sendSocket.close();
        sendContext.term();
        // default sender will not unregister itself from zookeeper since it never registered
        {
            //debug
            System.out.println("default sender stopped: " + topic);
        }
    }

    // user should send messages only through this method
    public void send(String topic, String message) {
        // wrap the message to ZMsg and push it to the message buffer, waiting to be sent
        ZMsg newMsg = ZMsg.newStringMsg();
        newMsg.addFirst(topic.getBytes());
        newMsg.addLast(message.getBytes());
        msgBuffer.add(newMsg);
    }

    @Override
    public void send(String message) {
        throw new UnsupportedOperationException("you must specify message topic when sending message through default data sender!");
    }
}
