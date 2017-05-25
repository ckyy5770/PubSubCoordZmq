package edu.vanderbilt.chuilian.util;

import java.util.concurrent.ExecutorService;

/**
 * Created by Killian on 5/24/17.
 */
public class DefaultSender extends DataSender {
    public DefaultSender(String address, MsgBufferMap msgBufferMap, ExecutorService executor, ZkConnect zkConnect) {
        super("", address, msgBufferMap, executor, zkConnect);
    }

    public void send(String topic, String message) {
        this.sendSocket.sendMore(topic);
        this.sendSocket.send(message);
    }

    public void send(String message) {
        throw new UnsupportedOperationException("you must specify message topic when sending message through default data sender!");
    }
}
