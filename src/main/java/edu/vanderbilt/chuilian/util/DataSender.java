package edu.vanderbilt.chuilian.util;

import org.zeromq.ZMQ;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Killian on 5/24/17.
 */

/**
 * one data DataSender can only send messages for one topic.
 */
public class DataSender {
    String topic;
    String address;
    ZMQ.Context sendContext;
    ZMQ.Socket sendSocket;
    ExecutorService executor;

    //default constructor simply do nothing
    protected DataSender() {
    }

    ;

    public DataSender(String topic, String address) {
        this.topic = topic;
        this.address = address;
        this.sendContext = ZMQ.context(1);
        this.sendSocket = sendContext.socket(ZMQ.PUB);
        this.executor = Executors.newFixedThreadPool(2);
    }

    public void start() {
        // connect to the receiver address
        this.sendSocket.connect("tcp://" + this.address);
    }

    public void send(String message) {
        this.sendSocket.sendMore(this.topic);
        this.sendSocket.send(message);
    }
}
