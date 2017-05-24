package edu.vanderbilt.chuilian.util;

import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Created by Killian on 5/24/17.
 */
public class DataReceiver {
    String topic;
    String address;
    MsgBufferMap msgBufferMap;
    MsgBuffer msgBuffer;
    ZMQ.Context recContext;
    ZMQ.Socket recSocket;
    ExecutorService executor;
    // future is a reference of the receiver thread, it can be used to stop the thread.
    Future<?> future;

    //default constructor simply do nothing
    protected DataReceiver() {
    }

    public DataReceiver(String topic, String address, MsgBufferMap msgBufferMap, ExecutorService executor) {
        this.topic = topic;
        this.address = address;
        this.msgBufferMap = msgBufferMap;
        this.recContext = ZMQ.context(1);
        this.recSocket = recContext.socket(ZMQ.SUB);
        this.executor = executor;
    }

    public void start() {
        // connect to the sender address
        this.recSocket.connect("tcp://" + this.address);
        // subscribe topic
        this.recSocket.subscribe(this.topic.getBytes());
        // register message buffer for this topic
        this.msgBuffer = this.msgBufferMap.register(this.topic);
        if (this.msgBuffer == null) {
            throw new IllegalStateException("message buffer with the topic name " + topic + " already exist!");
        }
        // execute receiver thread for this topic
        this.future = executor.submit(() -> {
            while (true) {
                this.receive();
            }
        });
    }

    public MsgBuffer stop() {
        // stop the receiver thread
        this.future.cancel(true);
        // unregister the message buffer, the return value is the old buffer, which may have some old message left
        // return them to subscriber for properly handling.
        return this.msgBufferMap.unregister(this.topic);
    }

    public void receive() {
        ZMsg receivedMsg = ZMsg.recvMsg(this.recSocket);
        msgBuffer.add(receivedMsg);
        {
            //debug
            System.out.println("Message Received:");
            System.out.println(new String(receivedMsg.getFirst().getData()));
            //System.out.println(DataSampleHelper.deserialize(receivedMsg.getLast().getData()).sampleId());
            System.out.println(new String(receivedMsg.getLast().getData()));
        }
    }
}