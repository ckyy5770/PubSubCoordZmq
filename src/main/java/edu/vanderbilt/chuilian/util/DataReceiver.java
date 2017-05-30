package edu.vanderbilt.chuilian.util;

import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Created by Killian on 5/24/17.
 */
public class DataReceiver {
    // TODO: 5/25/17 hard coded ip
    String ip = "127.0.0.1";
    String topic;
    String address;
    MsgBufferMap msgBufferMap;
    MsgBuffer msgBuffer;
    ZMQ.Context recContext;
    ZMQ.Socket recSocket;
    ExecutorService executor;
    // future is a reference of the receiver thread, it can be used to stop the thread.
    Future<?> future;
    // zookeeper client
    ZkConnect zkConnect;
    // unique sub ID assigned by zookeeper
    String subID;

    //default constructor simply do nothing
    protected DataReceiver() {
    }

    DataReceiver(String topic, String address, MsgBufferMap msgBufferMap, ExecutorService executor, ZkConnect zkConnect) {
        this.topic = topic;
        this.address = address;
        this.msgBufferMap = msgBufferMap;
        this.recContext = ZMQ.context(1);
        this.recSocket = recContext.socket(ZMQ.SUB);
        this.executor = executor;
        this.zkConnect = zkConnect;
        this.subID = null;
        {
            //debug
            System.out.println("new receiver object created: " + topic);
        }
    }

    public void start() throws Exception {
        // connect to the sender address
        recSocket.connect("tcp://" + address);
        // subscribe topic
        recSocket.subscribe(topic.getBytes());
        // register message buffer for this topic
        msgBuffer = msgBufferMap.register(topic);
        if (msgBuffer == null) {
            throw new IllegalStateException("message buffer with the topic name " + topic + " already exist!");
        }
        // register this subscriber to zookeeper
        subID = zkConnect.registerSub(topic, ip);
        // execute receiver thread for this topic
        future = executor.submit(() -> {
            {
                //debug
                System.out.println("new receiver thread created: " + topic);
            }
            while (true) {
                this.receiver();
            }
        });
    }

    public MsgBuffer stop() throws Exception {
        {
            //debug
            System.out.println("stopping receiver: " + topic);
        }
        // unregister itself from zookeeper server
        zkConnect.unregisterSub(topic, subID);
        // stop the receiver thread
        future.cancel(false);
        // shutdown zmq socket and context
        recSocket.close();
        recContext.term();
        {
            //debug
            System.out.println("receiver stopped: " + topic);
        }
        // unregister the message buffer, the return value is the old buffer, which may have some old message left
        // return them to subscriber for properly handling.
        return msgBufferMap.unregister(topic);
    }

    public void receiver() {
        ZMsg receivedMsg = ZMsg.recvMsg(recSocket);
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