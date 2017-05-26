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

    public DataReceiver(String topic, String address, MsgBufferMap msgBufferMap, ExecutorService executor, ZkConnect zkConnect) {
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
        this.recSocket.connect("tcp://" + this.address);
        // subscribe topic
        this.recSocket.subscribe(this.topic.getBytes());
        // register message buffer for this topic
        this.msgBuffer = this.msgBufferMap.register(this.topic);
        if (this.msgBuffer == null) {
            throw new IllegalStateException("message buffer with the topic name " + this.topic + " already exist!");
        }
        // register this subscriber to zookeeper
        this.subID = this.zkConnect.registerSub(this.topic, this.ip);
        // execute receiver thread for this topic
        this.future = executor.submit(() -> {
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
        this.zkConnect.unregisterSub(this.topic, this.subID);
        // stop the receiver thread
        this.future.cancel(false);
        // shutdown zmq socket and context
        this.recSocket.close();
        this.recContext.term();
        // shutdown zookeeper connection
        this.zkConnect.close();
        {
            //debug
            System.out.println("receiver stopped: " + topic);
        }
        // unregister the message buffer, the return value is the old buffer, which may have some old message left
        // return them to subscriber for properly handling.
        return this.msgBufferMap.unregister(this.topic);
    }

    public void receiver() {
        ZMsg receivedMsg = ZMsg.recvMsg(this.recSocket);
        this.msgBuffer.add(receivedMsg);
        {
            //debug
            System.out.println("Message Received:");
            System.out.println(new String(receivedMsg.getFirst().getData()));
            //System.out.println(DataSampleHelper.deserialize(receivedMsg.getLast().getData()).sampleId());
            System.out.println(new String(receivedMsg.getLast().getData()));
        }
    }
}