package edu.vanderbilt.chuilian.util;

import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Created by Killian on 5/24/17.
 */

/**
 * one data DataSender can only send messages for one topic.
 */
public class DataSender {
    // TODO: 5/25/17 hard coded ip
    String ip = "127.0.0.1";
    String topic;
    String address;
    ZMQ.Context sendContext;
    ZMQ.Socket sendSocket;
    MsgBufferMap msgBufferMap;
    MsgBuffer msgBuffer;
    ExecutorService executor;
    // future is a reference of the receiver thread, it can be used to stop the thread.
    Future<?> future;
    // zookeeper client
    ZkConnect zkConnect;

    //default constructor simply do nothing
    protected DataSender() {
    }


    public DataSender(String topic, String address, MsgBufferMap msgBufferMap, ExecutorService executor, ZkConnect zkConnect) {
        this.topic = topic;
        this.address = address;
        this.sendContext = ZMQ.context(1);
        this.sendSocket = sendContext.socket(ZMQ.PUB);
        this.executor = executor;
        this.msgBufferMap = msgBufferMap;
        this.zkConnect = zkConnect;
    }

    public void start() throws Exception {
        // connect to the receiver address
        this.sendSocket.connect("tcp://" + this.address);
        // register message buffer for this topic
        this.msgBuffer = this.msgBufferMap.register(this.topic);
        if (this.msgBuffer == null) {
            throw new IllegalStateException("message buffer with the topic name " + this.topic + " already exist!");
        }
        // register this subscriber to zookeeper
        this.zkConnect.registerSub(this.topic, this.ip);
        // execute sender thread for this topic
        this.future = executor.submit(() -> {
            while (true) {
                this.sender();
            }
        });
    }

    public void sender() {
        // keep checking message buffer and send message.
        while (true) {
            try {
                TimeUnit.SECONDS.sleep(2);
            } catch (InterruptedException e) {
                throw new IllegalStateException("I'm sleeping! Why interrupt me?!", e);
            }
            // create a new empty buffer for this topic
            MsgBuffer buff = new MsgBuffer(this.topic);
            // swap the new empty buffer with old buffer
            // TODO: 5/24/17 here may need lock
            this.msgBufferMap.get(this.topic).swap(buff);
            // then we process messages in this old buffer
            this.processBuffer(buff);
        }
    }

    private void processBuffer(MsgBuffer buff) {
        Iterator<ZMsg> iter = buff.iterator();
        while (iter.hasNext()) {
            processMsg(iter.next());
        }
    }

    private void processMsg(ZMsg msg) {
        this.sendSocket.sendMore(new String(msg.getFirst().getData()));
        this.sendSocket.send(new String(msg.getLast().getData()));

        {
            //debug
            System.out.println("Sent Message:");
            System.out.println(new String(msg.getFirst().getData()));
            //System.out.println(DataSampleHelper.deserialize(receivedMsg.getLast().getData()).sampleId());
            System.out.println(new String(msg.getLast().getData()));
        }
    }

    public void send(String message) {
        // wrap the message to ZMsg and push it to the message buffer, waiting to be sent
        ZMsg newMsg = ZMsg.newStringMsg();
        newMsg.addFirst(this.topic.getBytes());
        newMsg.addLast(message.getBytes());
        this.msgBuffer.add(newMsg);
    }
}
