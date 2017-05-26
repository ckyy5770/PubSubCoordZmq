package edu.vanderbilt.chuilian.util;

import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

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
    // unique pub ID assigned by zookeeper
    String pubID;

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
        {
            //debug
            System.out.println("new sender object created: " + topic);
        }
    }

    public void start() throws Exception {
        // connect to the receiver address
        this.sendSocket.connect("tcp://" + this.address);
        // register message buffer for this topic
        this.msgBuffer = this.msgBufferMap.register(this.topic);
        if (this.msgBuffer == null) {
            throw new IllegalStateException("message buffer with the topic name " + this.topic + " already exist!");
        }
        // register this publisher to zookeeper
        this.pubID = this.zkConnect.registerPub(this.topic, this.ip);
        // execute sender thread for this topic
        this.future = executor.submit(() -> {
            {
                //debug
                System.out.println("new sender thread created: " + topic);
            }
            while (true) {
                // checking message buffer and send message every 0.1 secs
                Thread.sleep(100);
                this.sender();
            }
        });
    }

    public void stop() throws Exception {
        {
            //debug
            System.out.println("stopping sender: " + topic);
        }
        // stop the sender thread first,
        // otherwise there could be interruption between who ever invoked this method and the sender thread.
        this.future.cancel(false);
        // stop logic should be different than receivers, since here in sender, we should make sure every messages
        // in the old sending buffer are sent before we shut down the sender.
        // unregister the message buffer, the return value is the old buffer, which may have some old message left
        // return them to publisher for properly handling.
        MsgBuffer oldBuffer = this.msgBufferMap.unregister(this.topic);
        // send messages in old buffer
        this.processBuffer(oldBuffer);
        // shutdown zmq socket and context
        this.sendSocket.close();
        this.sendContext.term();
        // unregister itself from zookeeper server
        this.zkConnect.unregisterPub(this.topic, this.pubID);
        {
            //debug
            System.out.println("sender stopped: " + topic);
        }
    }

    public void sender() {
        // checking message buffer and send message.
        // create a new empty buffer for this topic
        MsgBuffer buff = new MsgBuffer(this.topic);
        // swap the new empty buffer with old buffer
        // TODO: 5/24/17 here may need lock
        this.msgBufferMap.get(this.topic).swap(buff);
        // then we process messages in this old buffer
        this.processBuffer(buff);
    }

    protected void processBuffer(MsgBuffer buff) {
        if (buff == null) return;
        Iterator<ZMsg> iter = buff.iterator();
        while (iter.hasNext()) {
            processMsg(iter.next());
        }
    }

    protected void processMsg(ZMsg msg) {
        this.sendSocket.sendMore(new String(msg.getFirst().getData()));
        this.sendSocket.send(new String(msg.getLast().getData()));

        {
            //debug
            System.out.println("Sent Message from sender: (" + this.topic + ")" + " to address: " + this.address);
            System.out.println(new String(msg.getFirst().getData()));
            //System.out.println(DataSampleHelper.deserialize(receivedMsg.getLast().getData()).sampleId());
            System.out.println(new String(msg.getLast().getData()));
        }
    }

    // user should send messages only through this method
    public void send(String message) {
        // wrap the message to ZMsg and push it to the message buffer, waiting to be sent
        ZMsg newMsg = ZMsg.newStringMsg();
        newMsg.addFirst(this.topic.getBytes());
        newMsg.addLast(message.getBytes());
        this.msgBuffer.add(newMsg);
    }
}
