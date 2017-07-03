package edu.vanderbilt.chuilian.clients.subscriber;

import edu.vanderbilt.chuilian.types.DataSample;
import edu.vanderbilt.chuilian.types.DataSampleHelper;
import edu.vanderbilt.chuilian.util.MsgBuffer;
import edu.vanderbilt.chuilian.util.MsgBufferMap;
import edu.vanderbilt.chuilian.util.ZkConnect;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
    // future is a reference of the receiverFromLB thread, it can be used to stop the thread.
    Future<?> future;
    // zookeeper client
    ZkConnect zkConnect;
    // unique sub ID assigned by zookeeper
    String subID;

    private static final Logger logger = LogManager.getLogger(DataReceiver.class.getName());
    private static final Logger loggerTestResult = LogManager.getLogger("TestResult");

    //default constructor simply do nothing
    protected DataReceiver() {
    }

    DataReceiver(String topic, String address, MsgBufferMap msgBufferMap, ExecutorService executor, ZkConnect zkConnect, String ip) {
        this.ip = ip;
        this.topic = topic;
        this.address = address;
        this.msgBufferMap = msgBufferMap;
        this.recContext = ZMQ.context(1);
        this.recSocket = recContext.socket(ZMQ.SUB);
        this.executor = executor;
        this.zkConnect = zkConnect;
        this.subID = null;
        logger.info("New receiver object created, topic: {} source: {}", topic, address);
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
        // execute receiverFromLB thread for this topic
        future = executor.submit(() -> {
            logger.info("New receiver thread started, topic: {}", topic);
            while (true) {
                this.receiver();
            }
        });
    }

    public MsgBuffer stop() throws Exception {
        logger.info("Stopping receiverFromLB, topic: {}", topic);
        // unregister itself from zookeeper server
        zkConnect.unregisterSub(topic, subID);
        // stop the receiverFromLB thread
        future.cancel(false);
        // shutdown zmq socket and context
        recSocket.close();
        recContext.term();
        logger.info("Receiver stopped, topic: {}", topic);
        // unregister the message buffer, the return value is the old buffer, which may have some old message left
        // return them to subscriber for properly handling.
        return msgBufferMap.unregister(topic);
    }

    public void receiver() {
        ZMsg receivedMsg = ZMsg.recvMsg(recSocket);
        String msgTopic = new String(receivedMsg.getFirst().getData());
        byte[] msgContent = receivedMsg.getLast().getData();
        DataSample data = DataSampleHelper.deserialize(msgContent);
        calculateLatencyAndSave(data);
        logger.info("Message Received at Receiver ({}) Topic: {} ID: {}", topic, msgTopic, DataSampleHelper.deserialize(msgContent).sampleId());
        msgBuffer.add(receivedMsg);
    }

    public void calculateLatencyAndSave(DataSample data){
        loggerTestResult.info("{}, {}", data.sampleId(), (System.currentTimeMillis() - data.tsMilisec()));
    }
}