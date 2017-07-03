package edu.vanderbilt.chuilian.clients.publisher;

import edu.vanderbilt.chuilian.types.DataSampleHelper;
import edu.vanderbilt.chuilian.util.MsgBuffer;
import edu.vanderbilt.chuilian.util.MsgBufferMap;
import edu.vanderbilt.chuilian.util.UtilMethods;
import edu.vanderbilt.chuilian.util.ZkConnect;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
    String ip = null;
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

    private static final Logger logger = LogManager.getLogger(DataSender.class.getName());

    //default constructor simply do nothing
    protected DataSender() {
    }


    DataSender(String topic, String address, MsgBufferMap msgBufferMap, ExecutorService executor, ZkConnect zkConnect, String ip) {
        this.ip = UtilMethods.getIPaddress();
        this.topic = topic;
        this.address = address;
        this.sendContext = ZMQ.context(1);
        this.sendSocket = sendContext.socket(ZMQ.PUB);
        this.executor = executor;
        this.msgBufferMap = msgBufferMap;
        this.zkConnect = zkConnect;
        logger.info("New sender object created, topic: {} destination: {} ", topic, address);
    }

    public void start() throws Exception {
        // connect to the receiver address
        sendSocket.connect("tcp://" + address);
        // register message buffer for this topic
        msgBuffer = msgBufferMap.register(topic);
        if (msgBuffer == null) {
            throw new IllegalStateException("message buffer with the topic name " + topic + " already exist!");
        }
        // register this publisher to zookeeper
        pubID = zkConnect.registerPub(topic, ip);
        // execute sender thread for this topic
        future = executor.submit(() -> {
            logger.info("New sender thread created, topic: {}", topic);
            int counter = 0;
            double sendInterval = 50.0;
            logger.info("sending messages in Interval: {} ms", sendInterval);
            while (true) {
                try {
                    long sleep_interval=exponentialInterarrival(sendInterval);
                    if(sleep_interval>0){
                        Thread.sleep(sleep_interval);
                    }
                } catch (InterruptedException ix) {
                    logger.error(ix.getMessage(),ix);
                    break;
                }
                senderSimplified(counter++);
            }
            return;
        });
    }

    public static long exponentialInterarrival(double averageInterval){
        return (long)(averageInterval*(-Math.log(Math.random())));
    }


    public void stop() throws Exception {
        logger.info("Stopping sender, topic: {}", topic);
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
        // unregister itself from zookeeper server
        zkConnect.unregisterPub(topic, pubID);
        logger.info("Sender stopped, topic: {}", topic);
    }

    /**
     * simplified version of sender, just making one fake message of this.topic and send it.
     */
    void senderSimplified(int MsgID){
        sendSocket.sendMore(topic);
        sendSocket.send(DataSampleHelper.serialize(MsgID, 1, 1, 0, System.currentTimeMillis(), 10));
    }

    void sender() {
        // checking message buffer and send message.
        // create a new empty buffer for this topic
        MsgBuffer buff = new MsgBuffer(topic);
        // swap the new empty buffer with old buffer
        // TODO: 5/24/17 here may need lock
        msgBufferMap.get(topic).swap(buff);
        // then we process messages in this old buffer
        processBuffer(buff);
    }

    void processBuffer(MsgBuffer buff) {
        if (buff == null) return;
        Iterator<ZMsg> iter = buff.iterator();
        while (iter.hasNext()) {
            processMsg(iter.next());
        }
    }

    void processMsg(ZMsg msg) {
        String msgTopic = new String(msg.getFirst().getData());
        byte[] msgContent = msg.getLast().getData();
        sendSocket.sendMore(msgTopic);
        sendSocket.send(msgContent);
        logger.info("Message Sent from Sender ({}) Topic: {} ID: {}", topic, msgTopic, DataSampleHelper.deserialize(msgContent).sampleId());
    }

    // user should send messages only through this method
    public void send(byte[] message) {
        // wrap the message to ZMsg and push it to the message buffer, waiting to be sent
        ZMsg newMsg = ZMsg.newStringMsg();
        newMsg.addFirst(topic.getBytes());
        newMsg.addLast(message);
        msgBuffer.add(newMsg);
        logger.debug("Message stored at buffer ({}) ID: {}", topic, DataSampleHelper.deserialize(message).sampleId());
    }
}
