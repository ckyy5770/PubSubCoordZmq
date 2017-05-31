package edu.vanderbilt.chuilian.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Created by Chuilian on 5/23/17.
 */

/**
 * Each MsgChannel have to be assigned to one and only one topic during initialization.
 * And the MsgChannel will only hold this topic of messages.
 * After started, each MsgChannel will have one worker thread which listening to the specific port and
 * subscribe specific messages. And send them to the specific sender port.
 */
public class MsgChannel {
    // the topic of all messages in this channel.
    String topic;
    // a message queue for storing message, not used for now
    Queue<ZMsg> msgList = null;
    // Channel map
    ChannelMap channelMap;
    // ref to available port list
    PortList portList;
    // socket configs
    // TODO: 5/25/17 hard coded ip for now
    String ip = "127.0.0.1";
    int sendPort;
    ZMQ.Context sendContext;
    ZMQ.Socket sendSocket;
    int recPort;
    ZMQ.Context recContext;
    ZMQ.Socket recSocket;
    // worker threads are all managed by this executor, which is passed from the broker main thread.
    ExecutorService executor;
    // zookeeper service handler, which is passed from the broker main thread.
    ZkConnect zkConnect;
    // worker future is a reference of the worker thread, it can be used to stop the thread.
    Future<?> workerFuture;
    // terminator will periodically check if there is still a publisher or subscriber alive on this topic
    Future<?> terminatorFuture;

    private static final Logger logger = LogManager.getLogger(MsgChannel.class.getName());

    //default constructor simply do nothing
    protected MsgChannel() {
    }

    /**
     * @param topic
     * @param portList available port list, there should be only one port list within a broker
     * @param executor executor for worker threads, there should be only one executor within a broker
     */
    public MsgChannel(String topic, PortList portList, ExecutorService executor, ZkConnect zkConnect, ChannelMap channelMap) {
        // initialize member vars
        this.topic = topic;
        this.portList = portList;
        this.executor = executor;
        this.zkConnect = zkConnect;
        // initialize socket
        this.recContext = ZMQ.context(1);
        this.recSocket = this.recContext.socket(ZMQ.SUB);
        this.sendContext = ZMQ.context(1);
        this.sendSocket = this.sendContext.socket(ZMQ.PUB);
        // get two unused port number from available list
        this.sendPort = portList.get();
        this.recPort = portList.get();
        // channel map
        this.channelMap = channelMap;
    }

    /**
     * start the channel: activate sender socket and receiver socket
     * keep receiving and sending messages.
     */
    public void start() throws Exception {
        // start listening to receiver port
        recSocket.bind("tcp://*:" + recPort);
        // subscribe topic
        recSocket.subscribe(topic.getBytes());
        // start connecting to sending port
        sendSocket.bind("tcp://*:" + sendPort);
        // register itself to zookeeper service
        zkConnect.registerChannel(topic, ip + ":" + Integer.toString(recPort), ip + ":" + Integer.toString(sendPort));

        logger.info("Channel Started. topic: {} ip: {} recPort: {} sendPort: {}", topic, ip, recPort, sendPort);

        // begin receiving and sending messages
        workerFuture = executor.submit(() -> {
            logger.info("Channel Worker Thread Started. topic: {} ", topic);
            while (true) {
                worker();
            }
        });
        // terminator will periodically check if there is still a publisher or subscriber alive on this topic
        terminatorFuture = executor.submit(() -> {
            logger.info("Channel Terminator Thread Started. topic: {} ", topic);
            try {
                while (true) {
                    Thread.sleep(5000);
                    if (!zkConnect.anyPublisher(topic) && !zkConnect.anySubsciber(topic)) {
                        logger.info("Terminator Thread ({}): Detected no pub/sub connected to this channel. Closing channel...", topic);
                        stop();
                        return;
                    }
                }
            } catch (Exception e) {
            }
        });
    }

    public void stop() throws Exception {
        logger.info("Closing channel. topic: {}", topic);
        // unregister itself from zookeeper server
        zkConnect.unregisterChannel(topic);
        // stop worker thread
        workerFuture.cancel(false);
        // shutdown zmq socket and context
        recSocket.close();
        recContext.term();
        sendSocket.close();
        sendContext.term();
        // return used port to port list
        portList.put(recPort);
        portList.put(sendPort);
        // unregister itself from Channel Map
        channelMap.unregister(topic);
        logger.info("Channel Closed. topic: {}", topic);
    }

    public void worker() throws Exception {
        // just keep receiving and sending messages
        ZMsg receivedMsg = ZMsg.recvMsg(recSocket);
        String msgTopic = new String(receivedMsg.getFirst().getData());
        String msgContent = new String(receivedMsg.getLast().getData());
        logger.info("Message Received at Channel ({}) Topic: {} Content: {}", topic, msgTopic, msgContent);
        sendSocket.sendMore(msgTopic);
        sendSocket.send(msgContent);
        logger.info("Message Sent from Channel ({}) Topic: {} Content: {}", topic, msgTopic, msgContent);
    }


    /**
     * add a message to the end of the queue
     *
     * @param msg
     */
    private void add(ZMsg msg) {
        msgList.add(msg);
    }

    /**
     * get and remove the first message from the queue
     *
     * @return the first message, if the queue is empty, return null.
     */
    private ZMsg poll() {
        return msgList.poll();
    }

    /**
     * is empty
     *
     * @return
     */
    protected boolean isEmpty() {
        return msgList.isEmpty();
    }


}
