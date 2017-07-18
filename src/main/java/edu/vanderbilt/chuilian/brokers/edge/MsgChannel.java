package edu.vanderbilt.chuilian.brokers.edge;

import edu.vanderbilt.chuilian.loadbalancer.*;
import edu.vanderbilt.chuilian.types.DataSampleHelper;
import edu.vanderbilt.chuilian.util.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.util.HashMap;
import java.util.concurrent.*;

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
    // a message queue for storing message
    MessageQueue messageQueue = new MessageQueue();
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
    Future<?> senderFuture;
    // terminator will periodically check if there is still a publisher or subscriber alive on this topic
    Future<?> terminatorFuture;
    // broker dispatcher
    Dispatcher dispatcher;
    LoadAnalyzer loadAnalyzer;
    int priority;

    private static final Logger logger = LogManager.getLogger(MsgChannel.class.getName());

    //default constructor simply do nothing
    protected MsgChannel() {
    }

    /**
     * @param topic
     * @param portList available port list, there should be only one port list within a broker
     * @param executor executor for worker threads, there should be only one executor within a broker
     */
    public MsgChannel(String topic, PortList portList, ExecutorService executor, ZkConnect zkConnect, ChannelMap channelMap, Dispatcher dispatcher, LoadAnalyzer loadAnalyzer, String ip) {
        // priority settings
        HashMap<String, Integer> priorityMap = new HashMap<>();
        for(int i=0; i<50; i++){
            priorityMap.put(Integer.toString(i), 50-i);
        }
        if(!priorityMap.containsKey(topic)){
            this.priority = 0;
        }else{
            this.priority = priorityMap.get(topic);
        }
        // initialize member vars
        this.ip = ip;
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
        // broker dispatcher
        this.dispatcher = dispatcher;
        this.loadAnalyzer = loadAnalyzer;
    }

    /**
     * start the channel: activate sender socket and receiverFromLB socket
     * keep receiving and sending messages.
     */
    public void start() throws Exception {
        // start listening to receiverFromLB port
        recSocket.bind("tcp://*:" + recPort);
        // subscribe topic
        recSocket.subscribe(topic.getBytes());
        // start connecting to sending port
        sendSocket.bind("tcp://*:" + sendPort);
        // register itself to zookeeper service
        zkConnect.registerChannel(topic, ip + ":" + Integer.toString(recPort), ip + ":" + Integer.toString(sendPort));

        logger.info("Channel Started. topic: {} ip: {} recPort: {} sendPort: {}", topic, ip, recPort, sendPort);

        // begin receiving messages
        PriorityWorker pWorker = new PriorityWorker(this.priority);
        workerFuture = executor.submit(pWorker);



        //

        /* msg channel do not have a terminator anymore
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
        */
    }

    public void stop() {
        logger.info("Closing channel. topic: {}", topic);
        // unregister itself from zookeeper server
        try {
            zkConnect.unregisterChannel(topic, ip + ":" + Integer.toString(recPort), ip + ":" + Integer.toString(sendPort));
        } catch (Exception e) {
            logger.error("can not unregister this channel from zookeeper server: {}, error message: {}", topic, e.getMessage());
        }
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

    public void worker(){
        // receive one msg
        ZMsg receivedMsg = ZMsg.recvMsg(recSocket);
        String msgTopic = new String(receivedMsg.getFirst().getData());
        byte[] msgContent = receivedMsg.getLast().getData();

        // load balancer module: update metrics
        BrokerReport br = loadAnalyzer.getBrokerReport();
        br.updateBytes(msgTopic, msgContent.length);

        sendSocket.sendMore(msgTopic);
        sendSocket.send(msgContent);

        // load balancer module: update metrics
        br.updateBytes(msgTopic, msgContent.length);
        br.updateMsgs(msgTopic, 1);
        br.updatePublications(msgTopic, 1);
    }


    public void receiver() {
        // receive one msg
        ZMsg receivedMsg = ZMsg.recvMsg(recSocket);
        String msgTopic = new String(receivedMsg.getFirst().getData());
        byte[] msgContent = receivedMsg.getLast().getData();
        messageQueue.add(receivedMsg);
        // load balancer module: update metrics
        loadAnalyzer.getBrokerReport().updateBytes(msgTopic, msgContent.length);
        //logger.info("Message Received at Channel ({}) Topic: {} ID: {}", topic, msgTopic, DataSampleHelper.deserialize(msgContent).sampleId());
    }

    public void sender() throws Exception {
        // get a message from messageQueue
        ZMsg sendingMsg = messageQueue.getNextMsg();
        if (sendingMsg == null) return;
        String msgTopic = new String(sendingMsg.getFirst().getData());
        byte[] msgContent = sendingMsg.getLast().getData();
        sendSocket.sendMore(msgTopic);
        sendSocket.send(msgContent);

        // load balancer module: update metrics
        BrokerReport br = loadAnalyzer.getBrokerReport();
        br.updateBytes(msgTopic, msgContent.length);
        br.updateMsgs(msgTopic, 1);
        br.updatePublications(msgTopic, 1);
        //logger.info("Message Sent from Channel ({}) Topic: {} ID: {}", topic, msgTopic, DataSampleHelper.deserialize(msgContent).sampleId());
    }

    class PriorityWorker implements PriorityRunnable {
        private int priority;

        public PriorityWorker(int priority) {
            this.priority = priority;
        }

        public void run() {
            //Thread.currentThread().setPriority(this.priority);
            logger.info("Channel Worker Thread Started. topic: {}, priority: {}", topic, priority);
            while (true) {
                worker();
                //receiver();
                //sender();
            }
        }

        public int getPriority() {
            return priority;
        }
    }

    class PriorityReceiver implements PriorityRunnable {
        private int priority;

        public PriorityReceiver(int priority) {
            this.priority = priority;
        }

        public void run() {
            logger.info("Channel Receiver Thread Started. topic: {}, priority: {}", topic, priority);
            while (true) {
                receiver();
                //sender();
            }
        }

        public int getPriority() {
            return priority;
        }
    }

    class PrioritySender implements PriorityRunnable {
        private int priority;

        public PrioritySender(int priority) {
            this.priority = priority;
        }

        public void run() {
            logger.info("Channel Sender Thread Started. topic: {}, priority: {}", topic, priority);
            while (true) {
                //receiver();
                try {
                    sender();
                }catch (Exception e){

                }
            }
        }

        public int getPriority() {
            return priority;
        }
    }



}
