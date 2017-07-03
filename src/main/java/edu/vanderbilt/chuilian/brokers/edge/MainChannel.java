package edu.vanderbilt.chuilian.brokers.edge;

import edu.vanderbilt.chuilian.loadbalancer.Dispatcher;
import edu.vanderbilt.chuilian.loadbalancer.LoadAnalyzer;
import edu.vanderbilt.chuilian.types.DataSampleHelper;
import edu.vanderbilt.chuilian.util.PortList;
import edu.vanderbilt.chuilian.util.ZkConnect;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMsg;

import java.util.concurrent.ExecutorService;

/**
 * Created by Killian on 5/23/17.
 */

/**
 * every new topic will be sent to Main channel, then main channel will then create a message channel for it.
 * note main channel will still behave like a normal message channel: sending messages directly to subscribers
 */
public class MainChannel extends MsgChannel {
    private static final Logger logger = LogManager.getLogger(MainChannel.class.getName());

    public MainChannel(String topic, PortList portList, ExecutorService executor, ZkConnect zkConnect, ChannelMap channelMap, Dispatcher dispatcher, LoadAnalyzer loadAnalyzer, String ip) {
        super(topic, portList, executor, zkConnect, channelMap, dispatcher, loadAnalyzer, ip);
        // channel map is used to create new channel
        this.channelMap = channelMap;
    }

    @Override
    public void start() throws Exception {
        // start listening to receiverFromLB port
        recSocket.bind("tcp://*:" + recPort);
        // subscribe topic
        recSocket.subscribe(topic.getBytes());
        // start connecting to sending port
        sendSocket.bind("tcp://*:" + sendPort);
        // register itself to zookeeper service
        zkConnect.registerDefaultChannel(ip + ":" + Integer.toString(recPort), ip + ":" + Integer.toString(sendPort));

        logger.info("Main Channel Started. ip {} recPort {} sendPort {} priority {}", ip, recPort, sendPort, Thread.currentThread().getPriority());

        // start receiving messages
        workerFuture = executor.submit(() -> {
            logger.info("Main Channel Worker Thread Started. ");
            while (true) {
                receiver();
                // main channel don't need a sender.
                //sender();
            }
        });


        // main channel won't have a terminator, it should be terminated explicitly by the broker
    }

    @Override
    // main channel will never stop automatically unless being stopped explicitly by broker
    public void stop() {
        logger.info("Closing main channel.");
        // unregister itself from zookeeper server
        try {
            zkConnect.unregisterDefaultChannel(ip + ":" + Integer.toString(recPort), ip + ":" + Integer.toString(sendPort));
        } catch (Exception e) {
            logger.error("cannot stop the default channel. error message:{}", e.getMessage());
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
        // unregister itself from Channel Map since never registered
        channelMap.setMain(null);
        logger.info("Main channel closed.");
    }

    @Override
    public void receiver() {
        // just keep receiving and sending messages
        ZMsg receivedMsg = ZMsg.recvMsg(recSocket);
        String msgTopic = new String(receivedMsg.getFirst().getData());
        byte[] msgContent = receivedMsg.getLast().getData();
        messageQueue.add(receivedMsg);
        logger.info("Message Received at Main Channel: Topic: {} ID: {}", msgTopic, DataSampleHelper.deserialize(msgContent).sampleId());
        // if this topic is new, create a new channel for it
        /*
        if (channelMap.get(msgTopic) == null) {
            logger.info("New topic detected, creating a new channel for it. topic: {}", msgTopic);
            MsgChannel newChannel = channelMap.register(msgTopic, this.portList, this.executor, this.zkConnect, this.channelMap);
            if (newChannel != null) newChannel.start();
        }
        */
        // if this topic is new, report to Load Balancer
        if (dispatcher.getPlan().getChannelMapping().getChannelPlan(msgTopic) == null) {
            logger.info("New topic detected, reporting to load balancer. topic: {}", msgTopic);
            dispatcher.registerChannelToLB(msgTopic);
        }
    }

    @Override
    public void sender() throws Exception {
        // get a message from messageQueue
        ZMsg sendingMsg = messageQueue.getNextMsg();
        if (sendingMsg == null) return;
        String msgTopic = new String(sendingMsg.getFirst().getData());
        byte[] msgContent = sendingMsg.getLast().getData();
        sendSocket.sendMore(msgTopic);
        sendSocket.send(msgContent);
        logger.info("Message Sent from Main Channel: Topic: {} ID: {}", topic, msgTopic, DataSampleHelper.deserialize(msgContent).sampleId());
    }


    // a method that is used for creating a new channel. Should only be invoked by dispatcher
    public void createChannel(String topic) {
        if (topic != null) {
            logger.info("Creating a new channel for topic: {}", topic);
            MsgChannel newChannel = channelMap.register(topic, this.portList, this.executor, this.zkConnect, this.channelMap, this.dispatcher, this.loadAnalyzer, this.ip);
            if (newChannel != null) {
                try {
                    newChannel.start();
                } catch (Exception e) {
                    logger.error("can not create start the new channel: {}, error message: {}", topic, e.getMessage());
                }
                logger.info("Channel created. topic: {}", topic);
            }
        }
    }
}

