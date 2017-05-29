package edu.vanderbilt.chuilian.util;

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

    public MainChannel(String topic, PortList portList, ExecutorService executor, ZkConnect zkConnect, ChannelMap channelMap) {
        super(topic, portList, executor, zkConnect, channelMap);
        // channel map is used to create new channel
        this.channelMap = channelMap;
    }

    @Override
    public void start() throws Exception {
        // start listening to receiver port
        recSocket.bind("tcp://*:" + recPort);
        // subscribe topic
        recSocket.subscribe(topic.getBytes());
        // start connecting to sending port
        sendSocket.bind("tcp://*:" + sendPort);
        // register itself to zookeeper service
        zkConnect.registerDefaultChannel(ip + ":" + Integer.toString(recPort), ip + ":" + Integer.toString(sendPort));
        {
            // debug
            System.out.println("Main Channel Started, topic: " + topic);
        }
        // start receiving and sending messages
        workerFuture = executor.submit(() -> {
            {
                // debug
                System.out.println("Main Channel Thread Started, topic: " + topic);
            }
            while (true) {
                worker();
            }
        });
        // main channel won't have a terminator, it should be terminated explicitly by the broker
    }

    @Override
    // main channel will never stop automatically unless being stopped explicitly by broker
    public void stop() throws Exception {
        {
            //debug
            {
                System.out.println("shutting down main channel: ");
            }
            // unregister itself from zookeeper server
            zkConnect.unregisterDefaultChannel();
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
            {
                //debug
                System.out.println("main channel closed: ");
            }
        }
    }

    @Override
    public void worker() throws Exception {
        // just keep receiving and sending messages
        ZMsg receivedMsg = ZMsg.recvMsg(recSocket);
        {
            // debug
            System.out.println("Message Received (From Main Channel) from Port " + recPort);
            System.out.println(new String(receivedMsg.getFirst().getData()));
            //System.out.println(DataSampleHelper.deserialize(receivedMsg.getLast().getData()).sampleId());
            System.out.println(new String(receivedMsg.getLast().getData()));
        }
        String msgTopic = new String(receivedMsg.getFirst().getData());
        // if this topic is new, create a new channel for it
        if (channelMap.get(topic) == null) {
            {
                // debug
                System.out.println("This is a new topic, building a new Channel for it...");
            }
            MsgChannel newChannel = channelMap.register(msgTopic, this.portList, this.executor, this.zkConnect, this.channelMap);
            if (newChannel != null) newChannel.start();
        }
        sendSocket.sendMore(receivedMsg.getFirst().getData());
        sendSocket.send(receivedMsg.getLast().getData());
        {
            // debug
            System.out.println("Message Sent (From Main Channel) to Port " + sendPort);
            System.out.println(new String(receivedMsg.getFirst().getData()));
            //System.out.println(DataSampleHelper.deserialize(receivedMsg.getLast().getData()).sampleId());
            System.out.println(new String(receivedMsg.getLast().getData()));
        }
    }

}
