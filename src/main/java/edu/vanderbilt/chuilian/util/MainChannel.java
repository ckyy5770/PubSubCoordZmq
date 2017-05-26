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
        this.recSocket.bind("tcp://*:" + this.recPort);
        // subscribe topic
        this.recSocket.subscribe(topic.getBytes());
        // start connecting to sending port
        this.sendSocket.bind("tcp://*:" + this.sendPort);
        // register itself to zookeeper service
        this.zkConnect.registerDefaultChannel(this.ip + ":" + Integer.toString(this.recPort), this.ip + ":" + Integer.toString(this.sendPort));
        {
            // debug
            System.out.println("Main Channel Started, topic: " + this.topic);
        }
        // start receiving and sending messages
        this.workerFuture = this.executor.submit(() -> {
            {
                // debug
                System.out.println("Main Channel Thread Started, topic: " + this.topic);
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
            this.zkConnect.unregisterDefaultChannel();
            // stop worker thread
            this.workerFuture.cancel(false);
            // shutdown zmq socket and context
            this.recSocket.close();
            this.recContext.term();
            this.sendSocket.close();
            this.sendContext.term();
            // return used port to port list
            this.portList.put(this.recPort);
            this.portList.put(this.sendPort);
            // unregister itself from Channel Map since never registered
            this.channelMap.setMain(null);
            {
                //debug
                System.out.println("main channel closed: ");
            }
        }
    }

    @Override
    public void worker() throws Exception {
        // just keep receiving and sending messages
        ZMsg receivedMsg = ZMsg.recvMsg(this.recSocket);
        {
            // debug
            System.out.println("Message Received (From Main Channel) from Port " + this.recPort);
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
        this.sendSocket.sendMore(receivedMsg.getFirst().getData());
        this.sendSocket.send(receivedMsg.getLast().getData());
        {
            // debug
            System.out.println("Message Sent (From Main Channel) to Port " + this.sendPort);
            System.out.println(new String(receivedMsg.getFirst().getData()));
            //System.out.println(DataSampleHelper.deserialize(receivedMsg.getLast().getData()).sampleId());
            System.out.println(new String(receivedMsg.getLast().getData()));
        }
    }

}
