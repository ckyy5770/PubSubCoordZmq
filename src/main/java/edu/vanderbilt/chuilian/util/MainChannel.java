package edu.vanderbilt.chuilian.util;

import org.zeromq.ZMsg;

import java.nio.channels.Channel;
import java.util.concurrent.ExecutorService;

/**
 * Created by Killian on 5/23/17.
 */

/**
 * every new topic will be sent to Main channel, then main channel will then create a channel for it.
 * main channel will also send messages directly to subscribers, before the new channel starts
 */
public class MainChannel extends MsgChannel{
    private ChannelMap channelMap;
    public MainChannel(String topic, PortList portList,ExecutorService executor, ChannelMap channelMap){
        super(topic, portList, executor);
        // channel map is used to create new channel
        this.channelMap = channelMap;
    }
    @Override
    public void start(){
        this.executor.submit(()->{
            // start listening to receiver port
            this.recSocket.bind("tcp://*:" + this.recPort);
            // subscribe topic
            this.recSocket.subscribe(topic.getBytes());
            // start connecting to sending port
            this.sendSocket.bind("tcp://*:" + this.sendPort);
            while(true){
                // just keep receiving and sending messages
                if(!this.isEmpty()){
                    ZMsg receivedMsg= ZMsg.recvMsg(this.recSocket);
                    {
                        // debug
                        System.out.println("Message Received (From Main Channel) from Port " + this.recPort);
                        System.out.println(new String(receivedMsg.getFirst().getData()));
                        //System.out.println(DataSampleHelper.deserialize(receivedMsg.getLast().getData()).sampleId());
                        System.out.println(new String(receivedMsg.getLast().getData()));
                    }
                    String msgTopic = new String(receivedMsg.getFirst().getData());
                    // if this topic is new, create a new channel for it
                    if(channelMap.get(topic) == null ){
                        MsgChannel newChannel = channelMap.register(msgTopic, this.portList, this.executor);
                        newChannel.start();
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
        });
        {
            // debug
            System.out.println("Main Channel Started, topic: " + this.topic);
        }
    }
}
