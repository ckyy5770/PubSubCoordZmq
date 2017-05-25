package edu.vanderbilt.chuilian.util;

import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.util.Queue;
import java.util.concurrent.ExecutorService;

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

    //default constructor simply do nothing
    protected MsgChannel() {
    }

    ;

    /**
     *
     * @param topic
     * @param portList available port list, there should be only one port list within a broker
     * @param executor executor for worker threads, there should be only one executor within a broker
     */
    public MsgChannel(String topic, PortList portList, ExecutorService executor, ZkConnect zkConnect) {
        // initialize member vars
        this.topic = topic;
        this.portList = portList;
        this.executor = executor;
        this.zkConnect = zkConnect;
        // initialize socket
        this.recContext= ZMQ.context(1);
        this.recSocket= this.recContext.socket(ZMQ.SUB);
        this.sendContext= ZMQ.context(1);
        this.sendSocket= this.sendContext.socket(ZMQ.PUB);
        // get two unused port number from available list
        this.sendPort = portList.get();
        this.recPort = portList.get();
    }

    /**
     * start the channel: activate sender socket and receiver socket
     * keep receiving and sending messages.
     */
    public void start(){
        this.executor.submit(()->{
            // start listening to receiver port
            this.recSocket.bind("tcp://*:" + this.recPort);
            // subscribe topic
            this.recSocket.subscribe(this.topic.getBytes());
            // start connecting to sending port
            this.sendSocket.bind("tcp://*:" + this.sendPort);
            // register itself to zookeeper service
            this.zkConnect.registerChannel(this.topic, this.ip + ":" + Integer.toString(this.recPort), this.ip + ":" + Integer.toString(this.sendPort));
            // begin receiving and sending messages
            while(true){
                // just keep receiving and sending messages
                if(!this.isEmpty()){
                    ZMsg receivedMsg= ZMsg.recvMsg(this.recSocket);
                    {
                        // debug
                        System.out.println("Message Received from Port " + this.recPort);
                        System.out.println(new String(receivedMsg.getFirst().getData()));
                        //System.out.println(DataSampleHelper.deserialize(receivedMsg.getLast().getData()).sampleId());
                        System.out.println(new String(receivedMsg.getLast().getData()));
                    }
                    this.sendSocket.sendMore(receivedMsg.getFirst().getData());
                    this.sendSocket.send(receivedMsg.getLast().getData());
                    {
                        // debug
                        System.out.println("Message Sent to Port " + this.sendPort);
                        System.out.println(new String(receivedMsg.getFirst().getData()));
                        //System.out.println(DataSampleHelper.deserialize(receivedMsg.getLast().getData()).sampleId());
                        System.out.println(new String(receivedMsg.getLast().getData()));
                    }
                }
            }
        });
        {
            // debug
            System.out.println("Channel Started, topic: " + this.topic);
        }
    }

    /**
     * add a message to the end of the queue
     * @param msg
     */
    private void add(ZMsg msg){
        msgList.add(msg);
    }

    /**
     * get and remove the first message from the queue
     * @return the first message, if the queue is empty, return null.
     */
    private ZMsg poll(){
        return msgList.poll();
    }

    /**
     * is empty
     * @return
     */
    protected boolean isEmpty(){
        return msgList.isEmpty();
    }




}
