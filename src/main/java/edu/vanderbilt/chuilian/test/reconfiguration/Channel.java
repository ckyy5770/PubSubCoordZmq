package edu.vanderbilt.chuilian.test.reconfiguration;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;


/**
 * Created by Killian on 7/24/17.
 */
public class Channel implements Runnable {
    // channel attrs
    private String topic;
    private String myIP;
    private String recPort;
    private String sendPort;
    private volatile boolean stop = false;

    // zmq config
    private ZMQ.Context sendContext;
    private ZMQ.Socket sendSocket;
    private ZMQ.Context recContext;
    private ZMQ.Socket recSocket;

    // logger config
    private static final Logger logger = LogManager.getLogger(Channel.class.getName());

    public Channel(String topic, String myIP, String recPort, String sendPort){
        this.topic = topic;
        this.myIP = myIP;
        this.recPort = recPort;
        this.sendPort = sendPort;

        // init zmq socket
        this.recContext = ZMQ.context(1);
        this.recSocket = this.recContext.socket(ZMQ.SUB);
        this.sendContext = ZMQ.context(1);
        this.sendSocket = this.sendContext.socket(ZMQ.PUB);
    }

    @Override
    public void run(){
        recSocket.bind("tcp://*:" + recPort);
        recSocket.subscribe(topic.getBytes());
        sendSocket.bind("tcp://*:" + sendPort);
        while(!stop){
            // rec 1 msg
            ZMsg receivedMsg = ZMsg.recvMsg(recSocket);
            String msgTopic = new String(receivedMsg.getFirst().getData());
            byte[] msgContent = receivedMsg.getLast().getData();
            // send it out
            sendSocket.sendMore(msgTopic);
            sendSocket.send(msgContent);
            // log
            logger.debug("Message Transmitted. topic: {} content: {}", msgTopic, msgContent);
        }
        // shutdown zmq socket and context
        recSocket.close();
        recContext.term();
        sendSocket.close();
        sendContext.term();
        logger.debug("Channel Stopped. topic: {}", topic);
    }

    public void stop(){
        stop = true;
    }
}
