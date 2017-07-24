package edu.vanderbilt.chuilian.test.reconfiguration;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by Killian on 7/24/17.
 */
public class Sender implements Runnable{
    // sender attrs
    private String topic;
    private String destAddr;
    private volatile boolean stop = false;
    private BlockingQueue<String> bq = new LinkedBlockingQueue<>();

    // zmq config
    private ZMQ.Context sendContext;
    private ZMQ.Socket sendSocket;


    // logger config
    private static final Logger logger = LogManager.getLogger(Sender.class.getName());

    public Sender(String topic, String destAddr){
        this.topic = topic;
        this.destAddr = destAddr;
        // init zmq socket
        this.sendContext = ZMQ.context(1);
        this.sendSocket = sendContext.socket(ZMQ.PUB);
    }

    @Override
    public void run(){
        sendSocket.connect("tcp://" + destAddr);
        String nextMsg = null;
        while(!stop){
            try{
                nextMsg = bq.take();
            }catch(InterruptedException ie){
                logger.warn(ie.getMessage());
            }
            sendSocket.sendMore(topic);
            sendSocket.send(nextMsg);
            // log
            logger.debug("Message Sent. topic: {} content: {} dest: {}", topic, nextMsg, destAddr);
        }
        // shutdown zmq socket and context
        sendSocket.close();
        sendContext.term();
        logger.debug("Sender Stopped. topic: {}, destAddr: {}", topic, destAddr);
    }

    public void send(String msg){
        try{
            bq.put(msg);
        }catch (InterruptedException ie){
            logger.warn(ie.getMessage());
        }
    }

    public void stop(){
        this.stop = true;
    }

    public String getDestAddr() {
        return destAddr;
    }
}
