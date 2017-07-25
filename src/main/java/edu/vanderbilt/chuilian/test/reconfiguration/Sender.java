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
    private Thread thread;
    // sender attrs
    private String topic;
    private String destAddr;
    private volatile boolean stop = false;
    private BlockingQueue<String> bq = new LinkedBlockingQueue<>();

    // zmq config
    private ZMQ.Context zmqContext;
    private ZMQ.Socket sendSocket;


    // logger config
    private static final Logger logger = LogManager.getLogger(Sender.class.getName());

    public Sender(String topic, String destAddr, ZMQ.Context zmqContext){
        this.topic = topic;
        this.destAddr = destAddr;
        // init zmq socket
        this.zmqContext = zmqContext;
        this.sendSocket = zmqContext.socket(ZMQ.PUB);
    }

    @Override
    public void run(){
        this.thread = Thread.currentThread();
        sendSocket.connect("tcp://" + destAddr);
        String nextMsg;
        try {
            while (!stop) {
                nextMsg = bq.take();
                sendSocket.sendMore(topic);
                sendSocket.send(nextMsg);
                // log
                logger.debug("Message Sent. topic: {} content: {} dest: {}", topic, nextMsg, destAddr);
            }
        } catch(Exception e){
            logger.debug("Exception: {}",e.getMessage());
        } finally {
            // shutdown zmq socket
            sendSocket.close();
            logger.debug("Sender Stopped. topic: {}, destAddr: {}", topic, destAddr);
        }
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
        this.thread.interrupt();
    }

    public String getDestAddr() {
        return destAddr;
    }
}
