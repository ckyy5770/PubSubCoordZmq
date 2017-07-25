package edu.vanderbilt.chuilian.test.reconfiguration;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.util.concurrent.BlockingQueue;

/**
 * Created by Killian on 7/24/17.
 */
public class Receiver implements Runnable{
    // receiver attrs
    private String topic;
    private String srcAddr;
    private volatile boolean stop = false;
    private BlockingQueue<String> msgBuff;

    // zmq config
    private ZMQ.Context zmqContext;
    private ZMQ.Socket recSocket;

    // logger config
    private static final Logger logger = LogManager.getLogger(Receiver.class.getName());

    public Receiver(String topic, String srcAddr, BlockingQueue<String> msgBuff, ZMQ.Context zmqContext){
        this.topic = topic;
        this.srcAddr = srcAddr;
        this.msgBuff = msgBuff;
        // init zmq socket
        this.zmqContext = zmqContext;
        this.recSocket = zmqContext.socket(ZMQ.SUB);
    }

    @Override
    public void run(){
        recSocket.connect("tcp://" + srcAddr);
        recSocket.subscribe(topic.getBytes());
        try{
            while(!stop){
                // rec 1 msg
                ZMsg receivedMsg = ZMsg.recvMsg(recSocket);
                String msgTopic = new String(receivedMsg.getFirst().getData());
                byte[] msgContent = receivedMsg.getLast().getData();
                // put msg to buffer
                String msgString = msgTopic + "," + new String(msgContent);
                msgBuff.put(msgString);
                // log
                logger.debug("Message Received. topic: {} content: {} src: {}", msgTopic, msgContent, srcAddr);
            }
        } catch(Exception e){
            logger.debug("Exception: {}",e.getMessage());
        } finally {
            // shutdown zmq socket
            recSocket.close();
            logger.debug("Receiver Stopped. topic: {}, srcAddr: {}", topic, srcAddr);
        }
    }

    public void stop(){
        this.stop = true;
        // shutdown zmq socket and context
        // note Thread.interrupt will not terminate a blocked socket call.
        recSocket.close();
    }

    public String getSrcAddr() {
        return srcAddr;
    }
}
