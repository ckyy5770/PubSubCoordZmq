package edu.vanderbilt.chuilian.test.reconfiguration;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zmq.Sub;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * Created by Killian on 7/24/17.
 */
public class Subscriber{
    String myID;
    String myIP;
    // key: topic, value: a list of senders corresponding to this topic
    HashMap<String, ArrayList<Receiver>> receivers;
    // msg buffer
    BlockingQueue<String> msgBuffer = new PriorityBlockingQueue<>();

    // logger config
    private static final Logger logger = LogManager.getLogger(Subscriber.class.getName());

    public Subscriber(String myID, String myIP){
        this.myID = myID;
        this.myIP = myIP;
    }

    public void createReceiver(String topic, String srcAddr){
        Receiver newReceiver = new Receiver(topic, srcAddr, msgBuffer);
        ArrayList<Receiver> receiverList = receivers.getOrDefault(topic, new ArrayList<>());
        receiverList.add(newReceiver);
        receivers.put(topic, receiverList);
        Thread t = new Thread(newReceiver);
        t.run();
        logger.debug("New Sender Created. topic: {}, srcAddr: {}", topic, srcAddr);
    }

    public void stopReceiver(String topic, String srcAddr){
        ArrayList<Receiver> receiverList = receivers.get(topic);
        if(receiverList != null && !receiverList.isEmpty()){
            for(int i=0; i<receiverList.size(); i++){
                Receiver cur = receiverList.get(i);
                if(cur.getSrcAddr().equals(srcAddr)){
                    cur.stop();
                    receiverList.remove(i--);
                }
            }
        }
    }

}
