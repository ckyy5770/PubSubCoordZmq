package edu.vanderbilt.chuilian.test.reconfiguration;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by Killian on 7/24/17.
 */
public class EdgeBroker {
    private String myID;
    private String myIP;
    private ZMQ.Context zmqContext;
    // key: topic, value: msg channel for this topic
    private HashMap<String, Channel> channels = new HashMap<>();

    // logger config
    private static final Logger logger = LogManager.getLogger(EdgeBroker.class.getName());

    public EdgeBroker(String myID, String myIP){
        this.myID = myID;
        this.myIP = myIP;

        zmqContext = ZMQ.context(1);
    }

    public void createChannel(String topic, String recPort, String sendPort){
        Channel newChannel = new Channel(topic, myIP, recPort, sendPort, zmqContext);
        channels.put(topic, newChannel);
        Thread t = new Thread(newChannel);
        t.start();
        logger.debug("New Channel Created. topic: {}", topic);
    }

    public void closeChannel(String topic){
        Channel channel = channels.get(topic);
        if(channel != null) channel.stop();
        channels.remove(topic);
    }

    public static void main(String[] args) throws Exception {
        testAllPubSubHash();
    }

    static void testHashAllPubSub() throws Exception{
        int BROKER_NUM = 2;
        int PHASE1_TIME = 15;
        int PHASE2_TIME = 15;
        int DELAY = 5;

        ArrayList<EdgeBroker> brokers = new ArrayList<>();
        for(int i =0; i< BROKER_NUM; i++){
            brokers.add(new EdgeBroker("broker" + i, "127.0.0.1"));
        }

        // create a channel of topic "t0" in broker 0
        brokers.get(0).createChannel("t0", Integer.toString(5000), Integer.toString(6000));

        // channel should be created first
        Thread.sleep((PHASE1_TIME-DELAY)*1000);

        // create a channel of topic "t0" in broker 1
        brokers.get(1).createChannel("t0", Integer.toString(5001), Integer.toString(6001));

        Thread.sleep((PHASE2_TIME + DELAY)*1000);

        brokers.get(0).closeChannel("t0");
        brokers.get(1).closeChannel("t0");
    }

    static void testAllPubSubHash() throws Exception{
        int BROKER_NUM = 2;
        int PHASE1_TIME = 15;
        int PHASE2_TIME = 15;
        int DELAY = 5;

        ArrayList<EdgeBroker> brokers = new ArrayList<>();
        for(int i =0; i< BROKER_NUM; i++){
            brokers.add(new EdgeBroker("broker" + i, "127.0.0.1"));
        }

        // create channels
        brokers.get(0).createChannel("t0", Integer.toString(5000), Integer.toString(6000));
        brokers.get(1).createChannel("t0", Integer.toString(5001), Integer.toString(6001));

        // channel should be closed lastly
        Thread.sleep((PHASE1_TIME+DELAY)*1000);

        // close channel in broker 1
        brokers.get(1).closeChannel("t0");

        Thread.sleep((PHASE2_TIME - DELAY)*1000);

        brokers.get(0).closeChannel("t0");

    }
}
