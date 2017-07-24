package edu.vanderbilt.chuilian.test.reconfiguration;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;

/**
 * Created by Killian on 7/24/17.
 */
public class EdgeBroker {
    private String myID;
    private String myIP;
    // key: topic, value: msg channel for this topic
    private HashMap<String, Channel> channels;

    // logger config
    private static final Logger logger = LogManager.getLogger(EdgeBroker.class.getName());

    public EdgeBroker(String myID, String myIP){
        this.myID = myID;
        this.myIP = myIP;
    }

    public void createChannel(String topic, String recPort, String sendPort){
        Channel newChannel = new Channel(topic, myIP, recPort, sendPort);
        channels.put(topic, newChannel);
        Thread t = new Thread(newChannel);
        t.run();
        logger.debug("New Channel Created. topic: {}", topic);
    }

    public void closeChannel(String topic){
        Channel channel = channels.get(topic);
        if(channel != null) channel.stop();
        channels.remove(topic);
    }
}
