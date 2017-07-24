package edu.vanderbilt.chuilian.test.reconfiguration;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by Killian on 7/24/17.
 */
public class Publisher{
    String myID;
    String myIP;
    // key: topic, value: a list of senders corresponding to this topic
    HashMap<String, ArrayList<Sender>> senders;

    // logger config
    private static final Logger logger = LogManager.getLogger(Publisher.class.getName());

    public Publisher(String myID, String myIP){
        this.myID = myID;
        this.myIP = myIP;
    }

    public void createSender(String topic, String destAddr){
        Sender newSender = new Sender(topic, destAddr);
        ArrayList<Sender> senderList = senders.getOrDefault(topic, new ArrayList<>());
        senderList.add(newSender);
        senders.put(topic, senderList);
        Thread t = new Thread(newSender);
        t.run();
        logger.debug("New Sender Created. topic: {}, destAddr: {}", topic, destAddr);
    }

    public void stopSender(String topic, String destAddr){
        ArrayList<Sender> senderList = senders.get(topic);
        if(senderList != null && !senderList.isEmpty()){
            for(int i=0; i<senderList.size(); i++){
                Sender cur = senderList.get(i);
                if(cur.getDestAddr().equals(destAddr)){
                    cur.stop();
                    senderList.remove(i--);
                }
            }
        }
    }

    public void send(String topic, String msg){
        ArrayList<Sender> senderList = senders.get(topic);
        if(senderList != null){
            for(Sender sender: senderList){
                sender.send(msg);
            }
        }
    }

}
