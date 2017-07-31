package edu.vanderbilt.chuilian.test.reconfiguration;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by Killian on 7/24/17.
 */
public class Publisher{
    String myID;
    String myIP;
    private ZMQ.Context zmqContext;
    // key: topic, value: a list of senders corresponding to this topic
    HashMap<String, ArrayList<Sender>> senders = new HashMap<>();

    // logger config
    private static final Logger logger = LogManager.getLogger(Publisher.class.getName());

    public Publisher(String myID, String myIP){
        this.myID = myID;
        this.myIP = myIP;
        this.zmqContext = ZMQ.context(1);
    }

    public void createSender(String topic, String destAddr){
        Sender newSender = new Sender(topic, destAddr, zmqContext);
        ArrayList<Sender> senderList = senders.getOrDefault(topic, new ArrayList<>());
        senderList.add(newSender);
        senders.put(topic, senderList);
        Thread t = new Thread(newSender);
        t.start();
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

    /**
     * To avoid lose any messages, publisher reconnect should always follows the following pattern:
     * 1. create a new publisher with new destination address
     * 2. wait for a proper amount of time for connection to be stable
     * 3. stop the old publisher with old destination address
     * In this way, reconnection will hopefully not lose any messages, but may have some duplication messages,
     * we could easily take care of duplicates whenever we want.
     * @param topic
     * @param oldDestAddr
     * @param newDestAddr
     */
    public void reconnect(String topic, String oldDestAddr, String newDestAddr){
        createSender(topic, newDestAddr);
        try{
            Thread.sleep(500);
        }catch (InterruptedException ie){
            logger.warn("interrupted during publisher reconfiguration! {}", ie.getMessage());
        }
        stopSender(topic, oldDestAddr);
    }

    public static void main(String[] args) throws Exception {
        // hard coded config
        String pubID = "pub0";
        String pubIP = "10.0.2.15";
        String[] topics = new String[1];
        topics[0] = "t0";
        String[] destAddrs = new String[1];
        destAddrs[0] = "10.0.2.15:5000";
        //destAddrs[0] = "127.0.0.1:5000";
        int[] sendIntervals = new int[1];
        sendIntervals[0] = 10;


        testBasic(pubID, pubIP, topics, destAddrs, sendIntervals);
    }

    static void testBasic(String pubID, String pubIP, String[] topics, String[] destAddrs, int[] sendIntervals) throws Exception{
        Publisher pub = new Publisher(pubID, pubIP);
        for(int i=0; i< topics.length; i++){
            pub.createSender(topics[i], destAddrs[i]);
        }
        for(int i=0; i<topics.length; i++){
            Thread t = new Thread(new SenderFeed(pub, topics[i], sendIntervals[i]));
            t.start();
        }
    }

    static void testHashAllPub() throws Exception{
        int PUB_NUM = 2;
        int PHASE1_TIME = 15;
        int PHASE2_TIME = 15;

        ArrayList<Publisher> pubs = new ArrayList<>();
        for(int i =0; i< PUB_NUM; i++){
            pubs.add(new Publisher("pub" + i, "127.0.0.1"));
        }

        // connect pubs to b0t1
        for(Publisher pub : pubs){
            pub.createSender("t0", "127.0.0.1:5000");
        }

        // send messages
        Thread t = new Thread(()->{
            int counter = 0;
            try{
                while(true){
                    for(int j=0; j<PUB_NUM; j++){
                        pubs.get(j).send("t0", "pub" + j + "," + counter);
                    }
                    counter++;
                    Thread.sleep(10);
                }
            }catch(Exception e){
                logger.debug(e.getMessage());
            }

        });
        t.start();

        Thread.sleep(PHASE1_TIME*1000);

        // enter phase 2: pubs connects to both brokers
        // connect pubs to b0t1
        for(Publisher pub : pubs){
            pub.createSender("t0", "127.0.0.1:5001");
        }

        Thread.sleep(PHASE2_TIME*1000);

        for(Publisher pub : pubs){
            pub.stopSender("t0", "127.0.0.1:5000");
            pub.stopSender("t0", "127.0.0.1:5001");
        }
    }


    static void testHashAllSub() throws Exception{
        int PUB_NUM = 2;
        int PHASE1_TIME = 15;
        int PHASE2_TIME = 15;
        int DELAY = 5;

        ArrayList<Publisher> pubs = new ArrayList<>();
        for(int i =0; i< PUB_NUM; i++){
            pubs.add(new Publisher("pub" + i, "127.0.0.1"));
        }

        // connect pubs to b0t1
        for(Publisher pub : pubs){
            pub.createSender("t0", "127.0.0.1:5000");
        }

        // send messages
        Thread t = new Thread(()->{
            int counter = 0;
            try{
                while(true){
                    for(int j=0; j<PUB_NUM; j++){
                        pubs.get(j).send("t0", "pub" + j + "," + counter);
                    }
                    counter++;
                    Thread.sleep(10);
                }
            }catch(Exception e){
                logger.debug(e.getMessage());
            }

        });
        t.start();

        Thread.sleep((PHASE1_TIME+DELAY)*1000);

        // enter phase 2: one publisher reconnect to another broker
        pubs.get(0).reconnect("t0","127.0.0.1:5000", "127.0.0.1:5001");

        Thread.sleep((PHASE2_TIME-DELAY)*1000);

        pubs.get(0).stopSender("t0", "127.0.0.1:5001");
        pubs.get(1).stopSender("t0", "127.0.0.1:5000");

    }

    static void testAllPubHash() throws Exception{
        int PUB_NUM = 2;
        int PHASE1_TIME = 15;
        int PHASE2_TIME = 15;

        ArrayList<Publisher> pubs = new ArrayList<>();
        for(int i =0; i< PUB_NUM; i++){
            pubs.add(new Publisher("pub" + i, "127.0.0.1"));
        }

        // connect pubs to b0t1, b1t1
        for(Publisher pub : pubs){
            pub.createSender("t0", "127.0.0.1:5000");
            pub.createSender("t0", "127.0.0.1:5001");
        }

        // send messages
        Thread t = new Thread(()->{
            int counter = 0;
            try{
                while(true){
                    for(int j=0; j<PUB_NUM; j++){
                        pubs.get(j).send("t0", "pub" + j + "," + counter);
                    }
                    counter++;
                    Thread.sleep(10);
                }
            }catch(Exception e){
                logger.debug(e.getMessage());
            }

        });
        t.start();

        Thread.sleep(PHASE1_TIME*1000);

        // enter phase 2: pubs disconnect from b1
        for(Publisher pub : pubs){
            pub.stopSender("t0", "127.0.0.1:5001");
        }

        Thread.sleep(PHASE2_TIME*1000);

        for(Publisher pub : pubs){
            pub.stopSender("t0", "127.0.0.1:5000");
        }
    }

    static void testAllSubHash() throws Exception{
        int PUB_NUM = 2;
        int PHASE1_TIME = 15;
        int PHASE2_TIME = 15;
        int DELAY = 5;

        ArrayList<Publisher> pubs = new ArrayList<>();
        for(int i =0; i< PUB_NUM; i++){
            pubs.add(new Publisher("pub" + i, "127.0.0.1"));
        }

        // connect pub1 to b0t1
        pubs.get(0).createSender("t0", "127.0.0.1:5001");
        // connect pub2 to b1t1
        pubs.get(1).createSender("t0", "127.0.0.1:5000");

        // send messages
        Thread t = new Thread(()->{
            int counter = 0;
            try{
                while(true){
                    for(int j=0; j<PUB_NUM; j++){
                        pubs.get(j).send("t0", "pub" + j + "," + counter);
                    }
                    counter++;
                    Thread.sleep(10);
                }
            }catch(Exception e){
                logger.debug(e.getMessage());
            }

        });
        t.start();

        Thread.sleep((PHASE1_TIME-DELAY)*1000);

        // enter phase 2: pub0 reconnect back to b1
        pubs.get(0).reconnect("t0","127.0.0.1:5001", "127.0.0.1:5000");

        Thread.sleep((PHASE2_TIME+DELAY)*1000);

        pubs.get(0).stopSender("t0", "127.0.0.1:5000");
        pubs.get(1).stopSender("t0", "127.0.0.1:5000");

    }

    static void testAllPubCreate() throws Exception {
        int PUB_NUM = 3;
        int PHASE1_TIME = 15;
        int PHASE2_TIME = 15;
        int DELAY = 5;

        ArrayList<Publisher> pubs = new ArrayList<>();
        for(int i =0; i< PUB_NUM; i++){
            pubs.add(new Publisher("pub" + i, "127.0.0.1"));
        }

        // connect pubs to all two brokers
        for(Publisher pub : pubs){
            pub.createSender("t0", "127.0.0.1:5000");
            pub.createSender("t0", "127.0.0.1:5001");
        }

        // send messages
        Thread t = new Thread(()->{
            int counter = 0;
            try{
                while(true){
                    for(int j=0; j<PUB_NUM; j++){
                        pubs.get(j).send("t0", "pub" + j + "," + counter);
                    }
                    counter++;
                    Thread.sleep(10);
                }
            }catch(Exception e){
                logger.debug(e.getMessage());
            }

        });
        t.start();

        Thread.sleep((PHASE1_TIME)*1000);

        // enter phase 2: pubs all connect to new broker
        for(Publisher pub : pubs){
            pub.createSender("t0", "127.0.0.1:5002");
        }

        Thread.sleep((PHASE2_TIME)*1000);

        for(Publisher pub : pubs){
            pub.stopSender("t0","127.0.0.1:5000");
            pub.stopSender("t0","127.0.0.1:5001");
            pub.stopSender("t0","127.0.0.1:5002");
        }

    }

    static void testAllSubCreate() throws Exception {
        int PUB_NUM = 3;
        int PHASE1_TIME = 15;
        int PHASE2_TIME = 15;
        int DELAY = 5;

        ArrayList<Publisher> pubs = new ArrayList<>();
        for(int i =0; i< PUB_NUM; i++){
            pubs.add(new Publisher("pub" + i, "127.0.0.1"));
        }

        // connect pub0 to b0, pub1,2 to b1
        pubs.get(0).createSender("t0", "127.0.0.1:5000");
        pubs.get(1).createSender("t0", "127.0.0.1:5001");
        pubs.get(2).createSender("t0", "127.0.0.1:5001");

        // send messages
        Thread t = new Thread(()->{
            int counter = 0;
            try{
                while(true){
                    for(int j=0; j<PUB_NUM; j++){
                        pubs.get(j).send("t0", "pub" + j + "," + counter);
                    }
                    counter++;
                    Thread.sleep(10);
                }
            }catch(Exception e){
                logger.debug(e.getMessage());
            }

        });
        t.start();

        Thread.sleep((PHASE1_TIME + DELAY)*1000);

        // enter phase 2: reconnect pub2 to b2
        pubs.get(2).reconnect("t0","127.0.0.1:5001", "127.0.0.1:5002");

        Thread.sleep((PHASE2_TIME - DELAY)*1000);

        pubs.get(0).stopSender("t0","127.0.0.1:5000");
        pubs.get(1).stopSender("t0","127.0.0.1:5001");
        pubs.get(2).stopSender("t0","127.0.0.1:5002");
    }

    static void testAllPubClose() throws Exception {
        int PUB_NUM = 3;
        int PHASE1_TIME = 15;
        int PHASE2_TIME = 15;
        int DELAY = 5;

        ArrayList<Publisher> pubs = new ArrayList<>();
        for(int i =0; i< PUB_NUM; i++){
            pubs.add(new Publisher("pub" + i, "127.0.0.1"));
        }

        // connect all pubs to all three brokers
        for(Publisher pub : pubs){
            pub.createSender("t0", "127.0.0.1:5000");
            pub.createSender("t0", "127.0.0.1:5001");
            pub.createSender("t0", "127.0.0.1:5002");
        }

        // send messages
        Thread t = new Thread(()->{
            int counter = 0;
            try{
                while(true){
                    for(int j=0; j<PUB_NUM; j++){
                        pubs.get(j).send("t0", "pub" + j + "," + counter);
                    }
                    counter++;
                    Thread.sleep(10);
                }
            }catch(Exception e){
                logger.debug(e.getMessage());
            }

        });
        t.start();

        Thread.sleep((PHASE1_TIME)*1000);

        // enter phase 2: all pub disconnect from b2
        for(Publisher pub : pubs){
            pub.stopSender("t0", "127.0.0.1:5002");
        }

        Thread.sleep((PHASE2_TIME)*1000);

        for(Publisher pub : pubs){
            pub.stopSender("t0", "127.0.0.1:5000");
            pub.stopSender("t0", "127.0.0.1:5001");
        }

    }

    static void testAllSubClose() throws Exception {
        int PUB_NUM = 3;
        int PHASE1_TIME = 15;
        int PHASE2_TIME = 15;
        int DELAY = 5;

        ArrayList<Publisher> pubs = new ArrayList<>();
        for(int i =0; i< PUB_NUM; i++){
            pubs.add(new Publisher("pub" + i, "127.0.0.1"));
        }

        // connect p0-b0 p1-b1 p2-b2
        pubs.get(0).createSender("t0", "127.0.0.1:5000");
        pubs.get(1).createSender("t0", "127.0.0.1:5001");
        pubs.get(2).createSender("t0", "127.0.0.1:5002");

        // send messages
        Thread t = new Thread(()->{
            int counter = 0;
            try{
                while(true){
                    for(int j=0; j<PUB_NUM; j++){
                        pubs.get(j).send("t0", "pub" + j + "," + counter);
                    }
                    counter++;
                    Thread.sleep(10);
                }
            }catch(Exception e){
                logger.debug(e.getMessage());
            }

        });
        t.start();

        Thread.sleep((PHASE1_TIME - DELAY)*1000);

        // enter phase 2: pub2 reconnect to b1
        pubs.get(2).reconnect("t0", "127.0.0.1:5002", "127.0.0.1:5001");

        Thread.sleep((PHASE2_TIME + DELAY)*1000);

        pubs.get(0).stopSender("t0", "127.0.0.1:5000");
        pubs.get(1).stopSender("t0", "127.0.0.1:5001");
        pubs.get(2).stopSender("t0", "127.0.0.1:5001");
    }

    static void testHashHash() throws Exception {
        int PUB_NUM = 2;
        int PHASE1_TIME = 15;
        int PHASE2_TIME = 20;
        int DELAY = 5;

        ArrayList<Publisher> pubs = new ArrayList<>();
        for(int i =0; i< PUB_NUM; i++){
            pubs.add(new Publisher("pub" + i, "127.0.0.1"));
        }

        // connect p0,1 to b0
        for(Publisher pub : pubs){
            pub.createSender("t0", "127.0.0.1:5000");
        }

        // send messages
        Thread t = new Thread(()->{
            int counter = 0;
            try{
                while(true){
                    for(int j=0; j<PUB_NUM; j++){
                        pubs.get(j).send("t0", "pub" + j + "," + counter);
                    }
                    counter++;
                    Thread.sleep(10);
                }
            }catch(Exception e){
                logger.debug(e.getMessage());
            }

        });
        t.start();

        Thread.sleep((PHASE1_TIME + DELAY)*1000);

        for(Publisher pub : pubs){
            pub.reconnect("t0", "127.0.0.1:5000", "127.0.0.1:5001");
        }

        Thread.sleep((PHASE2_TIME - DELAY)*1000);

    }

}
