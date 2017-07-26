package edu.vanderbilt.chuilian.test.reconfiguration;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ;
import zmq.Sub;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * Created by Killian on 7/24/17.
 */
public class Subscriber{
    private String myID;
    private String myIP;
    private ZMQ.Context zmqContext;
    // key: topic, value: a list of senders corresponding to this topic
    private HashMap<String, ArrayList<Receiver>> receivers = new HashMap<>();
    // msg buffer
    private BlockingQueue<String> msgBuffer = new PriorityBlockingQueue<>();

    // logger config
    private static final Logger logger = LogManager.getLogger(Subscriber.class.getName());
    private static final Logger resLogger = LogManager.getLogger("TestResult");

    public Subscriber(String myID, String myIP){
        this.myID = myID;
        this.myIP = myIP;
        this.zmqContext = ZMQ.context(1);
    }

    public void createReceiver(String topic, String srcAddr){
        Receiver newReceiver = new Receiver(topic, srcAddr, msgBuffer, zmqContext);
        ArrayList<Receiver> receiverList = receivers.getOrDefault(topic, new ArrayList<>());
        receiverList.add(newReceiver);
        receivers.put(topic, receiverList);
        Thread t = new Thread(newReceiver);
        t.start();
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

    /**
     * To avoid lose any messages, subscriber reconnect should always follows the following pattern:
     * 1. create a new receiver with new source address
     * 2. wait for a proper amount of time for connection to be stable
     * 3. stop the old receiver with old source address
     * In this way, reconnection will hopefully not lose any messages, but may have some duplication messages,
     * we could easily take care of duplicates whenever we want.
     * @param topic
     * @param oldSrcAddr
     * @param newSrcAddr
     */
    public void reconnect(String topic, String oldSrcAddr, String newSrcAddr){
        createReceiver(topic, newSrcAddr);
        try{
            Thread.sleep(500);
        }catch (InterruptedException ie){
            logger.warn("interrupted during subscriber reconfiguration! {}", ie.getMessage());
        }
        stopReceiver(topic, oldSrcAddr);
    }

    public BlockingQueue<String> getMsgBuffer() {
        return msgBuffer;
    }

    public String getMyID() {
        return myID;
    }

    public ArrayList<String> getCleanedBuffer(boolean removeDup){
        BlockingQueue<String> buff = getMsgBuffer();
        ArrayList<String> res = new ArrayList<>();
        try{
            while(!buff.isEmpty()){
                res.add(buff.take());
            }
        }catch(Exception e){

        }
        res.sort((String m1, String m2)->{
            String[] data1 = m1.split(",");
            String[] data2 = m2.split(",");
            String t1 = data1[0];
            String t2 = data2[0];
            String pubID1 = data1[1];
            String pubID2 = data2[1];
            String msgID1 = data1[2];
            String msgID2 = data2[2];
            if(!t1.equals(t2)){
                return t1.compareTo(t2);
            }else if(!pubID1.equals(pubID2)){
                return pubID1.compareTo(pubID2);
            }else{
                return Integer.compare(Integer.parseInt(msgID1), Integer.parseInt(msgID2));
            }
        });
        if(removeDup){
            // remove dups
            if(res.size() <= 1) return res;
            String prev = res.get(0);
            for(int i=1; i<res.size(); i++){
                String cur = res.get(i);
                if(cur.equals(prev)) res.remove(i--);
                prev = cur;
            }
        }
        return res;
    }

    public static void main(String[] args) throws Exception {
        testHashHash();
    }

    static void testHashAllPub() throws Exception{
        int SUB_NUM = 2;
        int PHASE1_TIME = 15;
        int PHASE2_TIME = 15;
        int DELAY = 5;

        ArrayList<Subscriber> subs = new ArrayList<>();
        for(int i =0; i< SUB_NUM; i++){
            subs.add(new Subscriber("sub" + i, "127.0.0.1"));
        }

        // connect pubs to b0t1
        for(Subscriber sub : subs){
            sub.createReceiver("t0", "127.0.0.1:6000");
        }

        Thread.sleep((PHASE1_TIME + DELAY) *1000);

        // enter phase 2: one subscriber reconnect to another broker
        subs.get(0).reconnect("t0","127.0.0.1:6000", "127.0.0.1:6001");

        Thread.sleep((PHASE2_TIME - DELAY)*1000);

        subs.get(0).stopReceiver("t0", "127.0.0.1:6001");
        subs.get(1).stopReceiver("t0", "127.0.0.1:6000");

        // get result
        for(Subscriber sub : subs){
            ArrayList<String> buff = sub.getCleanedBuffer(true);
            String id = sub.getMyID();
            for(String elem : buff){
                resLogger.info("{},{}", id, elem);
            }
        }
    }

    static void testHashAllSub() throws Exception{
        int SUB_NUM = 2;
        int PHASE1_TIME = 15;
        int PHASE2_TIME = 15;
        int DELAY = 5;

        ArrayList<Subscriber> subs = new ArrayList<>();
        for(int i =0; i< SUB_NUM; i++){
            subs.add(new Subscriber("sub" + i, "127.0.0.1"));
        }

        // connect pubs to b0t1
        for(Subscriber sub : subs){
            sub.createReceiver("t0", "127.0.0.1:6000");
        }

        Thread.sleep((PHASE1_TIME) *1000);

        // enter phase 2: pubs connects to both brokers
        // connect pubs to b0t1
        for(Subscriber sub : subs){
            sub.createReceiver("t0", "127.0.0.1:6001");
        }

        Thread.sleep((PHASE2_TIME)*1000);

        for(Subscriber sub : subs){
            sub.stopReceiver("t0", "127.0.0.1:6000");
            sub.stopReceiver("t0", "127.0.0.1:6001");
        }

        // get result
        for(Subscriber sub : subs){
            ArrayList<String> buff = sub.getCleanedBuffer(true);
            String id = sub.getMyID();
            for(String elem : buff){
                resLogger.info("{},{}", id, elem);
            }
        }
    }


    static void testAllPubHash() throws Exception{
        int SUB_NUM = 2;
        int PHASE1_TIME = 15;
        int PHASE2_TIME = 15;
        int DELAY = 5;

        ArrayList<Subscriber> subs = new ArrayList<>();
        for(int i =0; i< SUB_NUM; i++){
            subs.add(new Subscriber("sub" + i, "127.0.0.1"));
        }

        // connect sub0 to b1t1, sub1 tob0t1
        subs.get(0).createReceiver("t0", "127.0.0.1:6001");
        subs.get(1).createReceiver("t0", "127.0.0.1:6000");

        Thread.sleep((PHASE1_TIME - DELAY) *1000);

        // enter phase 2: sub0 reconnect back to b0
        subs.get(0).reconnect("t0","127.0.0.1:6001", "127.0.0.1:6000");

        Thread.sleep((PHASE2_TIME + DELAY)*1000);

        subs.get(0).stopReceiver("t0", "127.0.0.1:6000");
        subs.get(1).stopReceiver("t0", "127.0.0.1:6000");

        // get result
        for(Subscriber sub : subs){
            ArrayList<String> buff = sub.getCleanedBuffer(true);
            String id = sub.getMyID();
            for(String elem : buff){
                resLogger.info("{},{}", id, elem);
            }
        }
    }


    static void testAllSubHash() throws Exception{
        int SUB_NUM = 2;
        int PHASE1_TIME = 15;
        int PHASE2_TIME = 15;
        int DELAY = 5;

        ArrayList<Subscriber> subs = new ArrayList<>();
        for(int i =0; i< SUB_NUM; i++){
            subs.add(new Subscriber("sub" + i, "127.0.0.1"));
        }

        // connect subs to b0t1 and b1t1
        for(Subscriber sub : subs){
            sub.createReceiver("t0", "127.0.0.1:6000");
            sub.createReceiver("t0", "127.0.0.1:6001");
        }

        Thread.sleep((PHASE1_TIME) *1000);

        // enter phase 2: disconnect from b1
        for(Subscriber sub : subs){
            sub.stopReceiver("t0", "127.0.0.1:6001");
        }

        Thread.sleep((PHASE2_TIME)*1000);

        for(Subscriber sub : subs){
            sub.stopReceiver("t0", "127.0.0.1:6000");
        }

        // get result
        for(Subscriber sub : subs){
            ArrayList<String> buff = sub.getCleanedBuffer(true);
            String id = sub.getMyID();
            for(String elem : buff){
                resLogger.info("{},{}", id, elem);
            }
        }
    }

    static void testAllPubCreate() throws Exception{
        int SUB_NUM = 3;
        int PHASE1_TIME = 15;
        int PHASE2_TIME = 15;
        int DELAY = 5;

        ArrayList<Subscriber> subs = new ArrayList<>();
        for(int i =0; i< SUB_NUM; i++){
            subs.add(new Subscriber("sub" + i, "127.0.0.1"));
        }

        // connect sub0 to b0, sub1,2 to b1
        subs.get(0).createReceiver("t0","127.0.0.1:6000");
        subs.get(1).createReceiver("t0","127.0.0.1:6001");
        subs.get(2).createReceiver("t0","127.0.0.1:6001");

        Thread.sleep((PHASE1_TIME + DELAY) *1000);

        // enter phase 2: reconnect sub2 to b2
        subs.get(2).reconnect("t0", "127.0.0.1:6001", "127.0.0.1:6002");

        Thread.sleep((PHASE2_TIME - DELAY)*1000);

        subs.get(0).stopReceiver("t0","127.0.0.1:6000");
        subs.get(1).stopReceiver("t0","127.0.0.1:6001");
        subs.get(2).stopReceiver("t0","127.0.0.1:6002");

        // get result
        for(Subscriber sub : subs){
            ArrayList<String> buff = sub.getCleanedBuffer(true);
            String id = sub.getMyID();
            for(String elem : buff){
                resLogger.info("{},{}", id, elem);
            }
        }
    }

    static void testAllSubCreate() throws Exception {
        int SUB_NUM = 3;
        int PHASE1_TIME = 15;
        int PHASE2_TIME = 15;
        int DELAY = 5;

        ArrayList<Subscriber> subs = new ArrayList<>();
        for(int i =0; i< SUB_NUM; i++){
            subs.add(new Subscriber("sub" + i, "127.0.0.1"));
        }

        //connect subs to all two brokers
        for(Subscriber sub : subs){
            sub.createReceiver("t0","127.0.0.1:6000");
            sub.createReceiver("t0","127.0.0.1:6001");
        }

        Thread.sleep((PHASE1_TIME) *1000);

        // enter phase 2: connect all subs to the new broker
        for(Subscriber sub : subs) {
            sub.createReceiver("t0", "127.0.0.1:6002");
        }

        Thread.sleep((PHASE2_TIME)*1000);

        for(Subscriber sub : subs){
            sub.stopReceiver("t0", "127.0.0.1:6000");
            sub.stopReceiver("t0", "127.0.0.1:6001");
            sub.stopReceiver("t0", "127.0.0.1:6002");
        }

        // get result
        for(Subscriber sub : subs){
            ArrayList<String> buff = sub.getCleanedBuffer(true);
            String id = sub.getMyID();
            for(String elem : buff){
                resLogger.info("{},{}", id, elem);
            }
        }
    }

    static void testAllPubClose() throws Exception {
        int SUB_NUM = 3;
        int PHASE1_TIME = 15;
        int PHASE2_TIME = 15;
        int DELAY = 5;

        ArrayList<Subscriber> subs = new ArrayList<>();
        for(int i =0; i< SUB_NUM; i++){
            subs.add(new Subscriber("sub" + i, "127.0.0.1"));
        }

        // connect sub0-b0, sub1-b1, sub2-b2
        subs.get(0).createReceiver("t0","127.0.0.1:6000");
        subs.get(1).createReceiver("t0","127.0.0.1:6001");
        subs.get(2).createReceiver("t0","127.0.0.1:6002");

        Thread.sleep((PHASE1_TIME - DELAY) *1000);

        // enter phase 2: reconnect sub2 to b1
        subs.get(2).reconnect("t0", "127.0.0.1:6002", "127.0.0.1:6001");

        Thread.sleep((PHASE2_TIME + DELAY)*1000);

        subs.get(0).stopReceiver("t0","127.0.0.1:6000");
        subs.get(1).stopReceiver("t0","127.0.0.1:6001");
        subs.get(2).stopReceiver("t0","127.0.0.1:6001");

        // get result
        for(Subscriber sub : subs){
            ArrayList<String> buff = sub.getCleanedBuffer(true);
            String id = sub.getMyID();
            for(String elem : buff){
                resLogger.info("{},{}", id, elem);
            }
        }

    }

    static void testAllSubClose() throws Exception {
        int SUB_NUM = 3;
        int PHASE1_TIME = 15;
        int PHASE2_TIME = 15;
        int DELAY = 5;

        ArrayList<Subscriber> subs = new ArrayList<>();
        for(int i =0; i< SUB_NUM; i++){
            subs.add(new Subscriber("sub" + i, "127.0.0.1"));
        }

        // connect all subs to all three brokers
        for(Subscriber sub : subs){
            sub.createReceiver("t0", "127.0.0.1:6000");
            sub.createReceiver("t0", "127.0.0.1:6001");
            sub.createReceiver("t0", "127.0.0.1:6002");
        }

        Thread.sleep((PHASE1_TIME) *1000);

        // enter phase 2: disconnect all subs from b2
        for(Subscriber sub : subs){
            sub.stopReceiver("t0", "127.0.0.1:6002");
        }

        Thread.sleep((PHASE2_TIME)*1000);

        // get result
        for(Subscriber sub : subs){
            ArrayList<String> buff = sub.getCleanedBuffer(true);
            String id = sub.getMyID();
            for(String elem : buff){
                resLogger.info("{},{}", id, elem);
            }
        }
    }

    static void testHashHash() throws Exception {
        int SUB_NUM = 2;
        int PHASE1_TIME = 15;
        int PHASE2_TIME = 20;
        int DELAY = 5;

        ArrayList<Subscriber> subs = new ArrayList<>();
        for(int i =0; i< SUB_NUM; i++){
            subs.add(new Subscriber("sub" + i, "127.0.0.1"));
        }

        // connect all subs to b0
        for(Subscriber sub : subs){
            sub.createReceiver("t0", "127.0.0.1:6000");
        }

        Thread.sleep((PHASE1_TIME) *1000);

        // connect all subs to b1
        for(Subscriber sub : subs){
            sub.createReceiver("t0", "127.0.0.1:6001");
        }

        Thread.sleep(2*(DELAY) *1000);

        // disconnect all subs from b0
        for(Subscriber sub : subs){
            sub.stopReceiver("t0", "127.0.0.1:6000");
        }

        Thread.sleep((PHASE2_TIME-2*DELAY)*1000);

        // get result
        for(Subscriber sub : subs){
            ArrayList<String> buff = sub.getCleanedBuffer(true);
            String id = sub.getMyID();
            for(String elem : buff){
                resLogger.info("{},{}", id, elem);
            }
        }
    }
}
