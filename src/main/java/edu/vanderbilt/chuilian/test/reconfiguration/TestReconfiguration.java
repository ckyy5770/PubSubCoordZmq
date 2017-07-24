package edu.vanderbilt.chuilian.test.reconfiguration;

import com.sun.corba.se.pept.broker.Broker;
import edu.vanderbilt.chuilian.loadbalancer.LoadBalancer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zmq.Pub;

import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;

/**
 * Created by Killian on 7/24/17.
 */

public class TestReconfiguration {
    private static final Logger logger = LogManager.getLogger(TestReconfiguration.class.getName());
    private static final Logger resLogger = LogManager.getLogger("TestResult");
    public static void main(String[] args) throws InterruptedException{
        int PUB_NUM = 2;
        int SUB_NUM = 2;
        int BROKER_NUM = 1;
        int MSG_NUM = 100;

        ArrayList<Publisher> pubs = new ArrayList<>();
        for(int i =0; i< PUB_NUM; i++){
            pubs.add(new Publisher("pub" + i, "127.0.0.1"));
        }

        ArrayList<Subscriber> subs = new ArrayList<>();
        for(int i =0; i< SUB_NUM; i++){
            subs.add(new Subscriber("sub" + i, "127.0.0.1"));
        }

        ArrayList<EdgeBroker> brokers = new ArrayList<>();
        for(int i =0; i< BROKER_NUM; i++){
            brokers.add(new EdgeBroker("broker" + i, "127.0.0.1"));
        }

        // create a channel of topic "t0" in broker 0
        brokers.get(0).createChannel("t0", Integer.toString(5000), Integer.toString(5001));

        // connect subs to b0t1
        for(Subscriber sub : subs){
            sub.createReceiver("t0", "127.0.0.1:5001");
        }

        // connect pubs to b0t1
        for(Publisher pub : pubs){
            pub.createSender("t0", "127.0.0.1:5000");
        }

        // wait for connection to establish
        Thread.sleep(2000);

        // send messages
        for(int i=0; i< MSG_NUM; i++){
            for(int j=0; j<PUB_NUM; j++){
                pubs.get(j).send("t0", "pub" + j + "," + i);
            }
        }

        // wait for message to be transmitted
        Thread.sleep(2000);

        // stop all
        for(Publisher pub : pubs){
            pub.stopSender("t0", "127.0.0.1:5000");
        }

        for(Subscriber sub : subs){
            sub.stopReceiver("t0", "127.0.0.1:5001");
        }

        for(EdgeBroker broker : brokers){
            broker.closeChannel("t0");
        }

        // get result
        for(Subscriber sub : subs){
            BlockingQueue<String> buff = sub.getMsgBuffer();
            String id = sub.getMyID();
            while(!buff.isEmpty()){
                resLogger.info("{},{}", id, buff.take());
            }
        }
    }
}
