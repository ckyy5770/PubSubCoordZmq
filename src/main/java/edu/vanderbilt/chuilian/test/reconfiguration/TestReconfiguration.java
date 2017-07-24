package edu.vanderbilt.chuilian.test.reconfiguration;

import zmq.Pub;

import java.util.ArrayList;

/**
 * Created by Killian on 7/24/17.
 */
public class TestReconfiguration {
    public static void main(String[] args){
        int PUB_NUM = 2;
        int SUB_NUM = 2;
        int BROKER_NUM = 1;
        int MSG_NUM = 1000;

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
        for(int i =0; i< SUB_NUM; i++){
            subs.get(i).createReceiver("t0", "127.0.0.1:5001");
        }

        // connect pubs to b0t1
        for(int i =0; i< PUB_NUM; i++){
            pubs.get(i).createSender("t0", "127.0.0.1:5000");
        }

        // send messages
        for(int i=0; i< MSG_NUM; i++){
            for(int j=0; j<PUB_NUM; j++){
                pubs.get(j).send("t0", "pub" + j + "," + i);
            }
        }

    }
}
