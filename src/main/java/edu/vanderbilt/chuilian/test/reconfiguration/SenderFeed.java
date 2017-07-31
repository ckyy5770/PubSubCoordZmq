package edu.vanderbilt.chuilian.test.reconfiguration;

/**
 * Created by Killian on 7/31/17.
 */
public class SenderFeed implements Runnable {
    private String topic;
    private int sendInterval;
    private Publisher pub;
    private boolean stop = false;
    private int msgID = 0;

    public SenderFeed(Publisher pub, String topic, int sendInterval){
        this.pub = pub;
        this.topic = topic;
        this.sendInterval = sendInterval;
    }
    public void run(){
        try{
            while(!stop){
                pub.send(topic, "" + pub.myID + "," + msgID + "," + System.currentTimeMillis());
                msgID++;
                Thread.sleep(sendInterval);
            }
        }catch(InterruptedException e){
            System.out.println("interrupt");
        }

    }
    public void stop(){
        stop = true;
    }
}
