package edu.vanderbilt.chuilian.brokers.edge;

import org.zeromq.ZMsg;

import java.util.ArrayList;

/**
 * Created by Killian on 6/8/17.
 */
public class MessageQueue {
    private final ArrayList<ZMsg> msgArrayList = new ArrayList<>();
    private int nextSendingMsg = 0;

    public MessageQueue() {
    }

    public void add(ZMsg msg) {
        msgArrayList.add(msg);
    }

    public ZMsg getNextMsg() {
        if (nextSendingMsg >= msgArrayList.size()) return null;
        return msgArrayList.get(nextSendingMsg++);
    }
}
