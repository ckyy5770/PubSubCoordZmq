package edu.vanderbilt.chuilian.util;

import org.zeromq.ZMsg;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

/**
 * Created by Killian on 5/24/17.
 */
public class MsgBuffer {
    private String topic; // topic associate with this buffer
    private Queue<ZMsg> buff;

    public MsgBuffer(String topic) {
        this.topic = topic;
        this.buff = new LinkedList<>();
    }

    public void add(ZMsg message) {
        this.buff.add(message);
    }

    public void swap(MsgBuffer newBuff) {
        Queue<ZMsg> temp = this.buff;
        this.buff = newBuff.buff;
        newBuff.buff = temp;
    }

    public Iterator<ZMsg> iterator() {
        return this.buff.iterator();
    }
}
