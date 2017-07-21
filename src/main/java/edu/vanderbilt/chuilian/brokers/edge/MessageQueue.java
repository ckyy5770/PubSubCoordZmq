package edu.vanderbilt.chuilian.brokers.edge;

import edu.vanderbilt.chuilian.types.DataSample;
import edu.vanderbilt.chuilian.types.DataSampleHelper;
import edu.vanderbilt.chuilian.util.PriorityFuture;
import org.zeromq.ZMsg;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * Created by Killian on 6/8/17.
 */
public class MessageQueue {
    //private final ArrayList<ZMsg> msgArrayList = new ArrayList<>();
    //private final PriorityBlockingQueue<ZMsg> msgPQ = new PriorityBlockingQueue<>(1024, new ZMsgComparator());
    private final PriorityQueue<ZMsg> msgPQ = new PriorityQueue<>(1024, new ZMsgComparator());
    private int nextSendingMsg = 0;

    public MessageQueue() {
    }

    public void add(ZMsg msg) {
        //msgPQ.put(msg);
        msgPQ.offer(msg);
    }

    public ZMsg getNextMsg() throws Exception{
        //return msgPQ.take();
        return msgPQ.poll();
    }

    public int getSize(){
        return msgPQ.size();
    }

    public class ZMsgComparator implements Comparator<ZMsg> {
        public int compare(ZMsg o1, ZMsg o2) {
            if (o1 == null && o2 == null)
                return 0;
            else if (o1 == null)
                return 1;
            else if (o2 == null)
                return -1;
            else {
                byte[] o1Content = o1.getLast().getData();
                byte[] o2Content = o2.getLast().getData();

                int p1 = DataSampleHelper.deserialize(o1Content).priority();
                int p2 = DataSampleHelper.deserialize(o2Content).priority();

                return p1 > p2 ? -1 : (p1 == p2 ? 0 : 1);
            }
        }
    }

    public static void main(String[] args) throws Exception {
    }
}
