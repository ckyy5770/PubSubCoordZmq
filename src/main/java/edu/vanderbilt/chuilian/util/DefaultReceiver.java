package edu.vanderbilt.chuilian.util;

import org.zeromq.ZMsg;

import java.util.concurrent.ExecutorService;

/**
 * Created by Killian on 5/24/17.
 */
public class DefaultReceiver extends DataReceiver {
    public DefaultReceiver(String address, MsgBufferMap msgBufferMap, ExecutorService executor) {
        super("", address, msgBufferMap, executor);
    }

    @Override
    // all message received by default receiver are stored in topic "", may need to change in the future
    public void receive() {
        ZMsg receivedMsg = ZMsg.recvMsg(this.recSocket);
        msgBuffer.add(receivedMsg);
        {
            //debug
            System.out.println("Message Received (from default receiver):");
            System.out.println(new String(receivedMsg.getFirst().getData()));
            //System.out.println(DataSampleHelper.deserialize(receivedMsg.getLast().getData()).sampleId());
            System.out.println(new String(receivedMsg.getLast().getData()));
        }
    }
}
