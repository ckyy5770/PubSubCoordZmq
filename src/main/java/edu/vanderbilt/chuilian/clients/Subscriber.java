package edu.vanderbilt.chuilian.clients;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;

public class Subscriber {
	
	public static void main(String args[]){
		Context context= ZMQ.context(1);
		Socket subscriber= context.socket(ZMQ.SUB);	
		subscriber.connect("tcp://localhost:5556");
		subscriber.subscribe("alerts".getBytes());

		while(true){
			ZMsg receivedMsg= ZMsg.recvMsg(subscriber);
			System.out.println(new String(receivedMsg.getFirst().getData()));
			//System.out.println(DataSampleHelper.deserialize(receivedMsg.getLast().getData()).sampleId());
			System.out.println(new String(receivedMsg.getLast().getData()));
		}

	}

}
