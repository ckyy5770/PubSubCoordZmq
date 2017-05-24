package edu.vanderbilt.chuilian.clients;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;


public class Publisher {
	public static void main(String args[]) throws InterruptedException{
		Context context= ZMQ.context(1);
		Socket publisher= context.socket(ZMQ.PUB);
		publisher.connect("tcp://localhost:5555");
		Thread.sleep(1000);
		for(int i=0;i<100;i++){
			publisher.sendMore("alerts");
			//publisher.send(DataSampleHelper.serialize(i, 1, 1, 0, 11111, 10),0,0,0);
			publisher.send("hello world " +i);
			System.out.println("sent msg:"+i);
			Thread.sleep(100);
		}
		publisher.close();
		context.term();
	}
}
