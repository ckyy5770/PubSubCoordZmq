package edu.vanderbilt.chuilian.clients;

import edu.vanderbilt.chuilian.util.DataSender;
import edu.vanderbilt.chuilian.util.DefaultSender;
import edu.vanderbilt.chuilian.util.TopicSenderMap;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


public class Publisher {
	private TopicSenderMap topicSenderMap;
	private ExecutorService executor;

	public Publisher() {
		this.topicSenderMap = null;
		this.executor = Executors.newFixedThreadPool(100);
	}

	public void start() {
		// initialize a default data sender
		// try to get default address, if fail, wait 2 seconds and do it again.
		String defaultAddress;
		while ((defaultAddress = getDefaultAddress()) == null) {
			try {
				TimeUnit.SECONDS.sleep(2);
			} catch (InterruptedException e) {
				throw new IllegalStateException("I'm sleeping! Why interrupt me?!", e);
			}
		}
		// here we get the default receiving address, make a default sender for it, and initialize topic sender map
		this.topicSenderMap = new TopicSenderMap(new DefaultSender(defaultAddress, this.executor));
		this.topicSenderMap.getDefault().start();
	}

	public void send(String topic, String message) {
		// try to get data sender for this topic
		DataSender sender = topicSenderMap.get(topic);
		if (sender == null) {
			// if the sender doesn't exist, get the broker receiver address
			String address = getAddress(topic);
			if (address == null) {
				// if can not get it, send the message through default data sender
				DefaultSender defaultSender = topicSenderMap.getDefault();
				if (sender == null) {
					// this should never happen in a well-designed system
					throw new IllegalStateException("cannot get default sender");
				}
				defaultSender.send(topic, message);
				return;
			} else {
				// successfully get the sender from zookeeper
				// create new sender
				sender = this.topicSenderMap.register(topic, address, this.executor);
				sender.start();
				sender.send(message);
				return;
			}
		} else {
			// successfully get the sender from local list
			sender.send(message);
		}

	}

	/**
	 * get the specific receiver address from zookeeper server
	 *
	 * @param topic
	 * @return null if can not get it
	 */
	private String getAddress(String topic) {
		// TODO: 5/24/17  
		return null;
	}

	/**
	 * get the default receiver address from zookeeper server
	 *
	 * @return
	 */
	private String getDefaultAddress() {
		// TODO: 5/24/17
		return null;
	}


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
