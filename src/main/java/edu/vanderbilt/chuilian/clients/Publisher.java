package edu.vanderbilt.chuilian.clients;

import edu.vanderbilt.chuilian.util.*;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


public class Publisher {
	private TopicSenderMap topicSenderMap;
	private final ExecutorService executor;
	private final MsgBufferMap msgBufferMap;
	private final ZkConnect zkConnect;

	public Publisher() {
		this.topicSenderMap = null;
		this.executor = Executors.newFixedThreadPool(100);
		this.zkConnect = new ZkConnect();
		this.msgBufferMap = new MsgBufferMap();
	}

	public void start() throws Exception {
		// start zookeeper client
		this.zkConnect.connect("127.0.0.1:2181");
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
		this.topicSenderMap = new TopicSenderMap(new DefaultSender(defaultAddress, this.msgBufferMap, this.executor, this.zkConnect));
		this.topicSenderMap.getDefault().start();
	}

	public void send(String topic, String message) throws Exception {
		// try to get data sender for this topic
		DataSender sender = topicSenderMap.get(topic);
		if (sender == null) {
			// if the sender doesn't exist, get the broker receiver address
			String address = getAddress(topic);
			if (address == null) {
				// if can not get it, send the message through default data sender
				DefaultSender defaultSender = topicSenderMap.getDefault();
				if (defaultSender == null) {
					// this should never happen in a well-designed system
					throw new IllegalStateException("cannot get default sender");
				}
				defaultSender.send(topic, message);
				return;
			} else {
				// successfully get the sender from zookeeper
				// create new sender
				sender = this.topicSenderMap.register(topic, address, this.msgBufferMap, this.executor, this.zkConnect);
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
	private String getAddress(String topic) throws Exception {
		String address = zkConnect.getNodeData("/topics/" + topic + "/pub");
		return address;
	}

	/**
	 * get the default receiver address from zookeeper server
	 *
	 * @return
	 */
	private String getDefaultAddress() throws Exception {
		String data = zkConnect.getNodeData("/topics");
		if (data == null) return null;
		String[] addresses = data.split("\n");
		if (addresses[0] == "null") return null;
		return addresses[0];
	}


	public static void main(String args[]) throws Exception {
		Publisher pub = new Publisher();
		pub.start();
		while (true) {
			for (int i = 0; i < 5; i++) {
				pub.send("topic1", Integer.toString(i));
				Thread.sleep(100);
			}
			Thread.sleep(1000);
			for (int i = 0; i < 5; i++) {
				pub.send("topic2", Integer.toString(i));
				Thread.sleep(100);
			}
			Thread.sleep(1000);
			for (int i = 0; i < 5; i++) {
				pub.send("topic3", Integer.toString(i));
				Thread.sleep(100);
			}
		}
		/*
		Thread.sleep(1000);
		for(int i=0; i<10; i++){
			pub.send("topic1", Integer.toString(i));
			Thread.sleep(100);
		}
		Thread.sleep(1000);
		for(int i=0; i<10; i++){
			pub.send("topic2", Integer.toString(i));
			Thread.sleep(100);
		}
		return;
		*/
		/*
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
		*/
	}
}
