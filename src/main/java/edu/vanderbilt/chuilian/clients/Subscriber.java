package edu.vanderbilt.chuilian.clients;

import edu.vanderbilt.chuilian.util.*;
import org.zeromq.ZMsg;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Subscriber {
	// TODO: 5/25/17 hard coded ip 
	private final String ip = "127.0.0.1";
	private TopicReceiverMap topicReceiverMap;
	private final MsgBufferMap msgBufferMap;
	private final ExecutorService executor;
	private final ZkConnect zkConnect;

	public Subscriber() {
		this.topicReceiverMap = null;
		this.msgBufferMap = new MsgBufferMap();
		this.executor = Executors.newFixedThreadPool(100);
		this.zkConnect = new ZkConnect();
	}

	public void start() throws Exception {
		// start zookeeper client
		this.zkConnect.connect("127.0.0.1:2181");
		// initialize a default data receiver
		// try to get default address, if fail, wait 2 seconds and do it again.
		String defaultAddress;
		while ((defaultAddress = getDefaultAddress()) == null) {
			try {
				TimeUnit.SECONDS.sleep(2);
			} catch (InterruptedException e) {
				throw new IllegalStateException("I'm sleeping! Why interrupt me?!", e);
			}
		}
		// here we get the default sending address, make a default receiver for it, and initialize topic receiver map
		this.topicReceiverMap = new TopicReceiverMap(new DefaultReceiver(defaultAddress, this.msgBufferMap, this.executor, this.zkConnect));
		this.topicReceiverMap.getDefault().start();
		// this thread will constantly (10s) check MsgBuffer and process msgs
		executor.submit(() -> {
			while (true) {
				Thread.sleep(10000);
				processor();
			}
		});
	}

	public void subscribe(String topic) throws Exception {
		{
			// debug
			System.out.println("Subscribe topic: " + topic);
		}
		// if the topic is already subscribed, do nothing
		if (this.topicReceiverMap.get(topic) != null) return;
		// try to get the broker sender address for this topic, if not found, throw a topic not found exception
		String address;
		if ((address = getAddress(topic)) == null) throw new RuntimeException("topic not found");
		// here we successfully get the broker sender address for this topic, create a data receiver for it.
		DataReceiver newReceiver = this.topicReceiverMap.register(topic, address, this.msgBufferMap, this.executor, this.zkConnect);
		newReceiver.start();
	}

	public void unsubscribe(String topic) throws Exception {
		{
			// debug
			System.out.println("Unsubscribe topic: " + topic);
		}
		// stop the receiver thread, get unprocessed messages
		MsgBuffer unprocessedMsg = this.topicReceiverMap.get(topic).stop();
		this.processBuffer(unprocessedMsg);
		// unregister from topic receiver map
		this.topicReceiverMap.unregister(topic);
	}

	/**
	 * shutdown all data receiver including default sender.
	 */
	public void close() throws Exception {
		// shutdown default sender first, so that no more new receiver will be created
		this.topicReceiverMap.getDefault().stop();
		// iterate through the map, shutdown every single receiver.
		for (Map.Entry<String, DataReceiver> entry : this.topicReceiverMap.entrySet()) {
			entry.getValue().stop();
		}
		// reset topicReceiverMap
		this.topicReceiverMap = null;
		// shutdown zookeeper client
		this.zkConnect.close();
	}

	/**
	 * a worker that loop through buffer map and process each messages in each msgbuffer
	 */
	private void processor() {
		// iterate through the map, process every buffer
		for (Map.Entry<String, MsgBuffer> entry : this.msgBufferMap.entrySet()) {
			// create a new empty buffer for this entry
			MsgBuffer buff = new MsgBuffer(entry.getKey());
			// swap the new empty buffer with old buffer
			// TODO: 5/24/17 here may need lock
			entry.getValue().swap(buff);
			// then we process messages in this old buffer
			this.processBuffer(buff);
		}
	}

	private void processBuffer(MsgBuffer buff) {
		if (buff == null) return;
		Iterator<ZMsg> iter = buff.iterator();
		while (iter.hasNext()) {
			processMsg(iter.next());
		}
	}

	private void processMsg(ZMsg msg) {
		System.out.println("Processed Message:");
		System.out.println(new String(msg.getFirst().getData()));
		//System.out.println(DataSampleHelper.deserialize(receivedMsg.getLast().getData()).sampleId());
		System.out.println(new String(msg.getLast().getData()));
	}

	/**
	 * get the specific receiver address from zookeeper server
	 *
	 * @param topic
	 * @return null if can not get it
	 */
	private String getAddress(String topic) throws Exception {
		String address = zkConnect.getNodeData("/topics/" + topic + "/sub");
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
		if (addresses[1] == "null") return null;
		return addresses[1];
	}

	public static void main(String args[]) throws Exception {
		Subscriber sub = new Subscriber();
		sub.start();
		sub.subscribe("topic1");
		Thread.sleep(10000);
		sub.subscribe("topic2");
		sub.subscribe("topic3");
		sub.unsubscribe("topic1");
		/*
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
		*/
	}

}
