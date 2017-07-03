package edu.vanderbilt.chuilian.clients.subscriber;

import edu.vanderbilt.chuilian.types.DataSample;
import edu.vanderbilt.chuilian.types.DataSampleHelper;
import edu.vanderbilt.chuilian.util.MsgBuffer;
import edu.vanderbilt.chuilian.util.MsgBufferMap;
import edu.vanderbilt.chuilian.util.UtilMethods;
import edu.vanderbilt.chuilian.util.ZkConnect;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMsg;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.*;

public class Subscriber {
	private String ip = "127.0.0.1";
	private String zkAddress;
	private TopicReceiverMap topicReceiverMap;
	private TopicWaiterMap topicWaiterMap;
	private final MsgBufferMap msgBufferMap;
	private final ExecutorService receiverExecutor;
	private final ExecutorService waiterExecutor;
	private final ExecutorService processorExecutor;
	private final ZkConnect zkConnect;
	private Future<?> processorFuture;
	private static final Logger logger = LogManager.getLogger(Subscriber.class.getName());

	public Subscriber() {
		// get zookeeper server address
		this.zkAddress = UtilMethods.getZookeeperAddress();
		this.ip = UtilMethods.getIPaddress();
		this.topicReceiverMap = null;
		this.topicWaiterMap = new TopicWaiterMap();
		this.msgBufferMap = new MsgBufferMap();
		this.receiverExecutor = Executors.newFixedThreadPool(100);
		this.waiterExecutor = Executors.newFixedThreadPool(100);
		this.processorExecutor = Executors.newFixedThreadPool(1);
		this.zkConnect = new ZkConnect();
	}

	public void start() throws Exception {
		// start zookeeper client
        zkConnect.connect(zkAddress);
        // initialize a default data receiverFromLB
        // try to get default address, if fail, wait 2 seconds and do it again.
        String defaultAddress;
        while ((defaultAddress = getDefaultAddress()) == null) {
			try {
				TimeUnit.SECONDS.sleep(2);
			} catch (InterruptedException e) {
				throw new IllegalStateException("I'm sleeping! Why interrupt me?!", e);
			}
		}
        // here we get the default sending address, make a default receiverFromLB for it, and initialize topic receiverFromLB map
        topicReceiverMap = new TopicReceiverMap(new DefaultReceiver(defaultAddress, this.msgBufferMap, this.receiverExecutor, this.zkConnect, this.ip));
        topicReceiverMap.getDefault().start();
        // this thread will constantly (3s) check MsgBuffer and process msgs
		processorFuture = processorExecutor.submit(() -> {
			while (true) {
				Thread.sleep(3000);
				processor();
			}
		});
	}

	public void subscribe(String topic) throws Exception {
		logger.info("Subscribe topic: {}", topic);
		// if the topic is already subscribed, do nothing
		if (topicReceiverMap.get(topic) != null || topicWaiterMap.get(topic) != null) return;
        // subscribe it for the default receiverFromLB
        topicReceiverMap.getDefault().subscribe(topic);
        // try to get the broker sender address for this topic, if not found, create a waiter to waiting for channel to be created
        String address;
		if ((address = getAddress(topic)) == null) {
			logger.info("Fail to get message channel for topic {}, creating a waiter for this topic.", topic);
			Waiter newWaiter = topicWaiterMap.register(topic, this.msgBufferMap, this.waiterExecutor, this.receiverExecutor, this.topicReceiverMap, this.zkConnect, this.ip);
			newWaiter.start();
			return;
		}
        // here we successfully get the broker sender address for this topic, create a data receiverFromLB for it.
        DataReceiver newReceiver = topicReceiverMap.register(topic, address, this.msgBufferMap, this.receiverExecutor, this.zkConnect, this.ip);
        newReceiver.start();
    }

	public void unsubscribe(String topic) throws Exception {
		logger.info("Unsubscribe topic: {}", topic);
        // unsubscribe it for the default receiverFromLB
        topicReceiverMap.getDefault().unsubscribe(topic);
        // check if the topic is on receiverFromLB map
        DataReceiver receiver = topicReceiverMap.get(topic);
        if (receiver != null) {
            // stop the receiverFromLB thread, get unprocessed messages
            MsgBuffer unprocessedMsg = topicReceiverMap.get(topic).stop();
            processBuffer(unprocessedMsg);
            // unregister from topic receiverFromLB map
            topicReceiverMap.unregister(topic);
        } else {
            // check if it is on waiter map
			Waiter waiter = topicWaiterMap.get(topic);
			if (waiter != null) {
				// stop the waiter thread, waiter will automatically unregister itself from topic-waiter map.
				waiter.stop();
			}
		}
	}

	/**
     * shutdown all data receiverFromLB including default sender.
     */
    public void close() throws Exception {
        // shutdown processor thread first
        processorFuture.cancel(false);
        // shutdown default receiverFromLB
        topicReceiverMap.getDefault().stop();
        // iterate through the map, shutdown every single receiverFromLB.
        for (Map.Entry<String, DataReceiver> entry : topicReceiverMap.entrySet()) {
            unsubscribe(entry.getKey());
        }
		// iterate through the map, shutdown every single waiter.
		for (Map.Entry<String, Waiter> entry : topicWaiterMap.entrySet()) {
			entry.getValue().stop();
		}
		// reset topicReceiverMap and topicWaiterMap
		topicReceiverMap = null;
		topicWaiterMap = null;
		// turn off executor
		waiterExecutor.shutdownNow();
		receiverExecutor.shutdownNow();
		processorExecutor.shutdownNow();
		// shutdown zookeeper client
        zkConnect.close();
    }

	/**
	 * a worker that loop through buffer map and process each messages in each msgbuffer
	 */
	private void processor() {
		// iterate through the map, process every buffer
        for (Map.Entry<String, MsgBuffer> entry : msgBufferMap.entrySet()) {
            // create a new empty buffer for this entry
			MsgBuffer buff = new MsgBuffer(entry.getKey());
			// swap the new empty buffer with old buffer
			// TODO: 5/24/17 here may need lock
			entry.getValue().swap(buff);
			// then we process messages in this old buffer
            processBuffer(buff);
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
		String msgTopic = new String(msg.getFirst().getData());
		byte[] msgContent = msg.getLast().getData();
		DataSample sample = DataSampleHelper.deserialize(msgContent);
		logger.debug("Message Processed by subscriber. Topic: {} ID: {}", msgTopic, sample.sampleId());
	}

	/**
	 * get the specific sender address from zookeeper server
	 *
	 * @param topic
	 * @return null if can not get it
	 */
	// TODO: 6/13/17 randomly picked from current available brokers for now, this should be changed in the next step.
	private String getAddress(String topic) throws Exception {
		String data = zkConnect.getNodeData("/topics/" + topic + "/sub");
		if (data == null) return null;
		String[] addresses = data.split("\n");
		if (addresses[0].length() == 0 || addresses[0].equals("null")) return null;
		int numOfAddresses = addresses.length;
		int randomNum = ThreadLocalRandom.current().nextInt(0, numOfAddresses);
		return addresses[randomNum];
	}

	/**
	 * get one default sender address (randomly picked from current available brokers) from zookeeper server
	 *
	 * @return
	 */
	private String getDefaultAddress() throws Exception {
		String data = zkConnect.getNodeData("/topics");
		if (data == null) return null;
		String[] addresses = data.split("\n");
		if (addresses[0].length() == 0 || addresses[0].equals("null")) return null;
		int numOfAddresses = addresses.length;
		int randomNum = ThreadLocalRandom.current().nextInt(0, numOfAddresses);
		return addresses[randomNum].split(",")[1];
	}

	public static void main(String args[]) throws Exception {
		Subscriber sub = new Subscriber();
		sub.start();
		sub.subscribe("topic1");
		Thread.sleep(200*1000);
		sub.close();

		// correctness test
		/*
		Subscriber sub = new Subscriber();
		sub.start();
		sub.subscribe("topic1");
		sub.subscribe("topic2");
		sub.subscribe("topic3");
		Thread.sleep(20000);
		sub.unsubscribe("topic1");
		Thread.sleep(10000);
		sub.subscribe("topic4");
		Thread.sleep(10000);
		sub.close();
		*/
	}

}
