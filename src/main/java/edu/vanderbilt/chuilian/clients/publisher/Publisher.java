package edu.vanderbilt.chuilian.clients.publisher;

import edu.vanderbilt.chuilian.types.DataSampleHelper;
import edu.vanderbilt.chuilian.util.MsgBufferMap;
import edu.vanderbilt.chuilian.util.UtilMethods;
import edu.vanderbilt.chuilian.util.ZkConnect;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;


public class Publisher {
    private String ip;
	private TopicSenderMap topicSenderMap;
	private final ExecutorService executor;
	private final MsgBufferMap msgBufferMap;
	private final ZkConnect zkConnect;
	private String zkAddress;
	private static final Logger logger = LogManager.getLogger(Publisher.class.getName());

	public Publisher() {
        // get zookeeper server address
        this.zkAddress = UtilMethods.getZookeeperAddress();
	    this.ip = UtilMethods.getIPaddress();
		this.topicSenderMap = null;
		this.executor = Executors.newFixedThreadPool(100);
		this.zkConnect = new ZkConnect();
		this.msgBufferMap = new MsgBufferMap();
	}

	public void start() throws Exception {
		// start zookeeper client
		zkConnect.connect(zkAddress);
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
		topicSenderMap = new TopicSenderMap(new DefaultSender(defaultAddress, this.msgBufferMap, this.executor, this.zkConnect, this.ip));
		topicSenderMap.getDefault().start();
	}

	public void send(String topic, byte[] message) throws Exception {
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
				sender = topicSenderMap.register(topic, address, this.msgBufferMap, this.executor, this.zkConnect, this.ip);
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
	 * register a topic through default channel, and create a new sender for it, this method is just for test.
	 * @param topic
	 */
	public void register(String topic) throws Exception{
		// already registered
		logger.info("registering topic: {}", topic);
		if(topicSenderMap.get(topic) != null) return;

		DefaultSender defaultSender = topicSenderMap.getDefault();
		if (defaultSender == null) {
			// this should never happen in a well-designed system
			throw new IllegalStateException("cannot get default sender");
		}
		logger.info("sending a register msg to default broker");

		//defaultSender.send(topic, DataSampleHelper.serialize(-1, 1, 1, 0, -1, 10));

		boolean isCreated = false;
		while(!isCreated){
			// register channel
			defaultSender.send(topic, DataSampleHelper.serialize(-1, 1, 1, 0, -1, 10));
			logger.debug("waiting for new channel to be created");
			// wait for new channel to be created
			Thread.sleep(5000);
			logger.debug("trying to get sending address from zookeeper");
			// try to get sending from zookeeper
			String address = getAddress(topic);
			if(address == null) continue;
			isCreated = true;
			logger.debug("got it, starting new sender");
			DataSender sender = topicSenderMap.register(topic, address, this.msgBufferMap, this.executor, this.zkConnect, this.ip);
			sender.start();
		}
	}

	/**
	 * shutdown the sender corresponding to the topic, if the sender does not exist, simply do nothing
	 *
	 * @param topic
	 */
	public void stop(String topic) throws Exception {
		DataSender sender = topicSenderMap.get(topic);
		if (sender != null) {
			sender.stop();
		}
		topicSenderMap.unregister(topic);
	}

	/**
	 * shutdown all data sender including default sender.
	 */
	public void close() throws Exception {
		// shutdown default sender first, so that no more new sender will be created
		topicSenderMap.getDefault().stop();
		// iterate through the map, shutdown every single sender.
		for (Map.Entry<String, DataSender> entry : topicSenderMap.entrySet()) {
			stop(entry.getKey());
		}
		// reset topicSenderMap
		topicSenderMap = null;
		// turn off executor
		executor.shutdownNow();
		// shutdown zookeeper client
		zkConnect.close();
	}

	/**
	 * get the specific receiver address from zookeeper server
	 *
	 * @param topic
	 * @return null if can not get it
	 */
    // TODO: 6/13/17 randomly picked from current available brokers for now, this should be changed in the next step.
    private String getAddress(String topic) throws Exception {
        String data = zkConnect.getNodeData("/topics/" + topic + "/pub");
        if (data == null) return null;
        String[] addresses = data.split("\n");
        if (addresses[0].length() == 0 || addresses[0].equals("null")) return null;
        int numOfAddresses = addresses.length;
        int randomNum = ThreadLocalRandom.current().nextInt(0, numOfAddresses);
        return addresses[randomNum];
    }

	/**
     * get one default receiver address (randomly picked from current available brokers) from zookeeper server
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
        return addresses[randomNum].split(",")[0];
    }


	/**
	 *
	 * @param args "topic1 topic2 topic3 ..."
	 * @throws Exception
	 */
	public static void main(String args[]) throws Exception {
		Publisher pub = new Publisher();
		pub.start();
		for(String arg : args){
			pub.register(arg);
		}
		Thread.sleep(6 * 60 * 1000);
		pub.close();

		// correctness test
		/*
		int counter = 0;
		Publisher pub = new Publisher();
		pub.start();
		for (int i = 0; i < 40; i++) {
			pub.send("topic1", DataSampleHelper.serialize(counter++, 1, 1, 0, 11111, 10));
			pub.send("topic2", DataSampleHelper.serialize(counter++, 1, 1, 0, 11111, 10));
			pub.send("topic3", DataSampleHelper.serialize(counter++, 1, 1, 0, 11111, 10));
			Thread.sleep(500);
		}
		pub.stop("topic1");
		for (int i = 0; i < 40; i++) {
			pub.send("topic2", DataSampleHelper.serialize(counter++, 1, 1, 0, 11111, 10));
			pub.send("topic3", DataSampleHelper.serialize(counter++, 1, 1, 0, 11111, 10));
			Thread.sleep(500);
		}
		pub.close();
		*/
	}
}
