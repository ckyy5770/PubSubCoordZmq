package edu.vanderbilt.chuilian.clients.subscriber;

import edu.vanderbilt.chuilian.types.DataSample;
import edu.vanderbilt.chuilian.types.DataSampleHelper;
import edu.vanderbilt.chuilian.util.MsgBuffer;
import edu.vanderbilt.chuilian.util.MsgBufferMap;
import edu.vanderbilt.chuilian.util.ZkConnect;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMsg;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class Subscriber {
	// TODO: 5/25/17 hard coded ip 
	private final String ip = "127.0.0.1";
	private TopicReceiverMap topicReceiverMap;
	private final MsgBufferMap msgBufferMap;
	private final ExecutorService executor;
	private final ZkConnect zkConnect;
	private Future<?> processorFuture;
	private static final Logger logger = LogManager.getLogger(Subscriber.class.getName());

	public Subscriber() {
		this.topicReceiverMap = null;
		this.msgBufferMap = new MsgBufferMap();
		this.executor = Executors.newFixedThreadPool(100);
		this.zkConnect = new ZkConnect();
	}

	public void start() throws Exception {
		// start zookeeper client
        zkConnect.connect("127.0.0.1:2181");
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
        topicReceiverMap = new TopicReceiverMap(new DefaultReceiver(defaultAddress, this.msgBufferMap, this.executor, this.zkConnect));
        topicReceiverMap.getDefault().start();
        // this thread will constantly (3s) check MsgBuffer and process msgs
        processorFuture = executor.submit(() -> {
            while (true) {
				Thread.sleep(3000);
				processor();
			}
		});
	}

	public void subscribe(String topic) throws Exception {
		logger.info("Subscribe topic: {}", topic);
		// if the topic is already subscribed, do nothing
        if (topicReceiverMap.get(topic) != null) return;
		// subscribe it for the default receiver
		topicReceiverMap.getDefault().subscribe(topic);
		// try to get the broker sender address for this topic, if not found, throw a topic not found exception
		String address;
		// TODO: 5/30/17 this exception should be properly handled
		if ((address = getAddress(topic)) == null) throw new RuntimeException("topic not found");
		// here we successfully get the broker sender address for this topic, create a data receiver for it.
        DataReceiver newReceiver = topicReceiverMap.register(topic, address, this.msgBufferMap, this.executor, this.zkConnect);
        newReceiver.start();
	}

	public void unsubscribe(String topic) throws Exception {
		logger.info("Unsubscribe topic: {}", topic);
		// unsubscribe it for the default receiver
		topicReceiverMap.getDefault().unsubscribe(topic);
		// stop the receiver thread, get unprocessed messages
        MsgBuffer unprocessedMsg = topicReceiverMap.get(topic).stop();
        processBuffer(unprocessedMsg);
        // unregister from topic receiver map
        topicReceiverMap.unregister(topic);
    }

	/**
	 * shutdown all data receiver including default sender.
	 */
	public void close() throws Exception {
		// shutdown processor thread first
        processorFuture.cancel(false);
        // shutdown default receiver
        topicReceiverMap.getDefault().stop();
        // iterate through the map, shutdown every single receiver.
        for (Map.Entry<String, DataReceiver> entry : topicReceiverMap.entrySet()) {
            unsubscribe(entry.getKey());
        }
		// reset topicReceiverMap
        topicReceiverMap = null;
        // turn off executor
        executor.shutdownNow();
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
		sub.subscribe("topic2");
		sub.subscribe("topic3");
		Thread.sleep(10000);
		sub.unsubscribe("topic1");
		Thread.sleep(10000);
		sub.close();
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
