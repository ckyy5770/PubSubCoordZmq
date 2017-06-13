package edu.vanderbilt.chuilian.loadbalancer;

/**
 * Created by Killian on 6/2/17.
 */

import edu.vanderbilt.chuilian.loadbalancer.consistenthashing.ConsistentHashingMap;
import edu.vanderbilt.chuilian.loadbalancer.plan.*;
import edu.vanderbilt.chuilian.types.TypesBrokerReport;
import edu.vanderbilt.chuilian.types.TypesBrokerReportHelper;
import edu.vanderbilt.chuilian.types.TypesPlanHelper;
import edu.vanderbilt.chuilian.util.ZkConnect;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * This is load balancer node
 * Load balancer will collect load reports from every single broker and analyze them, decide whether it should generate a new plan
 */
public class LoadBalancer {
    Plan currentPlan;
    private final ConsistentHashingMap consistentHashingMap;
    private final BrokerLoadReportBuffer brokerLoadReportBuffer;
    private final ExecutorService executor;
    private final ZkConnect zkConnect;
    /**
     * receiverFromLB socket will receive load reports from all local load analyzer, residing in each broker
     * sender socket will send new plans to all dispatcher, residing in each broker
     */
    private ZMQ.Context sendContext;
    private ZMQ.Socket sendSocket;
    private ZMQ.Context recContext;
    private ZMQ.Socket recSocket;
    // TODO: 6/2/17 hard coded port, port 25001 is receiverFromLB port, port 25002 is sender port
    private int recPort = 25001;
    private int sendPort = 25002;
    // TODO: 5/25/17 hard coded ip for now
    String ip = "127.0.0.1";

    Future<?> receiverFuture;
    Future<?> processorFuture;

    private static final Logger logger = LogManager.getLogger(LoadBalancer.class.getName());

    public LoadBalancer() {
        this.brokerLoadReportBuffer = new BrokerLoadReportBuffer();
        this.consistentHashingMap = new ConsistentHashingMap();
        this.executor = Executors.newFixedThreadPool(3);
        this.zkConnect = new ZkConnect();
        // initialize socket
        this.recContext = ZMQ.context(1);
        this.recSocket = this.recContext.socket(ZMQ.SUB);
        this.sendContext = ZMQ.context(1);
        this.sendSocket = this.sendContext.socket(ZMQ.PUB);
        // initialize plan
        this.currentPlan = new Plan();
    }

    public void start() throws Exception {
        // start listening to receiverFromLB port
        recSocket.bind("tcp://*:" + recPort);
        // subscribe all topic
        recSocket.subscribe("".getBytes());
        // start connecting to sending port
        sendSocket.bind("tcp://*:" + sendPort);
        // start zookeeper client
        zkConnect.connect("127.0.0.1:2181");
        // clear the data tree
        zkConnect.resetServer();
        // register itself to zookeeper service
        zkConnect.registerBalancer(ip + ":" + recPort, ip + ":" + sendPort);

        logger.info("LoadBalancer started. ip {} recPort {} sendPort {}", ip, recPort, sendPort);

        // start receiving reports and updating channel stats on each broker
        receiverFuture = executor.submit(() -> {
            logger.info("LoadBalancer receiver Thread Started. ");
            while (true) {
                receiver();
            }
        });

        // every 10 seconds, processor thread will take a snapshot of current
        processorFuture = executor.submit(() -> {
            logger.info("LoadBalancer processor Thread Started. ");
            while (true) {
                Thread.sleep(1200);
                processor();
            }
        });
    }

    public void stop() throws Exception {
        logger.info("Closing balancer.");
        zkConnect.unregisterBalancer();
        // stop threads
        receiverFuture.cancel(false);
        processorFuture.cancel(false);
        // shutdown zmq socket and context
        recSocket.close();
        recContext.term();
        sendSocket.close();
        sendContext.term();
        executor.shutdownNow();
        // clear the data tree
        zkConnect.resetServer();
        logger.info("Balancer closed.");
    }

    /**
     * receive reports sent by local load analyzer
     *
     * @see LoadAnalyzer
     */
    private void receiver() {
        ZMsg receivedMsg = ZMsg.recvMsg(recSocket);
        String msgTopic = new String(receivedMsg.getFirst().getData());
        byte[] msgContent = receivedMsg.getLast().getData();
        if (msgTopic.equals("registerChannel")) {
            String channel = new String(msgContent);
            logger.debug("Received Channel Register request. Channel: {}", channel);
            if (consistentHashingMap.getBroker(channel) != null) {
                logger.debug("Channel Already Exists. Channel: {}", channel);
            } else {
                consistentHashingMap.registerChannel(channel);
                currentPlan.getChannelMapping().registerNewChannel(channel, consistentHashingMap);
                currentPlan.updateVersion();
                logger.info("Registered new channel: {}", channel);
            }
            return;
        }
        String brokerID = msgTopic;
        TypesBrokerReport report = TypesBrokerReportHelper.deserialize(msgContent);
        logger.debug("Report Received at LoadBalancer. brokerID: {} timeTag: {}", brokerID, report.timeTag());
        //logger.info("LR {}", report.loadRatio());
        if (!brokerLoadReportBuffer.exist(brokerID)) {
            logger.info("New Broker Detected. brokerID: {}", brokerID);
            consistentHashingMap.registerBroker(brokerID);
        }
        brokerLoadReportBuffer.update(brokerID, report);
    }

    /**
     * process reports and generate new plan, send new plan to dispatchers
     */
    private void processor() {
        //logger.info("Processor starts.");
        BrokerLoadReportBuffer reports = brokerLoadReportBuffer.snapShot();
        // TODO: 6/7/17 not support Low load plan now. the generator for low load plan always returns null.
        // initialize a report analyzer
        BrokerReportAnalyzer analyzer = new BrokerReportAnalyzer(reports);
        // generate channel plan first
        ArrayList<ChannelPlan> channelPlans = ChannelPlanGenerator.generatePlans(currentPlan, analyzer, consistentHashingMap);
        // then system plans
        ArrayList<HighLoadPlan> highLoadPlans = SystemPlanGenerator.highLoadPlanGenerator(analyzer);
        ArrayList<LowLoadPlan> lowLoadPlans = SystemPlanGenerator.lowLoadPlanGenerator(analyzer);
        // apply plan --> generate channel mapping
        currentPlan.applyNewPlan(channelPlans, highLoadPlans, lowLoadPlans);

        // TODO: 6/7/17 plan serialization
        sendSocket.sendMore("plan");
        sendSocket.send(TypesPlanHelper.serialize(currentPlan, System.currentTimeMillis()));
        logger.debug("Current plan sent. plan version: {}", currentPlan.getVersion());
    }

    public static void main(String args[]) throws Exception {
        LoadBalancer loadBalancer = new LoadBalancer();
        loadBalancer.start();
        //Thread.sleep(90000);
        //loadBalancer.stop();
    }

}
