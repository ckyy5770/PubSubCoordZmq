package edu.vanderbilt.chuilian.loadbalancer;

/**
 * Created by Killian on 6/2/17.
 */

import edu.vanderbilt.chuilian.types.BalancerPlanHelper;
import edu.vanderbilt.chuilian.types.LoadReport;
import edu.vanderbilt.chuilian.types.LoadReportHelper;
import edu.vanderbilt.chuilian.util.ZkConnect;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * This is load balancer node
 * Load balancer will collect load reports from every single broker and analyze them, decide whether it should generate a new plan
 */
public class LoadBalancer {
    BrokerReportMap brokerReportMap;
    private final ExecutorService executor;
    private final ZkConnect zkConnect;
    /**
     * receiver socket will receive load reports from all local load analyzer, residing in each broker
     * sender socket will send new plans to all dispatcher, residing in each broker
     */
    private ZMQ.Context sendContext;
    private ZMQ.Socket sendSocket;
    private ZMQ.Context recContext;
    private ZMQ.Socket recSocket;
    // TODO: 6/2/17 hard coded port, port 25001 is receiver port, port 25002 is sender port
    private int recPort = 25001;
    private int sendPort = 25002;
    // TODO: 5/25/17 hard coded ip for now
    String ip = "127.0.0.1";

    Future<?> receiverFuture;
    Future<?> processorFuture;

    private static final Logger logger = LogManager.getLogger(LoadBalancer.class.getName());

    public LoadBalancer() {
        this.brokerReportMap = new BrokerReportMap();
        this.executor = Executors.newFixedThreadPool(3);
        this.zkConnect = new ZkConnect();
        // initialize socket
        this.recContext = ZMQ.context(1);
        this.recSocket = this.recContext.socket(ZMQ.SUB);
        this.sendContext = ZMQ.context(1);
        this.sendSocket = this.sendContext.socket(ZMQ.PUB);
    }

    public void start() throws Exception {
        // start listening to receiver port
        recSocket.bind("tcp://*:" + recPort);
        // subscribe all topic
        recSocket.subscribe("".getBytes());
        // start connecting to sending port
        sendSocket.bind("tcp://*:" + sendPort);
        // register itself to zookeeper service
        zkConnect.registerBalancer(ip + recPort, ip + sendPort);

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
                Thread.sleep(10000);
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
        logger.info("Balancer closed.");
    }

    /**
     * receive reports sent by local load analyzer
     *
     * @see LoadAnalyzer
     */
    private void receiver() {
        ZMsg receivedMsg = ZMsg.recvMsg(recSocket);
        String brokerID = new String(receivedMsg.getFirst().getData());
        byte[] msgContent = receivedMsg.getLast().getData();
        LoadReport report = LoadReportHelper.deserialize(msgContent);
        logger.info("Report Received at LoadBalancer. brokerID: {} timeTag: {}", brokerID, report.timeTag());
        brokerReportMap.update(brokerID, report);
    }

    /**
     * process reports and generate new plan, send new plan to dispatchers
     */
    private void processor() {
        HashMap<String, LoadReport> reports = brokerReportMap.snapShot();
        for (Map.Entry<String, LoadReport> entry : reports.entrySet()) {
            int length = entry.getValue().channelReportsLength();
            for (int i = 0; i < length; i++) {
                logger.info("Report entry processed. brokerID: {} topic: {}", entry.getKey(), entry.getValue().channelReports(i).topic());
            }
        }
        // TODO: 6/2/17 need to add processing logic and plan generation logic
        Plan plan = null;
        if (plan != null) {
            sendSocket.sendMore("plan");
            sendSocket.send(BalancerPlanHelper.serialize(plan, System.currentTimeMillis()));
            logger.info("New plan sent.");
        }
    }

}
