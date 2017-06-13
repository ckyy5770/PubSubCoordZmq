package edu.vanderbilt.chuilian.loadbalancer;

/**
 * Created by Killian on 6/1/17.
 */

import edu.vanderbilt.chuilian.types.TypesBrokerReportHelper;
import edu.vanderbilt.chuilian.util.ZkConnect;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * local load analyzer
 * This module will deployed on every broker, and continuously gather extensive load metrics of every channel.
 * The recorded metrics for every time unit t(1s) includes:
 * 1. (the number and list of publishers) -> this should get from zookeeper server by load balancer node
 * 2. (the number of publications) -> this should be the same as #4
 * 3. (the number and list of subscribers) -> this should get from zookeeper server by load balancer node
 * 4. the number of sent messages
 * 5. incoming and outgoing number of bytes transmitted
 */
public class LoadAnalyzer {
    private String brokerID;
    private String balancerAddress;
    private ExecutorService executor;
    private ZMQ.Context sendContext;
    private ZMQ.Socket sendSocket;
    private Future<?> future;
    private BrokerReport brokerReport;
    private ZkConnect zkConnect;

    private static final Logger logger = LogManager.getLogger(LoadAnalyzer.class.getName());

    public LoadAnalyzer(String brokerID, ZkConnect zkConnect) {
        this.brokerID = brokerID;
        this.balancerAddress = null;
        this.executor = Executors.newFixedThreadPool(2);
        this.zkConnect = zkConnect;
        this.sendContext = ZMQ.context(1);
        this.sendSocket = sendContext.socket(ZMQ.PUB);
        this.brokerReport = new BrokerReport(brokerID);
    }

    public BrokerReport getBrokerReport() {
        return brokerReport;
    }

    public static double getBandWidthBytes() {
        return 1024 * 1024;
    }

    public void start() throws Exception {
        future = executor.submit(() -> {
            logger.info("Local LoadAnalyzer thread started, trying to connect LoadBalancer.");
            // try to get balancer address
            try {
                this.balancerAddress = zkConnect.getBalancerRecAddress();
                while (balancerAddress == null) {
                    // if cannot get it, keep trying, every 5 secs
                    Thread.sleep(5000);
                    balancerAddress = zkConnect.getBalancerRecAddress();
                }
            } catch (Exception e) {
                return;
            }
            // here we get a non-null address, connect it
            sendSocket.connect("tcp://" + balancerAddress);
            logger.info("Local LoadAnalyzer connected to LoadBalancer receiver port @{}", balancerAddress);

            while (true) {
                try {
                    Thread.sleep(1000);
                    report();
                    reset();
                } catch (Exception e) {
                    return;
                }
            }
        });
    }

    public void stop() {
        logger.info("Closing Local LoadAnalyzer.");
        future.cancel(false);
        sendSocket.close();
        sendContext.term();
        executor.shutdownNow();
        logger.info("Local load analyzer closed.");
    }

    private void report() {
        sendSocket.sendMore(brokerID);
        sendSocket.send(TypesBrokerReportHelper.serialize(brokerReport, System.currentTimeMillis()));
        logger.debug("load report sent from Load Analyzer of broker: {}", brokerID);
    }

    private void reset() {
        brokerReport.reset();
    }

}

