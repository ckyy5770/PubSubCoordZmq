package edu.vanderbilt.chuilian.loadbalancer;

/**
 * Created by Killian on 6/1/17.
 */

import edu.vanderbilt.chuilian.types.LoadReportHelper;
import edu.vanderbilt.chuilian.util.ZkConnect;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ;

import java.util.concurrent.ExecutorService;
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
    private ReportMap reportMap;
    private ZkConnect zkConnect;

    private static final Logger logger = LogManager.getLogger(LoadAnalyzer.class.getName());

    public LoadAnalyzer(String brokerID, ExecutorService executor, ZkConnect zkConnect) {
        this.brokerID = brokerID;
        this.balancerAddress = null;
        this.executor = executor;
        this.zkConnect = zkConnect;
        this.sendContext = ZMQ.context(1);
        this.sendSocket = sendContext.socket(ZMQ.PUB);
        this.reportMap = new ReportMap();
    }

    void start() throws Exception {
        future = executor.submit(() -> {
            logger.info("Local LoadAnalyzer thread started.");
            // try to get balancer address
            try {
                String address = zkConnect.getBalancerRecAddress();
                while (address == null) {
                    // if cannot get it, keep trying, every 5 secs
                    Thread.sleep(5000);
                    address = zkConnect.getBalancerRecAddress();
                }
            } catch (Exception e) {
                return;
            }
            // here we get a non-null address, connect it
            sendSocket.connect(balancerAddress);

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

    void stop() {
        logger.info("Closing Local LoadAnalyzer.");
        future.cancel(false);
        sendSocket.close();
        sendContext.term();
        logger.info("Local load analyzer closed.");
    }

    void report() {
        sendSocket.sendMore(brokerID);
        sendSocket.send(LoadReportHelper.serialize(reportMap, System.currentTimeMillis()));
    }

    void reset() {
        reportMap.reset();
    }

}

