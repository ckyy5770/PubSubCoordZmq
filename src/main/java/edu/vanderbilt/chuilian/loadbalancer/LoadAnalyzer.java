package edu.vanderbilt.chuilian.loadbalancer;

/**
 * Created by Killian on 6/1/17.
 */

import edu.vanderbilt.chuilian.types.LoadReportHelper;
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

    public LoadAnalyzer(String balancerAddress, String brokerID, ExecutorService executor) {
        this.brokerID = brokerID;
        this.balancerAddress = balancerAddress;
        this.executor = executor;
        this.sendContext = ZMQ.context(1);
        this.sendSocket = sendContext.socket(ZMQ.PUB);
        this.reportMap = new ReportMap();
    }

    void start() {
        sendSocket.connect(balancerAddress);
        future = executor.submit(() -> {
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
        future.cancel(false);
        sendSocket.close();
        sendContext.term();
    }

    void report() {
        sendSocket.sendMore(brokerID);
        sendSocket.send(LoadReportHelper.serialize(reportMap));
    }

    void reset() {
        reportMap.reset();
    }

}

