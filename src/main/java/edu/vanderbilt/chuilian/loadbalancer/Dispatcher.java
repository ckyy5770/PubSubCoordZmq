package edu.vanderbilt.chuilian.loadbalancer;

/**
 * Created by Killian on 6/2/17.
 */

import edu.vanderbilt.chuilian.types.BalancerPlan;
import edu.vanderbilt.chuilian.types.BalancerPlanHelper;
import edu.vanderbilt.chuilian.util.ZkConnect;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Dispatcher residing in each edge broker will keep the latest balancer plan
 */
public class Dispatcher {
    private BalancerPlan plan;
    private String brokerID;
    private ZkConnect zkConnect;
    private ZMQ.Context recContext;
    private ZMQ.Socket recSocket;
    private ExecutorService executor;
    private String balancerAddress;

    private Future<?> future;

    private static final Logger logger = LogManager.getLogger(Dispatcher.class.getName());

    public Dispatcher(String brokerID, ExecutorService executor, ZkConnect zkConnect) {
        this.brokerID = brokerID;
        this.plan = null;
        this.zkConnect = zkConnect;
        this.balancerAddress = null;
        this.executor = executor;
        this.zkConnect = zkConnect;
        this.recContext = ZMQ.context(1);
        this.recSocket = recContext.socket(ZMQ.SUB);
    }

    public void start() {
        future = executor.submit(() -> {
            logger.info("Dispatcher thread started.");
            // try to get balancer sender address
            try {
                this.balancerAddress = zkConnect.getBalancerSendAddress();
                while (balancerAddress == null) {
                    // if cannot get it, keep trying, every 5 secs
                    Thread.sleep(5000);
                    balancerAddress = zkConnect.getBalancerSendAddress();
                }
            } catch (Exception e) {
                return;
            }
            // here we get a non-null address, connect it
            recSocket.connect(balancerAddress);
            // subscribe "plan" topic see @LoadBalancer
            recSocket.subscribe("plan".getBytes());
            // keep receiving and updating local plan
            while (true) {
                receiver();
            }
        });
    }

    public void receiver() {
        ZMsg receivedMsg = ZMsg.recvMsg(recSocket);
        byte[] msgContent = receivedMsg.getLast().getData();
        BalancerPlan balancerPlan = BalancerPlanHelper.deserialize(msgContent);
        logger.info("New Balancer Plan Received at Dispatcher. brokerID: {} timeTag: {}", brokerID, balancerPlan.timeTag());
        // update plan
        this.plan = balancerPlan;
    }

    public void stop() {
        logger.info("Closing Dispatcher.");
        future.cancel(false);
        recSocket.close();
        recContext.term();
        logger.info("Dispatcher closed.");
    }

    public BalancerPlan getPlan() {
        return this.plan;
    }

}
