package edu.vanderbilt.chuilian.loadbalancer;

/**
 * Created by Killian on 6/2/17.
 */

import edu.vanderbilt.chuilian.loadbalancer.plan.Plan;
import edu.vanderbilt.chuilian.types.TypesPlan;
import edu.vanderbilt.chuilian.types.TypesPlanHelper;
import edu.vanderbilt.chuilian.util.ZkConnect;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Dispatcher residing in each edge broker will keep the latest balancer plan
 */
public class Dispatcher {
    private Plan plan;
    private String brokerID;
    private ZkConnect zkConnect;
    // socket connect to LB's sender
    private ZMQ.Context recContext;
    private ZMQ.Socket recSocket;
    // socket connect to LB's receiver
    private ZMQ.Context sendContext;
    private ZMQ.Socket sendSocket;
    private ExecutorService executor;
    private String balancerSendAddress;
    private String balancerRecAddress;

    private Future<?> futureReceiverFromLB;

    private static final Logger logger = LogManager.getLogger(Dispatcher.class.getName());

    public Dispatcher(String brokerID, ZkConnect zkConnect) {
        this.brokerID = brokerID;
        this.plan = null;
        this.zkConnect = zkConnect;
        this.balancerSendAddress = null;
        this.executor = Executors.newFixedThreadPool(10);
        this.zkConnect = zkConnect;
        this.recContext = ZMQ.context(1);
        this.recSocket = recContext.socket(ZMQ.SUB);
        this.sendContext = ZMQ.context(1);
        this.sendSocket = sendContext.socket(ZMQ.PUB);
    }

    public void start() {
        futureReceiverFromLB = executor.submit(() -> {
            logger.info("Dispatcher thread started, trying to connect LoadBalancer.");
            // try to get balancer sender address
            try {
                this.balancerSendAddress = zkConnect.getBalancerSendAddress();
                while (balancerSendAddress == null) {
                    // if cannot get it, keep trying, every 5 secs
                    Thread.sleep(5000);
                    balancerSendAddress = zkConnect.getBalancerSendAddress();
                }
            } catch (Exception e) {
                return;
            }
            // try to get balancer receiver address
            try {
                this.balancerRecAddress = zkConnect.getBalancerRecAddress();
                while (balancerRecAddress == null) {
                    // if cannot get it, keep trying, every 5 secs
                    Thread.sleep(5000);
                    balancerRecAddress = zkConnect.getBalancerRecAddress();
                }
            } catch (Exception e) {
                return;
            }
            // here we get a non-null address, connect it
            recSocket.connect("tcp://" + balancerSendAddress);
            sendSocket.connect("tcp://" + balancerRecAddress);
            logger.info("Dispatcher connected to LoadBalancer sender port @{}", balancerSendAddress);
            logger.info("Dispatcher connected to LoadBalancer receiver port @{}", balancerRecAddress);
            // subscribe "plan" topic see @LoadBalancer
            recSocket.subscribe("plan".getBytes());
            // keep receiving and updating local plan
            while (true) {
                receiverFromLB();
            }
        });
    }

    public void receiverFromLB() {
        ZMsg receivedMsg = ZMsg.recvMsg(recSocket);
        byte[] msgContent = receivedMsg.getLast().getData();
        TypesPlan typesPlan = TypesPlanHelper.deserialize(msgContent);
        Plan plan = TypesPlanHelper.toPlan(typesPlan);
        logger.info("New Balancer Plan Received at Dispatcher. brokerID: {} timeTag: {}", brokerID, typesPlan.timeTag());
        // update plan
        this.plan = plan;
    }

    public void stop() {
        logger.info("Closing Dispatcher.");
        futureReceiverFromLB.cancel(false);
        recSocket.close();
        recContext.term();
        sendSocket.close();
        sendContext.term();
        executor.shutdownNow();
        logger.info("Dispatcher closed.");
    }

    public Plan getPlan() {
        return this.plan;
    }


    public void registerChannelToLB(String channel) {
        sendMsgToLB("registerChannel", channel);
    }

    private void sendMsgToLB(String topic, String content) {
        sendSocket.sendMore(topic);
        sendSocket.send(content.getBytes());
        logger.info("Message sent from dispatcher to LoadBalancer. Topic:{} Content:{}", topic, content);
    }
}
