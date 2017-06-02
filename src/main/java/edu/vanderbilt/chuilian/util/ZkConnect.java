package edu.vanderbilt.chuilian.util;

/**
 * Created by Killian on 5/25/17.
 */

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class ZkConnect {
    private ZooKeeper zk;
    private CountDownLatch connSignal = new CountDownLatch(0);
    private static final Logger logger = LogManager.getLogger(ZkConnect.class.getName());

    public ZooKeeper connect(String host) throws Exception {
        logger.debug("Connecting host: " + host);
        //host should be 127.0.0.1:2187
        zk = new ZooKeeper(host, 3000, new Watcher() {
            public void process(WatchedEvent event) {
                if (event.getState() == KeeperState.SyncConnected) {
                    connSignal.countDown();
                }
            }
        });
        connSignal.await();
        logger.debug("Connected host: " + host);
        return zk;
    }

    public void close() throws InterruptedException {
        zk.close();
    }

    /**
     * clear data tree at zookeeper server, this method should be called when the broker starts and stops
     *
     * @throws Exception
     */
    public void resetServer() throws Exception {
        if (existsNode("/topics")) this.recursiveDelete("/topics");
        // "null" means the default channel address is not set yet
        this.createNode("/topics", "null".getBytes());
    }

    /**
     * get the number of children within a node, -1 if node does not exist
     *
     * @param path
     * @return
     * @throws Exception
     */
    public int getNumChildren(String path) throws Exception {
        Stat stat = zk.exists(path, true);
        if (stat == null) return -1;
        else return stat.getNumChildren();
    }

    /**
     * get the data within a node, null if node does not exist
     *
     * @param path
     * @return
     * @throws Exception
     */
    public String getNodeData(String path) throws Exception {
        Stat stat = zk.exists(path, true);
        if (stat == null) return null;
        else return new String(zk.getData(path, true, null));
    }

    /**
     * register a channel on zookeeper server
     *
     * @param topic
     * @param recAddress
     * @param sendAddress
     * @throws Exception
     */
    public void registerChannel(String topic, String recAddress, String sendAddress) throws Exception {
        // if there is no /topics yet, throw a exception, this will never happen in a well-designed system
        if (!existsNode("/topics")) throw new IllegalStateException("/topics node does not exist");
        // if there is already a channel corresponding to this topic, throw a exception
        if (existsNode("/topics/" + topic))
            throw new IllegalStateException("try to register a topic that already exists on zookeeper. topic: " + topic);
        // here everything is fine, we create a new node with "null" data
        createNode("/topics/" + topic, "null".getBytes());
        // Create two child node under this node: pub and sub
        // and put receiver address and sender address in them separately
        createNode("/topics/" + topic + "/pub", recAddress.getBytes());
        createNode("/topics/" + topic + "/sub", sendAddress.getBytes());
    }

    public void unregisterChannel(String topic) throws Exception {
        // if the channel does not exist on zookeeper, throw an exception
        if (!existsNode("/topics/" + topic))
            throw new IllegalStateException("/topics/" + topic + " node does not exist");
        // if there still are subscribers or publishers on this topic, throw
        if (anyPublisher(topic) || anySubsciber(topic))
            throw new IllegalStateException("/topics/" + topic + " still has alive pub/sub!");
        // now delete this channel
        recursiveDelete("/topics/" + topic);
    }

    /**
     * check if there is any subscriber on the topic, return true if there exists one.
     *
     * @param topic
     * @return
     */
    public boolean anySubsciber(String topic) throws Exception {
        // if the topic does not exist on zookeeper, throw an exception
        if (!existsNode("/topics/" + topic))
            throw new IllegalStateException("/topics/" + topic + " node does not exist");
        return getNumChildren("/topics/" + topic + "/sub") > 0;
    }

    /**
     * check if there is any publisher on the topic, return true if there exists one.
     *
     * @param topic
     * @return
     */
    public boolean anyPublisher(String topic) throws Exception {
        // if the topic does not exist on zookeeper, throw an exception
        if (!existsNode("/topics/" + topic))
            throw new IllegalStateException("/topics/" + topic + " node does not exist");
        return getNumChildren("/topics/" + topic + "/pub") > 0;
    }

    /**
     * check if there is any channel, return true if there exists one.
     *
     * @return
     */
    public boolean anyChannel() throws Exception {
        // if the topics does not exist on zookeeper, throw an exception
        if (!existsNode("/topics")) throw new IllegalStateException("/topics node does not exist");
        return getNumChildren("/topics") > 0;
    }

    /**
     * register default channel
     *
     * @param recAddress
     * @param sendAddress
     * @throws Exception
     */
    public void registerDefaultChannel(String recAddress, String sendAddress) throws Exception {
        // if there is no /topics yet, throw a exception, this will never happen in a well-designed system
        if (!existsNode("/topics")) throw new IllegalStateException("/topics node does not exist");
        // here everything is fine, we update node data
        String data = recAddress + "\n" + sendAddress;
        updateNode("/topics", data.getBytes());
    }

    /**
     * unregister default channel
     *
     * @throws Exception
     */
    public void unregisterDefaultChannel() throws Exception {
        // if there is no /topics yet, throw a exception, this will never happen in a well-designed system
        if (!existsNode("/topics")) throw new IllegalStateException("/topics node does not exist");
        // here we delete default channel by updating /topics to "null"
        updateNode("/topics", "null".getBytes());
    }

    /**
     * register a publisher, return its unique ID
     *
     * @param topic
     * @param sendAddress
     * @return
     * @throws Exception
     */
    public String registerPub(String topic, String sendAddress) throws Exception {
        // if there is no /topics/topic/pub yet, throw a exception
        Stat stat = zk.exists("/topics/" + topic + "/pub", true);
        if (stat == null) throw new IllegalStateException("/topics/" + topic + "/pub" + " node does not exist");
        // here we create a new pub node under /pub
        int pubNum = stat.getNumChildren() + 1;
        createNode("/topics/" + topic + "/pub/" + "pub" + Integer.toString(pubNum), sendAddress.getBytes());
        return Integer.toString(pubNum);
    }

    /**
     * unregister a publisher
     *
     * @param topic
     * @param id
     * @throws Exception
     */
    public void unregisterPub(String topic, String id) throws Exception {
        // if the given pub doesn't exist in zookeeper, throw a exception
        Stat stat = zk.exists("/topics/" + topic + "/pub/" + "pub" + id, false);
        if (stat == null)
            throw new IllegalStateException("/topics/" + topic + "/pub/" + "pub" + id + " node does not exist");
        this.deleteNode("/topics/" + topic + "/pub/" + "pub" + id);
    }

    /**
     * register a subscriber, return its unique ID
     *
     * @param topic
     * @param recAddress
     * @return
     * @throws Exception
     */
    public String registerSub(String topic, String recAddress) throws Exception {
        // if there is no /topics/topic/sub yet, throw a exception
        Stat stat = zk.exists("/topics/" + topic + "/sub", true);
        if (stat == null) throw new IllegalStateException("/topics/" + topic + "/sub" + " node does not exist");
        // here we create a new sub node under /sub
        int subNum = stat.getNumChildren() + 1;
        createNode("/topics/" + topic + "/sub/" + "sub" + Integer.toString(subNum), recAddress.getBytes());
        return Integer.toString(subNum);
    }

    /**
     * unregister a subscriber
     *
     * @param topic
     * @param id
     * @throws Exception
     */
    public void unregisterSub(String topic, String id) throws Exception {
        // if the given sub doesn't exist in zookeeper, throw a exception
        Stat stat = zk.exists("/topics/" + topic + "/sub/" + "sub" + id, false);
        if (stat == null)
            throw new IllegalStateException("/topics/" + topic + "/sub/" + "sub" + id + " node does not exist");
        deleteNode("/topics/" + topic + "/sub/" + "sub" + id);
    }

    /**
     * register Balancer
     *
     * @param recAddress
     * @param sendAddress
     * @throws Exception
     */
    public void registerBalancer(String recAddress, String sendAddress) throws Exception {
        // if /balancer exists, delete it
        if (existsNode("/balancer")) recursiveDelete("/balancer");
        String data = recAddress + "\n" + sendAddress;
        // make a new /balancer with this information
        createNode("/balancer", data.getBytes());
    }

    /**
     * unregister balancer
     *
     * @throws Exception
     */
    public void unregisterBalancer() throws Exception {
        // if there is no /balancer yet, throw a exception, this will never happen in a well-designed system
        if (!existsNode("/balancer")) throw new IllegalStateException("/balancer node does not exist");
        // here we delete default channel by updating /topics to "null"
        updateNode("/balancer", "null".getBytes());
    }

    /**
     * get balancer's receiver address
     *
     * @return
     * @throws Exception
     */
    public String getBalancerRecAddress() throws Exception {
        String data = getNodeData("/balancer");
        if (data == null) return null;
        String[] addresses = data.split("\n");
        if (addresses[0] == "null") return null;
        return addresses[0];
    }

    /**
     * get balancer's sender address
     *
     * @return
     * @throws Exception
     */
    public String getBalancerSendAddress() throws Exception {
        String data = getNodeData("/balancer");
        if (data == null) return null;
        String[] addresses = data.split("\n");
        if (addresses[0] == "null") return null;
        return addresses[1];
    }

    /**
     * recursively delete all nodes under a path
     */
    public void recursiveDelete(String path) throws Exception {
        List<String> children = zk.getChildren(path, true);
        if (children.size() == 0) {
            deleteNode(path);
        } else {
            for (String child : children) {
                recursiveDelete(path + "/" + child);
            }
            deleteNode(path);
        }
    }
    /**
     * true if the node exists
     *
     * @param path
     * @return
     * @throws Exception
     */
    public boolean existsNode(String path) throws Exception {
        return zk.exists(path, true) != null;
    }

    public void createNode(String path, byte[] data) throws Exception {
        zk.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    public void updateNode(String path, byte[] data) throws Exception {
        zk.setData(path, data, zk.exists(path, true).getVersion());
    }

    public void deleteNode(String path) throws Exception {
        zk.delete(path, zk.exists(path, true).getVersion());
    }


    public static void main(String args[]) throws Exception {
        ZkConnect connector = new ZkConnect();
        ZooKeeper zk = connector.connect("127.0.0.1:2181");
        String newNode = "/deepakDate" + new Date();
        connector.createNode(newNode, new Date().toString().getBytes());
        List<String> zNodes = zk.getChildren("/", true);
        for (String zNode : zNodes) {
            System.out.println("ChildrenNode " + zNode);
        }
        byte[] data = zk.getData(newNode, true, zk.exists(newNode, true));
        System.out.println("GetData before setting");
        for (byte dataPoint : data) {
            System.out.print((char) dataPoint);
        }

        System.out.println("GetData after setting");
        connector.updateNode(newNode, "Modified data".getBytes());
        data = zk.getData(newNode, true, zk.exists(newNode, true));
        for (byte dataPoint : data) {
            System.out.print((char) dataPoint);
        }
        connector.deleteNode(newNode);
        connector.resetServer();
    }

}