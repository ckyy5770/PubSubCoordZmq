# PubSubCoordZmq

## Discovery Service

### Znode data tree structure

root: 

* topics --> storing default channel address, including receiver port and sender port, first line in the data file is receiver address and the second line is sender address.

topics: 

* topic_A --> storing nothing

* topic_B --> storing nothing

topic_A:

* pub --> storing specific channel receiver address of topic_A

* sub --> storing specific channel sender address of topic_A

### WorkFlow

When the broker starts, it will initialize a default channel with a receiver port and a sender port associate with it. Then it will go to zookeeper server, wipe out all data in the /topics node alone with all sub-nodes if needed, then rewrite its current sender and receiver address to it. Then it claims the broker started.

Publisher will maintain a list in its local machine that says which topic should send to which address. Before it send something, it will first look up the local list, try to figure out which address should it send the message to, if not found in local list, it will go to zookeeper server and try to find the receiver port under /topics/sometopic, if sometopic does not exist, it just go to /topic and try to send the message to the default receiver address. If there is not default receiver address, that means the broker is not started yet or something wrong with the zookeeper sever, it should just wait for certain amount of time and try again. If the publisher successfully get the receiver address for that topic, it will add its address under /topic/sometopic/pub, then add the receiver address to local list.

When a new topic comes into the default channel, the main channel thread will first check if this is a unregistered topic, then if it is, make a new channel (in new thread) for it. When starting the channel, the channel thread will first go to zookeeper server, make a new topic node under /topics, and make two node /pub /sub, write its receiver address and sender address to the corresponding node.

Subscriber will have similar logic as the publisher.

When the broker stops, it should delete all topics and data under /topics, and all data in /topics.