# PubSubCoordZmq

## Edge Broker

### Work flow overview

when the edge broker starts, it will make a default channel object and start it.

### Message Channel

Message channel is an object that transfer messages of one topic from publisher to subscriber. To create a message channel, you must assign a topic to it.

* When a message channel initializes and starts, it will require two available ports from the broker port list and then bind its receiver and sender socket to these two ports. 

* Then it subscribe the specific topic it interested in and register itself to the zookeeper server by making a new node /topics/sometopic, and two subnode /sub, /pub and write its sender/receiver address information to them respectively. 

* Then it will start one thread for receiving/sending messages.

* (TBD) Message channel should also be able to notify clients that "there is a channel plan change, please reconnect to correct channel for send/receive publications."

### Default Channel (Main Channel)

Default channel is a message channel that is used for transferring messages of new topics.

Default is essentially a message channel with some modifications including:

* there should be one and only one default channel, and it should be created when the broker starts.

* in stead of receiving messages from one specific topic like a normal message channel, it will receive messages from all topics.

* the default channel will be taking charge of registering new topics to load balancer: for every message received, the main channel will look up the current load plan, see if there is a channel for it, if there is not, report it. 

* if a publisher do not have a sender for a specific topic, it should send the message to default channel of a random broker to get channel registered on load balancer.

## Publisher

### Work flow overview

When the publisher starts, it will try to get the receiver address of default channel from zookeeper server. If fails, just try again until the broker is online. After get the default receiver address of the broker, it will create a default sender which should then connect to the default channel of the broker.

Publisher provide a send(topic, message) method for user to send messages. for each message it gets from user, it will first look at the topic sender map, try to find a existed sender, if fails, it will then try to acquire the address of channel for this topic from zookeeper server, if fails, it will then try to get the default channel address. 

### Data Sender

Data sender is a object that sends messages of one topic, and it should connect to the specific channel in broker for that topic.

* When the data sender starts, it will connect to given receiver address and register itself on the zookeeper server by creating a node under /sometopic/pub, then it will register a local message buffer for it, and start a thread that keep checking and sending messages from the buffer. (Note the sender provides a send(message) method for publisher to send messages to its internal message buffer.)

Note: once started, the sender will not automatically stopped when there are no messages in its sending buffer, it can only be removed by explicitly by publisher calling sender.stop();

* (TBD) data sender should also has a thread to receive notifications from the broker, if there is a channel plan change for this topic, it should reconnect itself to the new correct channel address.

### Default Sender

Every publisher born with a default sender, which directly connects to the default message channel of the broker. When there is not a message channel for one specific topic, the message will be sent through default sender.

## Subscriber

the subscriber has the similar structure like the publisher.

### Work flow overview

When the subscriber starts, it will create a default receiver that connects to the default channel of the broker. And starts a processor thread that checks message buffers every 3 seconds. 

By default, the default receiver will subscribe nothing when it starts. To receive messages, we need use method subscribe(topic). subscribe method will first subscribe this topic in the default receiver so that the default receiver can receive the messages that relevant to this topic from default channel. Then it will try to get the topic channel address from zookeeper, if the channel not found, it throw an exception. (Here may need to change the logic, for now, all calls to subscribe method will fail if topic channel not found on the zookeeper server) If found, create a new receiver for it.

Note: updated subscriber logic:

the default receiver will subscribe nothing when it starts. When calling subscribe(topic) method from subscriber, it will first check if this topic on the topic-receiver map, if so, just return. If not, it will then check if this topic on the topic-waiter map, if so, do nothing but return. 

If not, the subscriber will first, subscribe the topic at default receiver, then try to get the channel address for this topic, then create a receiver using this address. if it cannot get the channel address, it will create a waiter object which will then create a thread that periodically check if the server opened a channel for this topic. Once it detected the channel is opened, it will create a receiver for this topic.

### Data Receiver

Data receiver is a object that receives messages of one topic, and it should connect to the specific channel in broker for that topic.

* (TBD) data receiver should also has a thread to receive notifications from the broker, if there is a channel plan change for this topic, it should reconnect itself to the new correct channel address.

* When the data receiver starts, it will connect to given sender address and register itself on the zookeeper server by creating a node under /sometopic/sub, then it will register a local message buffer for it, and start a thread that keep receiving and storing messages to the buffer.

### Default Receiver

Every publisher born with a default receiver, which directly connects to the default message channel of the broker. It behaves exactly like a normal receiver except:

* default receiver may subscribe multiple topics

* default receiver store received messages under the buffer named ""

* default receiver cannot be stopped by calling unsubscribe(topic) from subscriber, it can be stopped by explicitly calling defaultreceiver.stop().

## Discovery Service

### Znode data tree structure

```
           root
          /    \
       topics  balancer
       /    \
   topic_A topic_B
     / \     / \
   pub sub  pub sub
   / \
pub1 pub2 ... 

```

root: 
* data: nothing
* children: topics 

topics: 
* data: default channel address, including receiver port and sender port, format: "broker1_receiver_address,broker1_sender_address\nbroker2_receiver_address,broker2_sender_address\n"
* children: topic_A, topic_B, ...

topic_A:
* data: must always be "null"
* children: always has two children, pub, sub.

topic_A/pub:
* data: must always be valid channel receiver addresses of topic_A
* children: pub1, pub2, pub3...
Note: since channel can be replicated, there might be multiple receiver addresses or sender addresses on topics/sometopic/pub(or sub).

topic_A/pub/pub1:
* data: must always be valid ip address of pub1.
* children: none.

balancer:
* data: balancer address, including receiver port and sender port, first line in the data file is receiver address and the second line is sender address. 
* children: none.

### WorkFlow

When load balancer starts, it will register itself on /loadbalancer.

When a broker starts, it will initialize a default channel with a receiver port and a sender port associate with it. Then it will go to zookeeper server, register the default channel on /topics.

Publisher will maintain a list in its local machine that says which topic should send to which address. Before it send something, it will first look up the local list, try to figure out which address should it send the message to, if not found in local list, it will go to zookeeper server and try to find the receiver port under /topics/sometopic, if sometopic does not exist, it just go to /topic and try to send the message to the default receiver address. If there is not default receiver address, that means the broker is not started yet or something wrong with the zookeeper sever, it should just wait for certain amount of time and try again. If the publisher successfully get the receiver address for that topic, it will add its address under /topic/sometopic/pub, then add the receiver address to local list.

When starting a channel, the channel thread will first go to zookeeper server, register itself on /topics/sometopic/

Subscriber will have similar logic as the publisher.

When the load balancer stops, it should delete all in /

### Data format

* all: "null" --> nodes with no information or initialization a node with empty information

* /topic: "broker1_receiver_address,broker1_sender_address\nbroker2_receiver_address,broker2_sender_address\n"

* /topics/sometopic/pub: "broker1_sometopic_receiver_address\nbroker2_sometopic_receiver_address\n"

* /topics/sometopic/sub: "broker1_sometopic_sender_address\nbroker2_sometopic_sender_address\n"

## Load Balancing

The load balancing system is inspired by [Dynamoth](http://ieeexplore.ieee.org/document/7164934/).

### Load Balancing Overview

Each edge broker will be equipped with a Local Load Analyzer(LLA) and a Dispatcher(D). LLA will send a local load report of the broker every 1 secs, to the Load Balancer(LB). Then Load Balancer will gather all current load information for each edge broker, then decide whether it should change the Load Plan. If a new plan is generated, it will be sent to dispatchers residing in each edge broker.

### Load Balancing Workflow

#### Load balancer:

* broker closing and opening channels are under the instruction of load balancer. More specifically, load balancer will keep sending latest load plan to each broker, and brokers should open/close channels strictly following the plan.

* receiver thread (keep running): keep receiving reports from brokers and update the broker load report buffer. also register new coming brokers.

* processor thread (run every x secs): take a snapshot of latest reports from the broker load report buffer, initialize an analyzer for this version of report. Generate channel-level plan, system-level plan using the analyzer. Apply two level plans (if any) to generate new channel mapping. Send new plan to dispatchers residing in each broker.

* first message from broker: after a new broker starts, it first send a message to register itself at load balancer. 

* (TBD) last message from broker: when a broker stops normally, it will send a message to notify load balancer to unregister itself.

* channel register: when a broker's default channel received a message of brand new topic, it will report the topic to load balancer, the load balancer should double-check if this is a new topic, then register the topic, and generate a proper mapping for it, then disseminate the new plan to brokers, some brokers will open new channel for it.

* (TBD) unregister channel: load balancer will periodically check if there are topics that don't have any subs/pubs and unregister them as needed.

* (TBD) unregister broker: load balancer will periodically check if there are broker that don't update reports for a long time and unregister them as needed. 

### Plan

plan is a data structure that encapsulates all information needed to instruct to which address a given publication or subscription should be sent to. It's like a lookup table where the keys are channels(topics), and the values are the list of servers that should be used for each channel.

### Channel level rebalancing:

Channel replication strategies:

* All-subscribers replication: used for situations when given channel has a very large number of subscribers. In this case, subscribers for this topic, should connect to all channels in the broker, and publishers could pick one of them to send their publications (of that topic) to.
 
* All-publishers replication: used for situations when given channel has a very large number of publishers. In this case, publishers should send the publication of this topic to all channels in the broker, and subscribers could pick one of them to connect.

Default strategy:

* consistent hashing: each new coming broker should first register itself on zookeeper and load balancer. Load balancer will then assign it a ID number to be used for consistent hashing. (Note this ID is different from the brokerID, this ID is only used for consistent hashing) If the load balancer didn't receive message from one broker for a certain amount of time, it will determine that the broker is down, then it will unregister the broker.

### System level rebalancing:

* High-load rebalancing: If there is a pub/sub server with a load ratio that exceeds a given threshold, then a new high-load plan must be generated so that the new plan ensures that the load returns below a safe threshold for all servers. Basically, what new plan does is to migrate some channels from the overload channel, to the least busiest channel.

* Low-load rebalancing: If the global load ratio is below a given threshold, then one or more servers can be freed. Channel from the lowest loaded server are slowly migrated to the other servers as long as the load on the other server stays below a given limit.

### Reconfiguration:

There are 9 scenarios when reconfiguration may happen. Each of those scenarios, we carefully defined a reconfiguration steps that clients and brokers should follow in order to prevent losing messages.

#### 1. consistent hashing --> all-pub replication

* broker open new channel

* all publishers of this topic connect to new channel

* all subscribers of this topic reconnect to new channel (**Important Note**: every "reconnect" step in all scenarios should be interpreted as following three sub-steps):

    * first connect to new channel
    
    * wait for connection stable
    
    * then disconnect from old channel
    
#### 2. consistent hashing --> all-sub replication

* broker open new channel

* all subscribers of this topic connect to new channel

* all publishers of this topic reconnect to new channel

#### 3. all-pub replication --> consistent hashing

* all subs on the closing channel reconnect to another broker

* all pubs on the closing channel disconnect from it

* broker close the channel

#### 4. all-sub replication --> consistent hashing

* all pubs on the closing channel reconnect to another broker

* all subs on the closing channel disconnect from it

* broker close the channel

#### 5. all-pub replication: create new channels

* broker open new channel

* all pubs connect to the new channel

* some subs reconnect to the new channel

#### 6. all-sub replication: create new channels

* broker open new channel

* all subs connect to the new channel

* some pubs reconnect to the new channel

#### 7. all-pub replication: close channels

* all subs on the closing channel reconnect to some other brokers

* all pubs on the closing channel disconnect from it

* broker close the channel

#### 8. all-sub replication: close channels

* all pubs on the closing channel reconnect to some other brokers

* all subs on the closing channel disconnect from it

* broker close the channel

#### 9. consistent hashing: rehash

* open new channel

* all subs connect to the new channel

* all pubs reconnect to the new channel

* all subs disconnect from the old channel

* close the old channel

## Serialization/Deserialization
using [flatbuffer](https://google.github.io/flatbuffers/index.html)

see /dataSchema

## Test

* config zookeeper server

* compile EdgeBroker, Publisher, Subscriber. (need libzmq and zookeeper library)

* run LoadBalancer first, wait for system started (about 2 secs), run two EdgeBroker, run Publisher, run Subscriber.