# PubSubCoordZmq

## Edge Broker

### Key member variables
* channel map: a hash map mapping topics to its own channel object
* port list: a list storing available port numbers
* zkconnect: a object that is used for communicating with zookeeper server. Note: according to current design, all channels share the same zkconnect object.

### Work flow
when the edge broker starts, it will make a default channel object and start it.

## Message Channel

Message channel is an object that transfer messages of one topic from publisher to subscriber. To create a message channel, you must assign a topic to it.

### Work flow

When a message channel initializes and starts, it will require two available ports from the broker port list and then bind its receiver and sender socket to these two ports. 

Then it subscribe the specific topic it interested in and register itself to the zookeeper server by making a new node /topics/sometopic, and two subnode /sub, /pub and write its sender/receiver address information to them respectively. 

Then it will start two threads, one for receiving/sending messages, the other for monitoring znode information, if there is no publisher/subscriber connect to this channel, it will be closed by this monitor thread.

## Default Channel (Main Channel)

Default channel is a message channel that is used for transferring messages with new topics.

### Work flow

Default is essentially a message channel with some modifications including:

1. there should be one and only one default channel, and it should be created when the broker starts.
2. default channel will never be automatically closed, hence don't need a monitor thread.
3. in stead of receiving messages from one specific topic like a normal message channel, it will receive messages from all topics.
4. the default channel will be taking charge of making new message channels: for every message received, the main channel will look up the channel map, see if there is a channel for it, if there is not, make a new one.
5. every message should be sent to default channel if zookeeper server doesn't have a registered channel for it.

## Publisher

### Key member variables
* topic-sender map: a hash map mapping topics to its own sender object
* msg-buffer map: a hash map mapping topics to its own message buffer
* zkconnect: a object that is used for communicating with zookeeper server. Note: according to current design, all senders share the same zkconnect object.

### Work flow
When the publisher starts, it will try to get the receiver address of default channel from zookeeper server. If fails, just try again until the broker is online. After get the default receiver address of the broker, it will create a default sender which should then connect to the default channel of the broker.

Publisher provide a send(topic, message) method for user to send messages. for each message it gets from user, it will first look at the topic sender map, try to find a existed sender, if fails, it will then try to acquire the address of channel for this topic from zookeeper server, if fails, it will then try to get the default channel address. 

## Data Sender
Data sender is a object that sends messages of one topic, and it should connect to the specific channel in broker for that topic.

### Work flow
When the data sender starts, it will connect to given receiver address and register itself on the zookeeper server by creating a node under /sometopic/pub, then it will register a local message buffer for it, and start a thread that keep checking and sending messages from the buffer. (Note the sender provides a send(message) method for publisher to send messages to its internal message buffer.)

Note: once started, the sender will not automatically stopped when there are no messages in its sending buffer, it can only be removed by explicitly by publisher calling sender.stop();

## Default Sender
Every publisher born with a default sender, which directly connects to the default message channel of the broker. When there is not a message channel for one specific topic, the message will be sent through default sender.

## Subscriber
the subscriber has the similar structure like the publisher.

### Key member variables

* topic-receiver map.
* msg-buffer map.
* zkconnect.

### Work flow
When the subscriber starts, it will create a default receiver that connects to the default channel of the broker. And starts a processor thread that checks message buffers every 3 seconds. 

By default, the default receiver will subscribe nothing when it starts. To receive messages, we need use method subscribe(topic). subscribe method will first subscribe this topic in the default receiver so that the default receiver can receive the messages that relevant to this topic from default channel. Then it will try to get the topic channel address from zookeeper, if the channel not found, it throw an exception. (Here may need to change the logic, for now, all calls to subscribe method will fail if topic channel not found on the zookeeper server) If found, create a new receiver for it.

## Data Receiver
Data receiver is a object that receives messages of one topic, and it should connect to the specific channel in broker for that topic.

### Work flow
When the data receiver starts, it will connect to given sender address and register itself on the zookeeper server by creating a node under /sometopic/sub, then it will register a local message buffer for it, and start a thread that keep receiving and storing messages to the buffer.

## Default Receiver
Every publisher born with a default receiver, which directly connects to the default message channel of the broker. It behaves exactly like a normal receiver except:

1. default receiver may subscribe multiple topics
2. default receiver store received messages under the buffer named ""
3. default receiver cannot be stopped by calling unsubscribe(topic) from subscriber, it can be stopped by explicitly calling defaultreceiver.stop().

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

### Data format

data storing at "/topics" should follow following format:

1. "null" --> this node should be initialized with a string "null", which indicates no valid information yet

2. "IP_ADDRESS:PORT_NUMBER\nIP_ADDRESS:PORT_NUMBER" this information should always added by the broker's message channel. When channel established, the channel thread should go to zookeeper server update its address under proper node. The first line should be the channel's receiver address, and the second line should be the channel's sender address. 

data storing at "/topics/sometopic" must be "null"

data storing at "/topics/sometopic/pub" or "/topics/sometopic/sub" must be:

"IP_ADDRESS:PORT_NUMBER" which indicates where publisher/subscriber should connect to.