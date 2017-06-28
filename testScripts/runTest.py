#!/usr/bin/python                                                                            

from time import sleep
																				   
from mininet.topo import Topo
from mininet.net import Mininet
from mininet.util import dumpNodeConnections
from mininet.log import setLogLevel

PATH_LOGS = '/media/sf_SharedFolderWithMininetVM/TestEnv/logs/'
PATH_TMP = '/media/sf_SharedFolderWithMininetVM/TestEnv/tmp/'
PATH_ZOOKEEPER_SERVER = '/media/sf_SharedFolderWithMininetVM/TestEnv/zookeeper-3.4.10/bin/zkServer.sh'
PORT_ZOOKEEPER_SERVER = '2181'
PATH_ZOOKEEPER_CLIENT = '/media/sf_SharedFolderWithMininetVM/TestEnv/zookeeper-3.4.10/bin/zkCli.sh'
PATH_LOADBALANCER = '/media/sf_SharedFolderWithMininetVM/TestEnv/testScripts/runLoadBalancer.sh'
PATH_EDGEBROKER = '/media/sf_SharedFolderWithMininetVM/TestEnv/testScripts/runEdgeBroker.sh'
PATH_PUBLISHER = '/media/sf_SharedFolderWithMininetVM/TestEnv/testScripts/runPublisher.sh'
PATH_SUBSCRIBER = '/media/sf_SharedFolderWithMininetVM/TestEnv/testScripts/runSubscriber.sh'

PATH_ZOOKEEPER_SERVER_OUT = PATH_TMP + 'zkServer.out'
PATH_ZOOKEEPER_CLIENT_OUT = PATH_TMP + 'zkCli.out'
PATH_LOADBALANCER_OUT = PATH_TMP + 'loadBalancer.out'
PATH_EDGEBROKER_OUT = PATH_TMP + 'edgeBroker.out'
PATH_PUBLISHER_OUT = PATH_TMP + 'publisher.out'
PATH_SUBSCRIBER_OUT = PATH_TMP + 'subscriber.out'

HOST_NUM = 100
EDGEBROKER_NUM = 2
SUBSCRIBER_NUM = 1
PUBLISHER_NUM = 1

class SingleSwitchTopo(Topo):
	"Single switch connected to n hosts."
	def build(self, n):
		switch = self.addSwitch('s1')
		# Python's range(N) generates 0..N-1
		for h in range(n):
			host = self.addHost('h%s' % (h + 1))
			self.addLink(host, switch)

def testConnectivity(net):
	"Test network connectivity"
	print "* Dumping host connections"
	dumpNodeConnections(net.hosts)
	print "* Testing network connectivity"
	net.pingAll()

def testIPconfig(net):
	"Test IP configuration"
	print "* Testing IP configuration"
	for h in range(HOST_NUM):
		host = net.get('h' + str(h + 1))
		print "Host", host.name, "has IP address", host.IP(), "and MAC address", host.MAC()

def runZooKeeper(zkhost):
	"Run zookeeper server on a host"
	print "* Starting zookeeper server on host " + str(zkhost)
	#print PATH_ZOOKEEPER_SERVER + " start" +" > " + PATH_TMP + "zkServer.out" + " &"
	zkhost.cmd(PATH_ZOOKEEPER_SERVER + " start" + " > " + PATH_ZOOKEEPER_SERVER_OUT + " &" )

def stopZooKeeper(zkhost):
	"Stop zookeeper server on a host"
	print "* Stopping zookeeper server on host " + str(zkhost)
	#print PATH_ZOOKEEPER_SERVER + " stop" +" > " + PATH_TMP + "zkServer.out" + " &"
	zkhost.cmd("sudo " + PATH_ZOOKEEPER_SERVER + " stop" + " > " + PATH_ZOOKEEPER_SERVER_OUT + " &" )

def testZooKeeper(clihost,zkhost):
	"Testing zookeeper basic connection using zkCli"
	print "* Testing zookeeper connection"
	#print "sudo " + PATH_ZOOKEEPER_CLIENT + " -server " + str(zkhost.IP()) + ":" + PORT_ZOOKEEPER_SERVER + " > "+ PATH_TMP + "zkCli.out" + " &"
	clihost.cmd("sudo " + PATH_ZOOKEEPER_CLIENT + " -server " + str(zkhost.IP()) + ":" + PORT_ZOOKEEPER_SERVER + " > "+ PATH_ZOOKEEPER_CLIENT_OUT + " &")

def runLoadBalancer(lbhost):
	"Run load balancer on a host"
	print "* Starting loadbalancer on host " + str(lbhost)
	lbhost.cmd("sudo " + PATH_LOADBALANCER + " > " + PATH_LOADBALANCER_OUT + " &")

def runEdgeBroker(host):
	"Run edge broker on a host"
	print "* Starting edgeBroker on host " + str(host)
	host.cmd("sudo " + PATH_EDGEBROKER + " > " + PATH_EDGEBROKER_OUT + " &")

def runPublisher(host):
	"Run publisher on a host"
	print "* Starting publisher on host " + str(host)
	host.cmd("sudo " + PATH_PUBLISHER + " > " + PATH_PUBLISHER_OUT + " &")

def runSubscriber(host):
	"Run subscriber on a host"
	print "* Starting subscriber on host " + str(host)
	host.cmd("sudo " + PATH_SUBSCRIBER + " > " + PATH_SUBSCRIBER_OUT + " &")

def stopAllProc(host):
	"Kill all background processes running on a host"
	print "* Killing all background processes on host " + str(host)
	host.cmd('kill %while')

def stopAllHosts(hosts):
	"Kill all background processes running on the given host set"
	print "* Stopping all backgroud processes running on hosts set"
	for host in hosts:
		stopAllProc(host)

def simpleTest(net):
	zkhost = net.get('h1')
	runZooKeeper(zkhost)
	sleep(5)

	lbhost = net.get('h2')
	runLoadBalancer(lbhost)
	sleep(5)

	# host number 11 - 50 is reserved for edge brokers
	edgeBrokerHosts = []
	for n in range(EDGEBROKER_NUM):
		host = net.get('h' + str(11 + n))
		runEdgeBroker(host)
		edgeBrokerHosts.append(host)

	# host number 51 - 70 is reserved for subscribers
	subscriberHosts = []
	for n in range(SUBSCRIBER_NUM):
		host = net.get('h' + str(51 + n))
		runSubscriber(host)
		subscriberHosts.append(host)

	# host number 71 - 90 is reserved for publishers
	publisherHosts = []
	for n in range(PUBLISHER_NUM):
		host = net.get('h' + str(71 + n))
		runPublisher(host)
		publisherHosts.append(host)


	sleep(120)

	stopAllHosts(publisherHosts)
	sleep(2)
	stopAllHosts(subscriberHosts)
	sleep(2)
	stopAllHosts(edgeBrokerHosts)
	sleep(2)
	stopAllProc(lbhost)
	sleep(2)
	stopZooKeeper(zkhost)
	sleep(2)
	stopAllProc(zkhost)

if __name__ == '__main__':
	# Tell mininet to print useful information
	setLogLevel('info')
	# build a mininet with HOST_NUM hosts and 1 switch
	topo = SingleSwitchTopo(n=HOST_NUM)
	net = Mininet(topo)
	# start mininet
	net.start()
	# testing.....
	# testConnectivity(net)
	# testIPconfig(net)

	simpleTest(net)
	
	# stop mininet
	net.stop()