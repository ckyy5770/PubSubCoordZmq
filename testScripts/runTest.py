#!/usr/bin/python                                                                            

import os
from time import sleep
																				   
from mininet.topo import Topo
from mininet.net import Mininet
from mininet.util import dumpNodeConnections
from mininet.log import setLogLevel
from functools import partial
from mininet.node import Host

PATH_LOGS = '/media/sf_SharedFolderWithMininetVM/TestEnv/logs/'
PATH_TMP = '/media/sf_SharedFolderWithMininetVM/TestEnv/tmp/'
PATH_RESULT = '/media/sf_SharedFolderWithMininetVM/TestEnv/results/'
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

PUBLISHING_TIME_SCEONDS = 180
HOST_NUM = 100;
SUB_NUM_PER_HOST = 5
TEST_NAME = "PUB1_BROKER1_SUB100"
TEST_RESULT_PATH = PATH_RESULT + TEST_NAME + '/'

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

def printIPconfig(net):
	"Save IP configuration info to the private folder of each host"
	print "printing ip config info to private folders"
	for h in range(HOST_NUM):
		host = net.get('h' + str(h + 1))
		host.cmd('echo ' + str(host.IP()) + ' > ' + '/var/run/' + 'hostIP.config')


def runZooKeeper(zkhost):
	"Run zookeeper server on a host"
	print "* Starting zookeeper server on host " + str(zkhost)
	#print PATH_ZOOKEEPER_SERVER + " start" + " &> " + PATH_ZOOKEEPER_SERVER_OUT + " &" 
	zkhost.cmd(PATH_ZOOKEEPER_SERVER + " start" + " &> " + PATH_ZOOKEEPER_SERVER_OUT + " &" )

def stopZooKeeper(zkhost):
	"Stop zookeeper server on a host"
	print "* Stopping zookeeper server on host " + str(zkhost)
	#print PATH_ZOOKEEPER_SERVER + " stop" +" > " + PATH_TMP + "zkServer.out" + " &"
	zkhost.cmd("sudo " + PATH_ZOOKEEPER_SERVER + " stop" + " &> " + PATH_ZOOKEEPER_SERVER_OUT + " &" )

def testZooKeeper(clihost,zkhost):
	"Testing zookeeper basic connection using zkCli"
	print "* Testing zookeeper connection"
	#print "sudo " + PATH_ZOOKEEPER_CLIENT + " -server " + str(zkhost.IP()) + ":" + PORT_ZOOKEEPER_SERVER + " > "+ PATH_TMP + "zkCli.out" + " &"
	clihost.cmd("sudo " + PATH_ZOOKEEPER_CLIENT + " -server " + str(zkhost.IP()) + ":" + PORT_ZOOKEEPER_SERVER + " &> "+ PATH_ZOOKEEPER_CLIENT_OUT + " &")

def runLoadBalancer(lbhost):
	"Run load balancer on a host"
	print "* Starting loadbalancer on host " + str(lbhost)
	#print "sudo " + PATH_LOADBALANCER + " &> " + PATH_LOADBALANCER_OUT + " &"
	lbhost.cmd("sudo " + PATH_LOADBALANCER + " &> " + PATH_LOADBALANCER_OUT + " &")

def runEdgeBroker(host):
	"Run edge broker on a host"
	print "* Starting edgeBroker on host " + str(host)
	host.cmd("sudo " + PATH_EDGEBROKER + " &> " + PATH_TMP + 'edgeBroker' + str(host) + '.out' + " &")

def runPublisher(host):
	"Run publisher on a host"
	print "* Starting publisher on host " + str(host)
	host.cmd("sudo " + PATH_PUBLISHER + " &> " + PATH_PUBLISHER_OUT + " &")

def runSubscriber(host, instanceID):
	"Run one subscriber instance on a host"
	print "* Starting subscriber " + str(instanceID) + " on host " + str(host)
	host.cmd("sudo " + PATH_SUBSCRIBER + " &> " + TEST_RESULT_PATH + 'Host' + str(host) + 'Sub' + str(instanceID) + '.out' + " &")

def stopAllProc(host):
	"Kill all background processes running on a host"
	print "* Killing all background processes on host " + str(host)
	host.cmd('kill %while')

def stopAllHosts(hosts):
	"Kill all background processes running on the given host set"
	print "* Stopping all backgroud processes running on hosts set"
	for host in hosts:
		stopAllProc(host)

def test(hostnum, pubnum, subnum, ebrokernum):
	global HOST_NUM
	global TEST_NAME
	global TEST_RESULT_PATH

	HOST_NUM = hostnum
	# build a mininet with hostnum hosts and 1 switch
	topo = SingleSwitchTopo(n=hostnum)
	privateDirs = [ ( '/var/log', PATH_TMP + '/private/%(name)s/var/log' ), ( '/var/run', PATH_TMP + '/private/%(name)s/var/run' ) ]
	host = partial(Host, privateDirs=privateDirs)
	net = Mininet(topo=topo, host=host)
	# start mininet
	net.start()

	TEST_NAME = "_PUB" + str(pubnum) + "_EBROKER" + str(ebrokernum) + "_SUB" + str(subnum)
	TEST_RESULT_PATH = PATH_RESULT + TEST_NAME + '/'

	if not os.path.exists(TEST_RESULT_PATH):
		os.makedirs(TEST_RESULT_PATH)

	printIPconfig(net)
	sleep(5)

	zkhost = net.get('h1')
	runZooKeeper(zkhost)
	sleep(10)

	lbhost = net.get('h2')
	runLoadBalancer(lbhost)
	sleep(10)

	# host number 3 - 5 is reserved for edge brokers
	edgeBrokerHosts = []
	for n in range(ebrokernum):
		host = net.get('h' + str(3 + n))
		runEdgeBroker(host)
		edgeBrokerHosts.append(host)	
	sleep(10)

	# host number 11 - 10000 is reserved for subscribers
	subscriberHosts = []
	for n in range(subnum):
		host = net.get('h' + str(11 + n))
		for i in range(SUB_NUM_PER_HOST):
			runSubscriber(host, i)
		subscriberHosts.append(host)
	sleep(10)

	# host number 6 - 10 is reserved for publishers
	publisherHosts = []
	for n in range(pubnum):
		host = net.get('h' + str(6 + n))
		runPublisher(host)
		publisherHosts.append(host)
	sleep(5)

	sleep(PUBLISHING_TIME_SCEONDS)

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
	sleep(2)

	# stop mininet
	net.stop()

if __name__ == '__main__':
	# Tell mininet to print useful information
	setLogLevel('info')

	# testing.....
	# testConnectivity(net)
	# testIPconfig(net)
	# printIPconfig(net)

	startPubHostNum = 7
	step = 2
	time = 1

	for i in range(time):
		curSubNum = startPubHostNum	 + step * i
		test(hostnum = 10 + curSubNum, subnum = curSubNum, pubnum = 1, ebrokernum = 1)
	