from __future__ import print_function
import os
import re
import subprocess
import shutil
import time
from time import localtime, strftime, sleep



# make sure collectd daemon is running before you run this script
# collectd plugin setting 
#
#<Plugin cpu>
#  ReportByCpu true
#  ReportByState true
#  ValuesPercentage true
#</Plugin>
#
#<Plugin memory>
#	ValuesAbsolute false
#	ValuesPercentage true
#</Plugin>
#
#<Plugin interface>
#	Interface "enp0s3"
#	IgnoreSelected false
#	ReportInactive true
#	UniqueName false
#</Plugin>


# set collectd data folder root path
DATA_ROOT_PATH = '/opt/collectd/var/lib/collectd/chuilian-VirtualBox-Ubuntu'

# cpu, network interfaces that we are interested in
CPU_SET = ['cpu-0']
INTERFACE_SET = ['interface-enp0s3']
MEMORY_SET = ['memory']

# get system date
TESTING_DATE = strftime("%Y-%m-%d", localtime())

# this file stores the latest metrics at the time of last query
LAST_METRICS_FOLDER = './lastMetrics'
NETWORK_LIMIT = 0.0



# get avg metrics between now and last query
def getMetrics():
	# get cpu metrics
	cpuUsage = []
	for cpu in CPU_SET:
		with open(DATA_ROOT_PATH + '/' + cpu + '/' + 'percent-idle-' + TESTING_DATE, 'r') as f:
			# save the last line
			lastLine = None
			# omit first line
			f.readline()
			# get average
			counter = 0
			datasum = 0
			for line in f:
				lastLine = line
				counter += 1
				data = line.split(',')
				datasum += float(data[1])
			# save result
			cpuUsage.append(100 - datasum/counter)
			# save last line
			directory = LAST_METRICS_FOLDER + '/' + cpu + '/' 
			with open(directory + 'percent-idle-' + TESTING_DATE, 'w') as lastf:
				print(lastLine, file=lastf)
		# clear the data file
		os.remove(DATA_ROOT_PATH + '/' + cpu + '/' + 'percent-idle-' + TESTING_DATE)

	# get memory metrics
	memUsage = []
	for mem in MEMORY_SET:
		with open(DATA_ROOT_PATH + '/' + mem + '/' + 'percent-used-' + TESTING_DATE, 'r') as f:
			# save the last line
			lastLine = None
			# omit first line
			f.readline()
			# get average
			counter = 0
			datasum = 0
			for line in f:
				lastLine = line
				counter += 1
				data = line.split(',')
				datasum += float(data[1])
			# save result
			memUsage.append(datasum/counter)
			# save last line
			directory = LAST_METRICS_FOLDER + '/' + mem + '/'
			with open(directory + 'percent-used-' + TESTING_DATE, 'w') as lastf:
				print(lastLine, file=lastf)
		# clear the data file
		os.remove(DATA_ROOT_PATH + '/' + mem + '/'  + 'percent-used-' + TESTING_DATE)

	# get network metrics
	# the calculation of network usage is different since collectd gives ACCUMULATIVE number of recived bytes, transmitted bytes of a interface
	# we here calculate the number of bytes received/transmitted per second in the time period between now and last query
	netUsage = []
	for interface in INTERFACE_SET:
		with open(DATA_ROOT_PATH + '/' + interface + '/' + 'if_octets-' + TESTING_DATE, 'r') as f:
			# save the last line
			lastLine = None
			# omit first line
			f.readline()
			for line in f:
				lastLine = line
			# read previous last line from file
			oldLastLine = None
			directory = LAST_METRICS_FOLDER + '/' + interface + '/' 
			with open(directory + 'if_octets-' + TESTING_DATE, 'r') as lastf:
				oldLastLine = lastf.readline()
			# get proper rx, tx rate
			oldData = oldLastLine.split(',')
			newData = lastLine.split(',')
			timeElapsed = float(newData[0]) - float(oldData[0])
			rxDiff = float(newData[1]) - float(oldData[1])
			txDiff = float(newData[2]) - float(oldData[2])
			res = [];
			# rx bytes/s
			res.append(rxDiff/timeElapsed)
			# rx usage rate -> bytes/s / limit
			res.append(rxDiff/timeElapsed/NETWORK_LIMIT)
			# tx bytes/s
			res.append(txDiff/timeElapsed)
			# tx usage rate -> bytes/s / limit
			res.append(txDiff/timeElapsed/NETWORK_LIMIT)
			netUsage.append(res)
			# save new last line to file
			with open(directory + 'if_octets-' + TESTING_DATE, 'w') as lastf:
				print(lastLine, file=lastf)
		# clear the data file
		os.remove(DATA_ROOT_PATH + '/' + interface + '/' + 'if_octets-' + TESTING_DATE)

	# print metric or any other output methods
	metricsOut(cpuUsage,memUsage,netUsage)

def metricsOut(cpu, mem, net):
	with open('test.out', 'a') as f:

		print('cpu usage:', file=f, end=' ')
		for entry in cpu:
			print(entry, file=f, end='% ')
		print('', file=f, end='\n')

		print('memory usage:', file=f, end=' ')
		for entry in mem:
			print(entry, file=f, end='% ')
		print('', file=f, end='\n')

		print('network usage:', file=f, end=' ')
		for entry in net:
			print("rx=%.2f (bytes/s) usage=%.2f %% tx=%.2f (bytes/s) usage=%.2f %%" % (entry[0], entry[1]*100, entry[2], entry[3]*100), file=f, end=' ')
		print('', file=f, end='\n')

def initLastMetrics():
	for cpu in CPU_SET:
		with open(DATA_ROOT_PATH + '/' + cpu + '/' + 'percent-idle-' + TESTING_DATE, 'r') as f:
			# save the last line
			lastLine = None
			# omit first line
			f.readline()
			for line in f:
				lastLine = line
			directory = LAST_METRICS_FOLDER + '/' + cpu + '/'
			if not os.path.exists(directory):
				os.makedirs(directory)
			with open(directory + 'percent-idle-' + TESTING_DATE, 'w') as lastf:
				print(lastLine, file=lastf)
	
	for mem in MEMORY_SET:
		with open(DATA_ROOT_PATH + '/' + mem + '/' + 'percent-used-' + TESTING_DATE, 'r') as f:
			# save the last line
			lastLine = None
			# omit first line
			f.readline()
			for line in f:
				lastLine = line
			directory = LAST_METRICS_FOLDER + '/' + mem + '/'
			if not os.path.exists(directory):
				os.makedirs(directory)
			with open(directory + 'percent-used-' + TESTING_DATE, 'w') as lastf:
				print(lastLine, file=lastf)
	
	for interface in INTERFACE_SET:
		with open(DATA_ROOT_PATH + '/' + interface + '/' + 'if_octets-' + TESTING_DATE, 'r') as f:
			# save the last line
			lastLine = None
			# omit first line
			f.readline()
			for line in f:
				lastLine = line
			directory = LAST_METRICS_FOLDER + '/' + interface + '/'
			if not os.path.exists(directory):
				os.makedirs(directory)
			with open(directory + 'if_octets-' + TESTING_DATE, 'w') as lastf:
				print(lastLine, file=lastf)

def getNetworkLimit():
	bashCommand = "sudo ethtool enp0s3"
	process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
	output, error = process.communicate()
	speedstr = re.findall('Speed: \d+', str(output))[0]
	limit = int(speedstr[7:])
	if not os.path.exists(LAST_METRICS_FOLDER):
		os.makedirs(LAST_METRICS_FOLDER)
	with open(LAST_METRICS_FOLDER + '/' + 'netLimit', 'w') as f:
		print(limit, file=f)
	global NETWORK_LIMIT
	# MegaBytes to Bytes
	NETWORK_LIMIT = limit * 1e6 /8 /10

if __name__ == '__main__':
	# get network interface limit
	getNetworkLimit()
	# clear data folder
	shutil.rmtree(DATA_ROOT_PATH)
	# wait for 20 second and get init metrics
	sleep(20)
	initLastMetrics()
	# collect and summarize system metrics every 20 seconds
	for i in range(3):
		sleep(20)
		getMetrics()



