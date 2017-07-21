#!/usr/bin/python
from __future__ import print_function
import os

DATA_SET_PATH = "/media/sf_SharedFolderWithMininetVM/TestEnv/results/msgPrioNonPQ/"
#RESULT_PATH = "/media/sf_SharedFolderWithMininetVM/TestEnv/results/" + 'pr10ms_5subperhost_5topics.res'
CALCULATE_AVG_FROM_MSG = 10000
TOPIC_NUM = 1
PRIORITY_NUM = 10

def isNumber(character):
	return character >= '0' and character <= '9'


class topicStats():
	def __init__(self):
		self.allMissingMsgNum = 0
		self.allAvgCount = 0
		self.allLatencySum = 0
		self.allMsgNum = 0
		self.allAvgCount = 0
		self.allSubsNum = 0
		self.allStartID = 0
		self.allAvgLatency = 0
		self.allAvgStartID = 0
		self.prioStats = []
		for i in range(PRIORITY_NUM):
			self.prioStats.append(prioStats())

class prioStats():
	def __init__(self):
		self.msgCount = 0
		self.latencySum = 0
		self.avgLatency = 0

if __name__ == '__main__':
	ignoreMsgMap = []
	ignoreMsgMap.append(10000)
	ignoreMsgMap.append(10000)
	ignoreMsgMap.append(10000)
	ignoreMsgMap.append(10000)
	ignoreMsgMap.append(10000)

	# clean up
	for testResultDir in os.listdir(DATA_SET_PATH):
		if testResultDir.endswith(".res"): os.remove(DATA_SET_PATH + testResultDir)
	for testResultDir in os.listdir(DATA_SET_PATH):
		if testResultDir[0] == '.': continue
		for filename in os.listdir(DATA_SET_PATH + testResultDir):
			if filename.endswith(".res"): os.remove(DATA_SET_PATH + testResultDir + '/' + filename)

	counter = 0
	for testResultDir in os.listdir(DATA_SET_PATH):
		if testResultDir[0] == '.': continue

		global CALCULATE_AVG_FROM_MSG
		CALCULATE_AVG_FROM_MSG = ignoreMsgMap[counter]
		counter += 1
		# statistics of this testset
		stats = [];
		for i in range(TOPIC_NUM):
			stats.append(topicStats())

		allSubsNum = 0
		allStartID = 0
		for filename in os.listdir(DATA_SET_PATH + testResultDir):
			startID = -1
			lastID = -1
			# for calculating missing messages
			msgCount = 0
			# for calculating avg latency
			latencySum = 0
			avgCount = 0

			msgTopic = None
			with open(DATA_SET_PATH + testResultDir + '/' + filename) as rf:
				for line in rf:
					# valid data line should start with a number character
					if not isNumber(line[0]):
						continue
					# get msgTopic, msgID and latency from data line
					splitedLine = line.split(",");
					msgTopic = int(splitedLine[0])
					if msgTopic == None:
						break
					msgID = int(splitedLine[1])
					msgPrio = int(splitedLine[2])
					latency = int(splitedLine[3])
					# get the starting ID of this test
					if startID == -1:
						startID = msgID
					# real msg count
					msgCount += 1
					# get avg msg count and latency sum
					if msgID >= CALCULATE_AVG_FROM_MSG:
						latencySum += latency
						avgCount += 1
						# priority related action
						stats[msgTopic].prioStats[msgPrio].msgCount += 1
						stats[msgTopic].prioStats[msgPrio].latencySum += latency
					# update last message
					lastID = msgID

			if msgTopic == None:
				continue
			# get missing msg num
			missingMsgNum = (lastID - startID + 1) - msgCount
			# calculate avg latency
			if avgCount != 0:
				avgLatency = float(latencySum) / avgCount
			else:
				avgLatency = 0
			# write result to file
			wf = open(DATA_SET_PATH + testResultDir + '/' + filename[:-4]+ '.res', 'w')
			print("startID: " + str(startID), file=wf)
			print("lastID: " + str(lastID), file=wf)
			print("missingMsgNum: " + str(missingMsgNum), file=wf)
			print("avgLatency: " + str(avgLatency), file=wf)
			wf.close()
			# add to statistics for all
			stats[msgTopic].allMissingMsgNum += missingMsgNum
			stats[msgTopic].allLatencySum += latencySum
			stats[msgTopic].allAvgCount += avgCount
			stats[msgTopic].allMsgNum += msgCount

			stats[msgTopic].allSubsNum += 1
			stats[msgTopic].allStartID += startID
		# write overall statistics
		allf = open(DATA_SET_PATH + testResultDir + '.res','w')
		for i in range(TOPIC_NUM):
			stats[i].allAvgLatency = float(stats[i].allLatencySum)/stats[i].allAvgCount
			stats[i].allAvgStartID = float(stats[i].allStartID)/stats[i].allSubsNum
			print("Topic: " + str(i), file=allf) 
			print("SubscriberNum: " + str(stats[i].allSubsNum), file=allf)
			print("avgLatency: " + str(stats[i].allAvgLatency), file=allf)
			print("avgStartID: " + str(stats[i].allAvgStartID), file=allf)

			print("allMissingMsgNum: " + str(stats[i].allMissingMsgNum), file=allf)
			print("allMsgNum: " + str(stats[i].allMsgNum), file=allf)

			print("allAvgCount: " + str(stats[i].allAvgCount), file=allf)
			print("allLatencySum: " + str(stats[i].allLatencySum), file=allf)

			for j in range(PRIORITY_NUM):
				count = stats[i].prioStats[j].msgCount;
				suml = stats[i].prioStats[j].latencySum;
				stats[i].prioStats[j].avgLatency = float(suml)/count
				print("priority " + str(j) + " avgLatency: " + str(stats[i].prioStats[j].avgLatency), file=allf)

		allf.close()




