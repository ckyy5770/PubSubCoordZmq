#!/usr/bin/python
from __future__ import print_function
import os

DATA_SET_PATH = "/media/sf_SharedFolderWithMininetVM/TestEnv/results/pr50ms/"
RESULT_PATH = "/media/sf_SharedFolderWithMininetVM/TestEnv/results/" + 'pr50ms.res'
CALCULATE_AVG_FROM_MSG = 1500


def isNumber(character):
	return character >= '0' and character <= '9'


if __name__ == '__main__':
	# clean up
	for testResultDir in os.listdir(DATA_SET_PATH):
		if testResultDir.endswith(".res"): os.remove(DATA_SET_PATH + testResultDir)
	for testResultDir in os.listdir(DATA_SET_PATH):
		if testResultDir[0] == '.': continue
		for filename in os.listdir(DATA_SET_PATH + testResultDir):
			if filename.endswith(".res"): os.remove(DATA_SET_PATH + testResultDir + '/' + filename)

	for testResultDir in os.listdir(DATA_SET_PATH):
		if testResultDir[0] == '.': continue
		# statistics of this testset
		allMissingMsgNum = 0
		allAvgCount = 0
		allLatencySum = 0
		allMsgNum = 0
		allAvgCount = 0

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
			with open(DATA_SET_PATH + testResultDir + '/' + filename) as rf:
				for line in rf:
					# valid data line should start with a number character
					if not isNumber(line[0]):
						continue
					# get msgID and latency from data line
					splitedLine = line.split(",");
					msgID = int(splitedLine[0])
					latency = int(splitedLine[1])
					# get the starting ID of this test
					if startID == -1:
						startID = msgID
					# real msg count
					msgCount += 1
					# get avg msg count and latency sum
					if msgID >= CALCULATE_AVG_FROM_MSG:
						latencySum += latency
						avgCount += 1
					# update last message
					lastID = msgID
			# get missing msg num
			missingMsgNum = (lastID - startID + 1) - msgCount
			# calculate avg latency
			if avgCount != 0:
				avgLatency = float(latencySum) / avgCount
			else:
				avgLatency = 0
			# write result to file
			wf = open(DATA_SET_PATH + testResultDir + '/' + filename[:-3]+ '.res', 'w')
			print("startID: " + str(startID), file=wf)
			print("lastID: " + str(lastID), file=wf)
			print("missingMsgNum: " + str(missingMsgNum), file=wf)
			print("avgLatency: " + str(avgLatency), file=wf)
			wf.close()
			# add to statistics for all
			allMissingMsgNum += missingMsgNum
			allLatencySum += latencySum
			allAvgCount += avgCount
			allMsgNum += msgCount

			allSubsNum += 1
			allStartID += startID
		# write overall statistics
		allf = open(DATA_SET_PATH + testResultDir + '.res','w')
		allAvgLatency = float(allLatencySum)/allAvgCount
		allAvgStartID = float(allStartID)/allSubsNum
		print("SubscriberNum: " + str(allSubsNum), file=allf)
		print("avgLatency: " + str(allAvgLatency), file=allf)
		print("avgStartID: " + str(allAvgStartID), file=allf)

		print("allMissingMsgNum: " + str(allMissingMsgNum), file=allf)
		print("allMsgNum: " + str(allMsgNum), file=allf)

		print("allAvgCount: " + str(allAvgCount), file=allf)
		print("allLatencySum: " + str(allLatencySum), file=allf)





