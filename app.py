from flask import Flask, request, redirect, abort, jsonify, Response, make_response
import ast
import requests
import os
import socket
import json
import math
import random
import operator
import time

app = Flask(__name__)
partitionList = []
causalPayload = []
nodeList = []
tupleList = {}
removedKeyLis = {}
K = 0
backup = {}
# Only used to keep # of replica nodes in a partition
partitionSize = []

timeBound = 0.25
# TODO (Add to it as needed):
#   - Node insertion rebalance
#   - Multiple nodes per partition
#   - Replication
#       - Upon receiving some change in node in partiton, push changes to all replicas
#   - Fault tolerance (single/multiple nodes and single/multiple partitions)
#       - Some Ideas:
#           - Everytime change happens, compare payloads of all nodes in partition
#               - This is to check if current node is behind, and if it is update itself
#               - If it sees that other nodes are behind, catch them up also

# Notes:
#   - PartitionList contains a list of nodes for each partition
#   - causalPayload establishes total ordering between nodes.
#       - updates (and merge) upon every client request received (GET/PUT/DELETE)
#       - updates (and merge) upon every successful node redirect received (GET/PUT/DELETE)
#
#   - partitionList and causalPayload are two dimensional lists
#       - partitionList = [['A', 'B', 'C'], ['D', 'E'], ['F']] -> 6 nodes total: 3 partitions
#       - causalPayload = [[0, 0, 0], [0, 0], [0]] -> stores vector clocks for each partition

# Testing ----------------------------------------------------------------------------------------
# Test global variables of container
@app.route("/kvs/test")
def test():
	string = "\n-------------------------------\n"
	string += str(nodeList) + "\n" + os.environ.get('ip_port') + '\n' + str(tupleList)
	try:
		string += "\n\nPartitions: " + str(partitionList)
		string += "\nCausal Payload: " + str(causalPayload)
		partitionIds = []
		for i in range(len(partitionList)):
			if partitionList[i]:
				partitionIds.append(i)
		string += "\nPartition Number: " + str(len(partitionIds))
		string += "\nK: " + str(K) + "\n"
	except:
		pass
	string += "-------------------------------\n"
	return string
# Test to see number of nodes each container recognizes
@app.route("/kvs/nodeCounter", methods = ['GET'])
def nodeCnter():
	length = len(nodeList)
	jsonString = jsonify(msg="success", length=length)
	return make_response(jsonString, 200, {'Content-Type':'application/json'})
# Test to check the nodes that are in each container's nodeList
@app.route("/kvs/nodeRan", methods = ['GET'])
def nodeRand():
	randV = random.choice(nodeList)
	jsonString = jsonify(msg="success", thev=randV)
	return make_response(jsonString, 200, {'Content-Type':'application/json'})
# Helper methods ---------------------------------------------------------------------------------
# Parses payload between 2 formats:
#   - list: [[0, 0, 0], [0, 0], [0]]
#   - string: 0.0.0.0.0.0
def parsePayload(payload, returnType):
	if returnType == 'list':
		if isinstance(payload, (list,)):
			return payload
		else:
			tempList = payload.split('.')
			payloadList = []
			index = 0
			for i in range(len(partitionList)):
				payloadList.append([])
				for j in range(len(partitionList[i])):
					payloadList[i].append(int(tempList[index]))
					index += 1
			return payloadList # If payload is a string, turn it into a list
	else:
		if isinstance(payload, (list,)): # If payload is a list, turn it into a string
			string = str(payload)
			return string.replace('[', '').replace(']', '').strip(', ').replace(', ', '.')
		else:
			return payload
# Hidden API functionalities ---------------------------------------------------------------------
# Updates nodeList with VIEW params at start
@app.before_first_request
def initializeNodes():
	# First batches of nodes are being created, load metadata
	if os.environ.get('VIEW') is not None:
		for ipPort in os.environ.get('VIEW').split(','):
			if ipPort not in nodeList:
				nodeList.append(ipPort)
	# Determine number of partitions and divide notes into partitions
	if os.environ.get('K') is not None:
		global K # Max nodes per partition
		K = int(os.environ.get('K'))
		if K not in partitionSize:
		  partitionSize.append(K)
		for i in range(int(math.ceil((len(nodeList) / K)))+1):
			partitionList.append([])
			causalPayload.append([])
		for node in nodeList:
			for i in range(len(partitionList)):
				if len(partitionList[i]) < K:
					partitionList[i].append(node)
					causalPayload[i].append(0)
					break
# Allows node to receive meta data to another node
@app.route("/kvs/get_metadata", methods = ['GET'])
def getMetadata():
	jsonString = jsonify(
		partitionList=str(partitionList), \
		causalPayload=str(causalPayload), \
		nodeList=str(nodeList), \
		tupleList=str(tupleList), \
		K=K
	)
	return make_response(jsonString, 200, {'Content-Type':'application/json'})
# Get key cnt of backup list
@app.route("/kvs/backupcnt", methods = ['GET'])
def backupcnt():
	jsonString = jsonify(
		backup_Count=len(backup)
	)
	return make_response(jsonString, 200, {'Content-Type':'application/json'})
# Transfer backup list from one node to another
@app.route("/kvs/sendbackuplist", methods = ['GET'])
def getBackupdata():
	jsonString = jsonify(
		backup=str(backup)
	)
	return make_response(jsonString, 200, {'Content-Type':'application/json'})
# Receives nodeList from other nodes, updates (Used when new node is added)
@app.route("/kvs/update_nodes", methods = ['PUT'])
def updateNodes():
	global nodeList
	nodeList = ast.literal_eval(request.values.get('view'))
	jsonString = jsonify(msg="success")
	return make_response(jsonString, 200, {'Content-Type':'application/json'})
# Receives partitionList from other nodes, updates, and creates a list for causalPayload
# Used when a new node is added
@app.route("/kvs/update_partitions", methods = ['PUT'])
def updatePartitions():
	global partitionList
	global K
	K = request.values.get('K')
	partitionList = ast.literal_eval(request.values.get('partitions'))
	# Create causalPayload with same structure as partitionList
	for i in range(len(partitionList)):
		causalPayload.append([])
		for j in range(len(partitionList[i])):
			causalPayload[i].append(0)
	jsonString = jsonify(msg="success")
	return make_response(jsonString, 200, {'Content-Type':'application/json'})
# Copies payload. Ensures payloads of replicas are in sync when request received
@app.route("/kvs/copy_payloads", methods = ['PUT'])
def copyPayloads():
	global causalPayload
	causalPayload = parsePayload(request.values.get('causal_payload'), 'list')
	jsonString = jsonify(msg="success")
	return make_response(jsonString, 200, {'Content-Type':'application/json'})
# Returns payload as response
@app.route("/kvs/get_payload", methods = ['GET'])
def getPayload():
	payload = parsePayload(causalPayload, 'string')
	jsonString = jsonify(causal_payload=payload)
	return make_response(jsonString, 200, {'Content-Type':'application/json'})
# Make nodes in partition consistent whenever we remove keys
@app.route("/kvs/kill_keys", methods = ['GET'])
def remKeys():
  key = (request.values.get('key'))
  tupleList.pop(key)
  jsonString = jsonify(msg="success")
  return make_response(jsonString, 200, {'Content-Type':'application/json'})
# Returns 1 random dict entry for a particular container
# & remove it from the container
@app.route("/kvs/return_dictEntries", methods = ['GET'])
def getNodeEntries():
	desiredPort = (request.values.get('desiredPort'))
	partition_id = (request.values.get('id'))
	randK,randV = random.choice(list(tupleList.items()))
	tupleList.pop(randK)
	forwardTo = "http://" + desiredPort + "/kvs/get_partition_members"
	req = requests.get(forwardTo, data={'partition_id':partition_id})
	memList = (req.json()['partition_members'])
	for i in memList:
	  if i != os.environ.get('ip_port'):
		forwardTo = "http://" + str(i) + "/kvs/kill_keys"
		req = requests.get(forwardTo, data={'key':randK})
	theLength = len(tupleList)
	jsonString = jsonify(theK=randK,theV=randV, theL = theLength)
	return make_response(jsonString, 200, {'Content-Type':'application/json'})
# Method to copy over data from a particular node
@app.route("/kvs/copy_over", methods = ['GET'])
def copyNodeEntries():
	reinsert = int(request.values.get('put_back'))
	if reinsert == 0:
	  randK,randV = random.choice(list(tupleList.items()))
	  removedKeyLis.update({randK:randV})
	  tupleList.pop(randK)
	  theLength = len(tupleList)
	  jsonString = jsonify(theK=randK, theV=randV, theL=theLength)
	else:
	  tupleList.update(removedKeyLis)
	  removedKeyLis.clear()
	  jsonString = jsonify(msg="success")
	return make_response(jsonString, 200, {'Content-Type':'application/json'})
# Created my own insertion method, because the already existing
# one was preventing my code from inserting into the newly inserted node
@app.route("/kvs/insertBalance", methods = ['PUT'])
def insertBalance():
	key = str(request.values.get('key'))
	value = str(request.values.get('value'))
	tupleList.update({key:value})
	jsonString = jsonify(msg="success")
	return make_response(jsonString, 200, {'Content-Type':'application/json'})
# For updating backup list
@app.route("/kvs/insertBalances", methods = ['PUT'])
def insertBalances():
	key = str(request.values.get('key'))
	value = str(request.values.get('value'))
	backup.update({key:value})
	jsonString = jsonify(msg="success")
	return make_response(jsonString, 200, {'Content-Type':'application/json'})
# Query backup list
@app.route("/kvs/checkbackuplist", methods = ['GET'])
def checkBackup():
	key = str(request.values.get('key'))
	value = backup.get(key, None)
	if value != None:
	  jsonString = jsonify(msg="success")
	else:
	  jsonString = jsonify(msg="error")
	return make_response(jsonString, 200, {'Content-Type':'application/json'})
# Obtaining Partition Information ----------------------------------------------------------------
# Returns partition id of node
@app.route("/kvs/get_partition_id", methods = ['GET'])
def getPartitionId():
	jsonString = jsonify(msg="error", error="partition id not available")
	for i in range(len(partitionList)):
		if os.environ.get('ip_port') in partitionList[i]:
			jsonString = jsonify(msg="success", partition_id=i)
	return make_response(jsonString, 200, {'Content-Type':'application/json'})
# Returns all partition ids that has nodes in them
@app.route("/kvs/get_all_partition_ids", methods = ['GET'])
def getAllPartitionIds():
	partitionIds = []
	for i in range(len(partitionList)):
		if partitionList[i]:
			partitionIds.append(i)
	jsonString = jsonify(msg="success", partition_id_list=partitionIds)
	return make_response(jsonString, 200, {'Content-Type':'application/json'})
# Returns all partition members of a specific partition id
@app.route("/kvs/get_partition_members", methods = ['GET'])
def getPartitionMembers():
	if request.values.get('partition_id') is None:
		jsonString = jsonify(msg="error", error="no partition id provided")
	else:
		i = int(request.values.get('partition_id'))
		if i >= len(partitionList):
			jsonString = jsonify(msg="error", error="partition id does not exist")
		else:
			jsonString = jsonify(msg="success", partition_members=partitionList[i])
	return make_response(jsonString, 200, {'Content-Type':'application/json'})
# Returns the number of keys within a node
@app.route("/kvs/get_number_of_keys", methods = ['GET'])
def getNumberOfKeys():
	jsonString = jsonify(count=len(tupleList))
	return make_response(jsonString, 200, {'Content-Type':'application/json'})
# Clear all keys within a node
@app.route("/kvs/clear_keys", methods = ['GET'])
def clearKeys():
	tupleList.clear()
	removedKeyLis.clear()
	jsonString = jsonify(msg="success")
	return make_response(jsonString, 200, {'Content-Type':'application/json'})

# View node contents
@app.route("/kvs/view_contents", methods=['GET'])
def viewContents():
  jsonString = jsonify(tupleList=str(tupleList), backup=str(backup))
  return make_response(jsonString, 200, {'Content-Type':'application/json'})
# Modifying Key Value Store ----------------------------------------------------------------------
# View updates for node insertions and deletions
@app.route("/kvs/view_update", methods = ['PUT'])
def viewUpdate():
	# Rebalance data upon deletion of a node
	def deleteRebalance():
		# Get counts all nodes & take special note of the key count in node to be deleted
		# Take average of all node counts (minus the 1 node container).
		# Redistribute random keys until no keys remain in node about to be deleted
		# Continually add to same node with lowest key until it has at least avg key count
		return
	# Rebalance data upon insertion of new node
	def insertRebalance(desiredPort, partition_id, number_of_partitions):
		# Check whether node is in an existing partition or new partition
		newPartition = False
		minNodes = partitionSize[0]
		forwardTo = "http://" + desiredPort + "/kvs/get_partition_members"
		req = requests.get(forwardTo, data={'partition_id':partition_id})
		memN = (req.json()['partition_members'])
		if len(memN) == 1:
		  newPartition = True
		# Clear existing keys of node to insert kv pairs in, in case we're reusing an old node
		#forwardTo = "http://" + desiredPort + "/kvs/clear_keys"
		#req = requests.get(forwardTo)
		# Insert node into an existing partition.
		if newPartition is False:
		  firstN = memN[0]
		  forwardTo = "http://" + firstN + "/kvs/get_number_of_keys"
		  req = requests.get(forwardTo, data={'redirect':1})
		  keyCounter = int(req.json()['count'])
		  keys = 0
		  while (keys < keyCounter):
			forwardTo = "http://" + firstN + "/kvs/copy_over"
			req = requests.get(forwardTo, data={'put_back':0})
			returnKey = str(req.json()['theK'])
			returnVal = str(req.json()['theV'])
			tupleList.update({returnKey:returnVal})
			forwardTo = "http://" + desiredPort + "/kvs/insertBalance"
			req = requests.put(forwardTo, data={'key':returnKey,'value':returnVal})
			keys += 1
		  # Tell node to reinsert back its own keys
		  forwardTo = "http://" + firstN + "/kvs/copy_over"
		  req = requests.get(forwardTo, data={'put_back':1})
		# Insert node into a new partition
		else:
		  # Get all parition ids besides the one the new node is in now
		  forwardTo = "http://" + desiredPort + "/kvs/get_all_partition_ids"
		  req = requests.get(forwardTo)
		  idList = req.json()['partition_id_list']
		  # Get all parition ids to check to get their first nodes
		  idTocheck = []
		  for i in idList:
			if i != partition_id:
			  idTocheck.append(i)
		  firstNodes = []
		  # Ping parition ids for the ip-ports of their first node
		  for i in idTocheck:
			forwardTo = "http://" + desiredPort + "/kvs/get_partition_members"
			req = requests.get(forwardTo, data={'partition_id':i})
			parMems = req.json()['partition_members']
			firstNo = parMems[0]
			firstNodes.append(firstNo)
		  nodeCnt = {}
		  # Ping the first node of every parition returned, & get their key counts
		  for i in firstNodes:
			if i != os.environ.get('ip_port'):
			  forwardTo = "http://" + i + "/kvs/get_number_of_keys"
			  req = requests.get(forwardTo, data={'redirect':1})
			  keyCounter = int(req.json()['count'])
			  nodeCnt.update({i:keyCounter})
			else:
			  # Record the key count of the node container you're on
			  yourIp = os.environ.get('ip_port')
			  tupleLength = len(tupleList)
			  nodeCnt.update({yourIp:tupleLength})
		  # Get avg count
		  # Get average count for nodes
		  avgNodes = 0
		  for k,v in nodeCnt.items():
			avgNodes += int(v)
		  avgNodes = math.floor(avgNodes / len(nodeList))
		  # Do same loop as last time, except every time we remove keys, we do it to every
		  # other node within the same partition

		  # Redistribute nodes until new node has at least avgNodes keys
		  newNodeKeys = 0
		  while (newNodeKeys < avgNodes) and bool(nodeCnt) != False:
			# Get node with highest key count (choose random, if tied)
			insertKey = max(nodeCnt.iteritems(), key=operator.itemgetter(1))[0]
			# Case where node is not the node you want to insert keys into
			# and is not the node you're currently on
			if insertKey != desiredPort and insertKey != os.environ.get('ip_port'):
				ipp = desiredPort
				numOfNodes = nodeCnt.get(insertKey, 0)
				diffCount = numOfNodes - avgNodes
				theLength = 0
				if diffCount > 0:
					theLength = diffCount
				# Check if we need to transfer nodes for chosen container
				while diffCount > 0:
					forwardTo = "http://" + insertKey + "/kvs/get_partition_id"
					req = requests.get(forwardTo)
					returnId = (req.json()['partition_id'])
					forwardTo = "http://" + insertKey + "/kvs/return_dictEntries"
					data = {
						'avgKeys':avgNodes, \
						'desiredPort':desiredPort, \
						'id':returnId \
					}
					req = requests.get(forwardTo, data=data)
					returnKey = str(req.json()['theK'])
					returnVal = str(req.json()['theV'])
					returnCount = str(req.json()['theL'])
					putKeysAt = "http://" + desiredPort + "/kvs/insertBalance"
					putReq = requests.put(putKeysAt, data={'key':returnKey,'value':returnVal})
					newNodeKeys  += 1
					diffCount -= 1
			# Case where we have to transfer keys to new node from the node we are currently on.
			# No need to ping other nodes & use this container's tupleList
			if insertKey != desiredPort and insertKey == os.environ.get('ip_port'):
				ipp = desiredPort
				numOfNodes = nodeCnt.get(insertKey, 0)
				diffCount = numOfNodes - avgNodes
				theLength = 0
				if diffCount > 0:
					theLength = diffCount
				while diffCount > 0:
					returnK,returnV = random.choice(list(tupleList.items()))
					tupleList.pop(returnK)
					id = 0
					for i in range(len(partitionList)):
					  if os.environ.get('ip_port') in partitionList[i]:
						id = i
					forwardTo = "http://" + desiredPort + "/kvs/get_partition_members"
					req = requests.get(forwardTo, data={'partition_id':id})
					myPartlist = (req.json()['partition_members'])
					for i in myPartlist:
					  if i != os.environ.get('ip_port'):
						forwardTo = "http://" + i + "/kvs/remKeys"
						req = requests.get(forwardTo, data={'key':returnK})
					putKeysAt = "http://" + desiredPort + "/kvs/insertBalance"
					putReq = requests.put(putKeysAt, data={'key':returnK,'value':returnV})
					newNodeKeys  += 1
					diffCount -= 1
			# Remove container so we don't have to take away keys from again
			# Also means that container has the average calculated amt of keys or less
			nodeCnt.pop(insertKey)
			# If new nodes needs more keys, we iterate to the node with next highest keyCount
			# within the while loop above, repeat the same process above in while loop

		# Return successfult JSON response
		jsonString = jsonify(msg="success")
		return make_response(jsonString, 200, {'Content-Type':'application/json'})
		#return
	# Add node into partitionList, nodeList, and causalPayload for all active nodes
	def addNode(node):
		nodeList.append(node) # Update nodeList with new node
		for i in range(len(partitionList)):
			if len(partitionList[i]) < int(os.environ.get('K')):
				partitionList[i].append(node) # Assign and update partition id to new node
				causalPayload[i].append(0) # Add a payload for new node
				return
		# If all partitions are full, create a new partition / payload
		partitionList.append([node])
		causalPayload.append([0])
	# Removes node from partitionList, nodeList, and causalPayload from all active nodes
	# Empty partitionList, nodeList, and causalPayload for node that is being delete
	def removeNode(node, origin):
		nodeList.remove(desiredPort)
		for i in range(len(partitionList)):
			if desiredPort in partitionList[i]: # Remove node and payload of removed node
				index = partitionList[i].index(node)
				del partitionList[i][index]
				del causalPayload[i][index]
		# Ensures number of node in partition is K by moving node from last partition
		# in partitionList to the partition that needs to statisfy K nodes
		for i in range(len(partitionList)):
			if len(partitionList[i]) < K and partitionList[i] != partitionList[-1]:
				lastPartition = [j for j in partitionList if j][-1] # last nonempty partiton
				index = partitionList.index(lastPartition)
				partitionList[i].append(partitionList[index][-1])
				causalPayload[i].append(0) # Add new payload for node
				partitionList[index].pop() # Remove that node
				causalPayload[index].pop() # Remove payload associated
				break
		# Determine number of partitions in node, before clearing lists
		numPartitions = 0
		for partition in partitionList:
			if partition:
				numPartitions += 1
		if node == os.environ.get('ip_port'): # If removed node is itself
			id_ = None
			for i in range(len(partitionList)):
				if os.environ.get('ip_port') in partitionList[i]:
					id_ = i
			placed = True
			while tupleList:
				key, value = tupleList.popitem()
				for node in nodeList:
					if node != origin and node != os.environ.get('ip_port'):
						while True:
							forwardTo = "http://" + node + "/kvs/insertBalance"
							req = requests.put(forwardTo, data={'key':key, 'value':value}, timeout=timeBound)
							if req.status_code == 200:
								break
			# Clear lists
			nodeList[:] = []
			partitionList[:] = []
			causalPayload[:] = []
		return numPartitions
	# ------------------------------------------
	desiredPort = str(request.values.get('ip_port'))
	requestType = str(request.values.get('type'))
	length = -100
	id_ = -100
	# If request was a redirect, add and remove nodes accordingly
	if request.values.get('redirect') is not None:
		if requestType == 'add' and desiredPort not in nodeList:
			addNode(desiredPort)
		elif requestType == 'remove' and desiredPort in nodeList:
			removeNode(desiredPort, request.values.get('origin'))
		jsonString = jsonify(msg="success")
	# If request was from a client, broadcast add and remove to all active nodes
	else:
		# Broadcast add and remove to active nodes to update
		for i in range(len(nodeList)):
			if nodeList[i] != os.environ.get('ip_port'):
				try:
					if requestType == 'remove':
						forwardTo = "http://" + nodeList[i] + "/kvs/view_update"
						data = {
							'ip_port':desiredPort, \
							'type':requestType, \
							'redirect':1, \
							'origin':os.environ.get('ip_port') \
						}
						req = requests.put(forwardTo, data=data, timeout=timeBound)
					if requestType == 'add':
						forwardTo = "http://" + nodeList[i] + "/kvs/view_update"
						data = {
							'ip_port':desiredPort, \
							'type':requestType, \
							'redirect':1 \
						}
					req = requests.put(forwardTo, data=data, timeout=timeBound)
				except:
					pass
		# Update metadata of current node and newly added node
		if requestType == 'add':
			length = -100
			id_ = -100
			if desiredPort not in nodeList:
				addNode(desiredPort)
				# Copy nodeList and partitionList to newly added node
				requests.put("http://" + desiredPort + "/kvs/update_nodes",
					data={'view':str(nodeList)})
				requests.put("http://" + desiredPort + "/kvs/update_partitions",
					data={'partitions':str(partitionList), 'K':K})
				# Create JSON return message
				req = requests.get("http://" + desiredPort + "/kvs/get_all_partition_ids")
				length = len(req.json()['partition_id_list'])
				req = requests.get("http://" + desiredPort + "/kvs/get_partition_id")
				id_ = req.json()['partition_id']
				jsonString = jsonify(msg="success", partition_id=id_, number_of_partitions=length)
			else:
				jsonString = jsonify(msg="error", error="node already exist")
		else:
			if desiredPort in nodeList:
				numPartitions = removeNode(desiredPort, os.environ.get('ip_port'))
				jsonString = jsonify(msg="success", number_of_partitions=numPartitions)
			else:
				jsonString = jsonify(msg="error", error="node does not exist")
	# Start of node insertion rebalancing method
	jsonSave = jsonString
	#if request.values.get('redirect') is None and requestType == 'add' and length > 0 and id_ != -100:
	   #insertRebalance(desiredPort, id_ , length)
	return make_response(jsonSave, 200, {'Content-Type':'application/json'})
# Key Value Store
@app.route("/kvs", methods = ['GET', 'PUT', 'DELETE'])
def kvs():
	# Gets partition id
	def getPartitionId():
		for i in range(len(partitionList)):
			if os.environ.get('ip_port') in partitionList[i]:
				return i
		return None
	# Sends GET with key and payload (string) to all active nodes
	def sendGetRequest(desiredKey, payload):
		for partition in partitionList:
			# Prevent redirecting to own partition and to partition with no nodes
			if os.environ.get('ip_port') not in partition and partition:
				forwardTo = "http://" + partition[1] + "/kvs"
				data = {
					'key':desiredKey, \
					'causal_payload':payload, \
					'redirect':1 \
				}
				req = requests.get(forwardTo, data=data)
				if req.status_code == 200: # Key is found
					value = req.json()['value']
					id_ = req.json()['partition_id']
					payload = req.json()['causal_payload']
					return value, id_, payload
			# Searches own dictionary for Key
			else:
				value = tupleList.get(desiredKey)
				# If key is found, return value, id, and payload of current node
				if value is not None:
					return value, getPartitionId(), payload
		# Return none to indicate no key is found within all nodes
		return None, None, None
	# Sends PUT request to all nodes in same partition
	def sendPutRequest(desiredKey, desiredValue):
		payload = parsePayload(causalPayload, 'string')
		for node in partitionList[getPartitionId()]:
			if node != os.environ.get('ip_port'):
				try:
					forwardTo = "http://" + node + "/kvs"
					data = {
						'key':desiredKey, \
						'value':desiredValue, \
						'causal_payload':payload, \
						'redirect':1, \
						'reredirect':1 \
					}
					req = requests.put(forwardTo, data=data, timeout=timeBound)
				except requests.ConnectionError:
					pass
	# Increase payload of current node by 1
	def updatePayload():
		global causalPayload
		id_ = getPartitionId()
		for i in range(len(partitionList[id_])):
			causalPayload[id_][i] += 1
	# Merges payload from client / other nodes with payload of current node with elementwise max
	def mergePayload(payload):
		global causalPayload
		for i in range(len(causalPayload)):
			for j in range(len(causalPayload[i])):
				if causalPayload[i][j] < payload[i][j]:
					causalPayload[i][j] = payload[i][j]
	# Forwards payload to every node in same partition
	def forwardPayload():
		for node in partitionList[getPartitionId()]:
			if node != os.environ.get('ip_port'):
				try:
					forwardTo = "http://" + node + "/kvs/copy_payloads"
					data = {'causal_payload':parsePayload(causalPayload, 'string')}
					req = requests.put(forwardTo, data=data, timeout=timeBound)
				except requests.ConnectionError:
					pass
	# Compares payload to payload of nodes in same partition to see if behind.
	# If current node is behind, return node with max payload
	def checkPayload():
		tempNodes = [] # Keeps track of which nodes cooresponds to which payloads
		sumPayloads = [] # Sum each version of payload to see if a node is behind
		id_ = getPartitionId()
		for i in range(len(partitionList[id_])):
			try:
				if partitionList[id_][i] != os.environ.get('ip_port'):
					forwardTo = "http://" + partitionList[id_][i] + "/kvs/get_payload"
					req = requests.get(forwardTo, timeout=timeBound)
					payload = parsePayload(req.json()['causal_payload'], 'list')
					sumPayloads.append(sum(payload[id_]))
					tempNodes.append(partitionList[id_][i])
				else:
					sumPayloads.append(sum(parsePayload(causalPayload[id_], 'list')))
					tempNodes.append(os.environ.get('ip_port'))
			except requests.ConnectionError:
				pass
		# Determine max and min
		minSum = min(sumPayloads)
		maxSum = max(sumPayloads)
		if minSum != maxSum: # A node is behind
			# Check if it is current node
			if sumPayloads.index(minSum) == tempNodes.index(os.environ.get('ip_port')):
				maxIndex = sumPayloads.index(maxSum)
				return tempNodes[maxIndex]
		return None

	def updateMetadata(node):
		global partitionList
		global causalPayload
		global nodeList
		global tupleList
		global K
		req = requests.get("http://" + node + "/kvs/get_metadata")
		# Assign highest payload node's meta data to current node
		partitionList = ast.literal_eval(req.json()['partitionList'].encode("utf-8"))
		causalPayload = ast.literal_eval(req.json()['causalPayload'].encode("utf-8"))
		nodeList = ast.literal_eval(req.json()['nodeList'].encode("utf-8"))
		tupleList = ast.literal_eval(req.json()['tupleList'].encode("utf-8"))
		K = int(req.json()['K'])
		# Get backup from other partitions if all nodes in partition was lost before
		if len(tupleList) == 0:
		  randK,randV = random.choice(list(tupleList.items()))
		  for i in partitionList:
			if len(i) != 0:
			  forwardTo = "http://" + str(i[0]) + "/kvs/checkbackuplist"
			  req = requests.get(forwardTo, data = {'key':randK})
			  if req.status_code == 200:
				if req.json()['msg'] == "success":
				  forwardTo = "http://" + str(i[0]) + "/kvs/sendbackuplist"
				  req = requests.get(forwardTo)
				  tupleList = ast.literal_eval(req.json()['backup'].encode("utf-8"))
				  return
		return
	# Stores data passed in by a request from client or other nodes
	desiredKey = str(request.values.get('key'))
	desiredValue = str(request.values.get('value'))
	inputPayload = request.values.get('causal_payload')
	if inputPayload is None or str(inputPayload) == '':
		inputPayload = causalPayload
	else:
		inputPayload = parsePayload(inputPayload, 'list')
	# Checks if key is valid (does not exceed 250 char length)
	if desiredKey is not None:
		if len(desiredKey) > 250:
			jsonString = jsonify(msg="error", error="key too long")
			return make_response(jsonString, 404, {'Content-Type':'application/json'})
	# Checks if node's payload is valid (up to date)
	if request.values.get('redirect') is None:
		highestPayload = checkPayload()
		# If current node's payload isn't the highest (up to date), get data from up to date node
		if highestPayload is not None:
			updateMetadata(highestPayload)
	# Record timestamp -------------------------
	timestamp = int(time.time())
	# GET Request ------------------------------
	if request.method == 'GET':
		# If request is a redirect, checks if current node has key
		if request.values.get('redirect') is not None:
			value = tupleList.get(desiredKey)
			if value is not None:
				updatePayload() # Update: GET is successful (key is found)
				mergePayload(inputPayload) # Updates payload to most recent (sent by partition)
				forwardPayload()
				jsonString = jsonify(msg="success", value=value, partition_id=getPartitionId(),
					causal_payload=parsePayload(causalPayload, 'string'))
		# If request is from a client, redirect request to all active nodes
		else:
			updatePayload() # Update: Received GET request from client
			value, id_, payload = sendGetRequest(desiredKey, parsePayload(causalPayload, 'string'))
			# If key is found
			if id_ is not None:
				mergePayload(parsePayload(payload, 'list'))
				forwardPayload()
				jsonString = jsonify(msg="success", value=value, partition_id=id_,
					causal_payload=parsePayload(causalPayload, 'string'), timestamp=timestamp)
		try:
			return make_response(jsonString, 200, {'Content-Type':'application/json'})
		except NameError:
			jsonString = jsonify(msg="error", error="key does not exist")
			return make_response(jsonString, 404, {'Content-Type':'application/json'})
	# PUT Request ------------------------------
	elif request.method == 'PUT':
		# Check if key has a corresponding value
		if desiredValue is None:
			jsonString = jsonify(msg="error", error="key does not have a corresponding value")
			return make_response(jsonString, 404, {'Content-Type':'application/json'})
		# Receive replicated PUT
		if request.values.get('reredirect') is not None:
			global causalPayload
			causalPayload = inputPayload
			tupleList.update({desiredKey:desiredValue})
			jsonString = jsonify(msg="success")
			return make_response(jsonString, 201, {'Content-Type':'application/json'})
		# If key already exist in system, update it
		if request.values.get('redirect') is None:
			value, id_, payload = sendGetRequest(desiredKey, parsePayload(causalPayload, 'string'))
			# If key is found
			if id_ is not None:
				updatePayload() # Update: PUT is successful (key found and updated)
				mergePayload(parsePayload(payload, 'list'))
				# If key is found in another active partiton
				if id_ != getPartitionId():
					forwardTo = "http://" + partitionList[id_][0] + "/kvs"
					data = {
						'key':desiredKey, \
						'value':desiredValue, \
						'causal_payload':parsePayload(causalPayload, 'string'), \
						'redirect':1 \
					}
					req = requests.put(forwardTo, data=data)
					mergePayload(parsePayload(req.json()['causal_payload'], 'list'))
					forwardPayload()
					# Update backups
					# forwardTo = "http://" + partitionList[id_][0] + "/kvs/get_all_partition_ids"
					# req = requests.get(forwardTo)
					# idlist = req.json()['partition_id_list']
					# Keep track of partition to be backup
					# update = -10
					# while update == -10:
					  # for i in idlist:
						# if i != getPartitionId():
						  # forwardTo = "http://" + partitionList[id_][0] + "/kvs/get_partition_members"
						  # data = {'partition_id':i}
						  # req = requests.get(forwardTo, data=data)
						  # if len(req.json()['partition_members']) != 0:
							# update = i
					  # update -= 1
					# if update >= 0:
					  # forwardTo = "http://" + partitionList[id_][0] + "/kvs/get_partition_members"
					  # data = {'partition_id':update}
					  # req = requests.get(forwardTo, data=data)
					  # membersL = req.json()['partition_members']
					  # for i in membersL:
						# forwardTo = "http://" + i+ "/kvs/insertBalances"
						# data = {'key':desiredKey,'value':desiredValue}
						# req = requests.put(forwardTo, data=data)
				# If key is in current partition
				else:
					tupleList.update({desiredKey:desiredValue})
					sendPutRequest(desiredKey, desiredValue)
					# partitionBackups = -10
					# for i in partitionList:
					  # if len(i) != 0:
						# if id_ not in i:
						  # partitionBackups = i
					# if partitionBackups != -10:
					  # for i in partitionBackups:
						# forwardTo = "http://" + i+ "/kvs/insertBalances"
						# data = {'key':desiredKey,'value':desiredValue}
						# req = requests.put(forwardTo, data=data)
				# Create JSON return
				jsonString = jsonify(msg="success", replaced=1, partition_id=id_,
					causal_payload=parsePayload(causalPayload, 'string'), timestamp=timestamp)
				return make_response(jsonString, 201, {'Content-Type':'application/json'})
		# If request is a redirect, store key/value pair
		if request.values.get('redirect') is not None:
			tupleList.update({desiredKey:desiredValue})
			updatePayload() # Update: PUT is successful (key is inserted)
			mergePayload(inputPayload)
			sendPutRequest(desiredKey, desiredValue)
			jsonString = jsonify(msg="success", partition_id=getPartitionId(),
				causal_payload=parsePayload(causalPayload, 'string'))
		# If request is from a client, redirect request to node with lowest key count
		else:
			# Determine key count of all active nodes, set id to partition with lowest key count
			if len(tupleList) == 0:
				id_ = getPartitionId()
			else:
				keyCount = []
				for partition in partitionList:
					# Prevent redirecting to own partition and to partitions with no nodes
					if os.environ.get('ip_port') not in partition and partition:
						req = requests.get("http://" + partition[0] + "/kvs/get_number_of_keys")
						keyCount.append(req.json()['count'])
					else:
						keyCount.append(len(tupleList))
				id_ = keyCount.index(min(keyCount))
			updatePayload() # Update: PUT is request (key inserted or request forwarded)
			# If lowest is current node add/update dictionary
			if id_ == getPartitionId():
				tupleList.update({desiredKey:desiredValue})
				sendPutRequest(desiredKey, desiredValue)
				partitionBackups = -10
				#while partitionBackups != -100:
				for i in partitionList:
					if len(i) != 0:
					  if os.environ.get('ip_port') not in i:
						partitionBackups = i
					  if partitionBackups != -10:
						for i in partitionBackups:
						  if i != os.environ.get('ip_port'):
							forwardTo = "http://" + str(i)+ "/kvs/insertBalances"
							data = {'key':desiredKey,'value':desiredValue}
							req = requests.put(forwardTo, data=data)
						#partitionBackups = -100
			# Otherwise redirect PUT to partition with lowest count
			else:
				forwardTo = "http://" + partitionList[id_][0] + "/kvs"
				data = {
					'key':desiredKey, \
					'value':desiredValue, \
					'causal_payload':parsePayload(causalPayload, 'string'), \
					'redirect':1 \
				}
				req = requests.put(forwardTo, data=data)
				mergePayload(parsePayload(req.json()['causal_payload'], 'list'))
				forwardPayload()
				#Update backups
				# forwardTo = "http://" + partitionList[id_][0] + "/kvs/get_all_partition_ids"
				# req = requests.get(forwardTo)
				# idlist = req.json()['partition_id_list']
				#Keep track of partition to be backup
				# update = -10
				# while update == -10:
				  # for i in idlist:
					# if i != getPartitionId():
					  # forwardTo = "http://" + partitionList[id_][0] + "/kvs/get_partition_members"
					  # data = {'partition_id':i}
					  # req = requests.get(forwardTo, data=data)
					  # if len(req.json()['partition_members']) != 0:
						# update = i
				  # update -= 1
				# if update >= 0:
					  # forwardTo = "http://" + partitionList[id_][0] + "/kvs/get_partition_members"
					  # data = {'partition_id':update}
					  # req = requests.get(forwardTo, data=data)
					  # membersL = req.json()['partition_members']
					  # for i in membersL:
						# forwardTo = "http://" + i+ "/kvs/insertBalances"
						# data = {'key':desiredKey,'value':desiredValue}
						# req = requests.put(forwardTo, data=data)
			jsonString = jsonify(msg="success", replaced=0, partition_id=id_,
				causal_payload=parsePayload(causalPayload, 'string'), timestamp=timestamp)
		return make_response(jsonString, 201, {'Content-Type':'application/json'})
	# DELETE Request ---------------------------
	else:
		pass
		# # Check to make sure we are provided a key to delete
		# if desiredKey is None:
		#   jsonString = jsonify(msg="error")
		#   return make_response(jsonString, 404, {'Content-Type':'application/json'})
		# # Check if remaining active nodes has key, by sending a GET request to each node
		# if request.values.get('redirect') is None: # Prevents deadlocks
		#   resp, statusCode, owner = sendGetRequest(desiredKey)
		#   if owner is not None:
		#       if owner == os.environ.get('ip_port'):
		#           tupleList.pop(desiredKey, None)
		#       else:
		#           forwardTo = "http://" + owner + "/kvs"
		#           data = {'key':desiredKey, 'value':desiredValue, 'redirect':1}
		#           req = requests.delete(forwardTo, data=data)
		#       jsonString = jsonify( msg="success", owner=owner)
		#       return make_response(jsonString, 200, {'Content-Type':'application/json'})
		#   else:
		#       jsonString = jsonify(msg="error",error="key does not exist")
		#       return make_response(jsonString, 404, {'Content-Type':'application/json'})
		# # If request is a redirect, store key/value pair
		# if request.values.get('redirect') is not None: # To prevent deadlocks
		#   tupleList.pop(desiredKey, None)
		#   owner = os.environ.get('ip_port')
		# jsonString = jsonify( msg="success", owner=owner)
		# return make_response(jsonString, 201, {'Content-Type':'application/json'})

if __name__ == "__main__":
	app.debug = True
	app.run(host='0.0.0.0', port=8080)
	app.config['JSON_AS_ASCII'] = False
