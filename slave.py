import boto3, time
import modules
import numpy as np

def processMessage(msg):
	# print msg
	msgArray = modules.str2arr(msg)
	N = (len(msgArray) - 1)/2
	elements = []
	for i in range(0, (len(msgArray)-1), N/2):
		elements.append(msgArray[i:i+N/2])
	elements = np.array(elements)
	answer = np.dot(elements[0],elements[1]) + np.dot(elements[2],elements[3])
	answer = np.concatenate((answer,[msgArray[-1]]),axis=0)
	print "ANS"*30
	print answer
	modules.sendToQueue('qresult', modules.arr2str(answer))

def fetchMessage(queueName):
	sqs = boto3.resource('sqs')
	queue = sqs.get_queue_by_name(QueueName=queueName)
	client = boto3.client('sqs')

	# Get URL for SQS queue
	response = client.receive_message(
	    QueueUrl=queue.url,
	    MaxNumberOfMessages=1,
	)
	# print response
	try:
		messages = response['Messages']
		for msg in messages:
			body = msg['Body']
			print "Message Received: { %s } from Queue: { %s }" %(modules.str2arr(body), queueName)
			rhandle = msg['ReceiptHandle']
			del_response = client.delete_message(
			    QueueUrl=queue.url,
			    ReceiptHandle=rhandle
			)
			# print del_response
			return body
	except:
		return ""


def listen():
	while True:
		msg = fetchMessage('qinfo')
		if len(msg) == 0:
			print "Queue is empty"
			time.sleep(3)
		else:
			processMessage(msg)

if __name__ == '__main__':
	fetchMessage('qinfo')
	listen()

