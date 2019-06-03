import boto3
from awsutils.settings import settings
import jsonpickle
import json


# Create a SQS queue
def create_sqs_queue(sqs, queue_params=None):
    if queue_params is None:
        queue_params = settings["queue"]

    try:
        response = sqs.create_queue(
            QueueName=queue_params["QueueName"],
            Attributes=queue_params["Attributes"]
        )
    except Exception as e:
        print("Error in creating sqs queue")

    return response


def get_sqs_queue(sqs, queue_name):
    print("getting url for queue name: " + queue_name)
    return sqs.get_queue_url(QueueName=queue_name)


def get_queue_attributes(sqs, queue_url, attributes=['All']):
    print("getting queue attributes for queue url: " + queue_url)
    return sqs.get_queue_attributes(sqs, QueueUrl=queue_url, Attributes=attributes)


def receive_messages(sqs, queue, MaxNumberOfMessages=1):
    print(
        "\nRUNNING SOLUTION CODE:",
        "get_messages!",
        "Follow the steps in the lab guide to replace this method with your own implementation.")
    return sqs.receive_message(
        QueueUrl=queue['QueueUrl'],
        AttributeNames=[
            'MessageGroupId'
        ],
        MaxNumberOfMessages=MaxNumberOfMessages,
    )


def delete_queue(sqs, queue_url):
    try:
        sqs.delete_queue(QueueUrl=queue_url)
    except Exception as e:
        print("failed to delete queue")


def delete_message(sqs, queue_url, receipt_handle):
    return sqs.delete_message(
        QueueUrl=queue_url,
        ReceiptHandle=receipt_handle
    )


class SQSConsumer:
    sqs = boto3.client('sqs', region_name=settings["region_name"])

    def __init__(self, queue_params=None, computeFn=None):

        self.queue_params = settings["queue"] if queue_params is None else queue_params
        self.queue = self.get_queue()

        if self.queue is None:
            try:
                self.queue = create_sqs_queue(self.sqs, self.queue_params)

                self.dlq = get_sqs_queue(self.sqs, settings["sqs_dlq"]["QueueName"])

                if self.dlq is not None:
                    dlq_attributes = get_queue_attributes(self.sqs, self.dlq["QueueUrl"], ["QueueArn"])
                    redrive_policy = {
                        'deadLetterTargetArn': dlq_attributes["attributes"]["QueueArn"],
                        'maxReceiveCount': '5'
                    }
                    self.sqs.set_queue_attributes(
                        QueueUrl=self.queue['QueueUrl'],
                        Attributes={
                        'RedrivePolicy' : json.dumps(redrive_policy)
                    })

            except Exception as err:
                print("Error Message {0}".format(err))

        self.queue_url = self.queue['QueueUrl']
        self.computeFn = (lambda x: True) if computeFn is None else computeFn
        self.results = list()

    def run(self):
        print("SQSConsumer running!")
        max_msgs = 10
        max_retry = 100
        num_msg = 0
        count = 0
        print("No. of Messages to consume:", max_msgs)
        while count < max_retry and num_msg < max_msgs:
            num_msg += self.receive_messages()
            count += 1
            print("Message No.:", count, num_msg)
        print("SQSConsumer stopped!")

    def get_queue(self):
        queue = None
        try:
            queue = get_sqs_queue(self.sqs, self.queue_params["QueueName"])
        except Exception as err:
            print("Error Message {0}".format(err))
        return queue

    def send_message(self, data):
        message_as_json = jsonpickle.encode(data)
        self.sqs.send_message(
            QueueUrl=self.queue_url,
            MessageBody=message_as_json,
            MessageGroupId='X1010'
        )

    def receive_messages(self):
        num_msgs = 0
        try:
            queue = self.queue
            if queue:
                messages = receive_messages(self.sqs, queue)
                if not len(messages):
                    print("There are no messages in Queue to display")
                    return num_msgs
                else:
                    num_msgs = len(messages)

                    for message in messages["Messages"]:
                        print(message)
                        process_status = self.process_message(message["Body"])
                        if process_status:
                            self.delete_message(message["ReceiptHandle"])
        except Exception as err:
            print("Error Message {0}".format(err))
        return num_msgs

    def delete_message(self, message_receipt_handle):
        try:
            delete_message(self.sqs, self.queue_url, message_receipt_handle)
            print("Message deleted from Queue")
            return True
        except Exception as err:
            print("Error Message {0}".format(err))
        return False

    def process_message(self, message):
            status = False
            arg = jsonpickle.decode(message)
            print("input args are: ")
            print(arg)
            try:
                result = self.computeFn(arg)
                self.results.append(result)
                status = True
            except Exception as e:
                print("failed to process")
            return status

    def delete_queue(self):
        try:
            self.sqs.delete_queue(QueueUrl=self.queue_url)
        except Exception as err:
            print("failed to delete queue: {}".format(err))


