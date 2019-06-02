settings = dict();

settings["s3"] = {
    # TODO 1: Set input.py bucket name (must be globally unique)
    "input_bucket": None,

    # TODO 2: Set output bucket name (must be globally unique)
    "output_bucket": "output-prime-ks"
};

settings["queue"] = {
    "QueueName": "sqs_prime.fifo",
    "Attributes": {
        "DelaySeconds": '60',
        "MessageRetentionPeriod": '86400',
        "FifoQueue": 'true',
        "ContentBasedDeduplication": 'true',
        "VisibilityTimeout": '300',
        "ReceiveMessageWaitTimeSeconds": '20'
    }
}

settings["sqs_dlq"] = {
    "QueueName": "dlq"
}

settings["region_name"] = 'ap-south-1'
