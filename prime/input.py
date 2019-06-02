
from awsutils import qutils
from random import randint

sqs_handler = qutils.SQSConsumer()

for i in range(1, 100):
    start = randint(1, 1e9)
    input_args = {"start": start, "end": start + randint(1, 1e5)}
    sqs_handler.send_message(input_args)




