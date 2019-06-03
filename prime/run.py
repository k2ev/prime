import prime
from awsutils import s3utils, qutils, utils
from settings import settings
import json

file_name = "output.csv"


def wrapperFn(data):
    try:
        data["count"] = prime.find_primes(data["start"], data["end"])
        return data
    except ValueError as err:
        print("Compute failed with error: {0}".format(err))


sqs_handler = qutils.SQSConsumer(computeFn=wrapperFn)
sqs_handler.run()
results = sqs_handler.results

if results:
    utils.write_csv(file_name, results, results[0].keys())
    s3_handler = s3utils.S3Handler()
    s3_handler.write_output_bucket(settings["output_file"], settings["output_file"] + str(hash(json.dumps(results))))

