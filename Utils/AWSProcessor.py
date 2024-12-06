# AWS credentials

def send_msg_to_aws(msg):
    pass

import boto3
## Get the service resource
sqs = boto3.resource('sqs')
def send_message_sqs(q_url, msg_body, message_attributes=None):
    queue_url = 'https://sqs.ap-south-1.amazonaws.com/863372932275/CandleData'
    resp = sqs.send_message(
    QueueUrl=queue_url,
    MessageBody=(
    msg_body
    )
    )
