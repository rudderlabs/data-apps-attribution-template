import boto3

# Ref: https://stackoverflow.com/questions/49622575/schedule-to-start-an-ec2-instance-and-run-a-python-script-within-it

def lambda_handler(event, context):
    ec2 = boto3.client("ec2", region_name="us-east-2") # change the region to your region
    ec2.start_instances(InstanceIds=["i-0aa7cc92b1bd542ea"]) # Change the instance id to your instance id
    print("Started your instances")
    return