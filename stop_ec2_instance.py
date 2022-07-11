import boto.ec2
import boto.utils
import logging
logger=logging.getLogger()
def stop_ec2():
    conn = boto.ec2.connect_to_region("us-east-2") # or your region
    # Get the current instance's id
    my_id = boto.utils.get_instance_metadata()['instance-id']
    logger.info(' stopping EC2 :'+str(my_id))
    conn.stop_instances(instance_ids=[my_id])

if __name__ == "__main__":
    stop_ec2()