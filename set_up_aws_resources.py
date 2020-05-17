from aws_manager import AWSManager

def set_up_aws_resources():
    '''
    - Creates the required IAM role to access data from S3
    - Creates a new Redshift Cluster
    '''
    aws_manager = AWSManager()
    
    print("Creating IAM role")
    aws_manager.create_iam_role()
    
    print("Attaching policy to IAM role\n")
    aws_manager.attach_policy()
    
    print("Creating Redshift Cluster. This might take a few minutes...")
    aws_manager.create_cluster()
    aws_manager.wait_for_cluster_creation()
    print("Success! Cluster created.\n")
    
if __name__ == '__main__':
    set_up_aws_resources()