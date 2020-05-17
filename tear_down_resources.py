from aws_manager import AWSManager

def tear_down_resources():
    '''
    Cleanup phase - tears down all create AWS resources during this ETL process.
    '''
    aws_manager = AWSManager()
    print("Deleting IAM Role & Attached Policy.")
    aws_manager.delete_iam_role()
    print("Deleting Cluster. This may take a few minutes to reflect in your account.")
    aws_manager.delete_cluster()
    
if __name__ == '__main__':
    tear_down_resources()