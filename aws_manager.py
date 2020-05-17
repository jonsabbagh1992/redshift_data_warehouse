import configparser
import boto3
from time import sleep
import json

FIRST = 0
SLEEP_TIME = 5

class AWSManager:
    '''
    A Helper class designed to handle interactions with the AWS API when setting up/tearing down resources.
    
    The class reads in a config file that define all the required parameters.
    '''
    def __init__(self, config_file='dwh.cfg'):
        config = configparser.ConfigParser()
        config.read_file(open(config_file))

        self._KEY                    = config.get('AWS','KEY')
        self._SECRET                 = config.get('AWS','SECRET')
        self._DWH_CLUSTER_TYPE       = config.get("CLUSTER","DWH_CLUSTER_TYPE")
        self._DWH_NUM_NODES          = config.get("CLUSTER","DWH_NUM_NODES")
        self._DWH_NODE_TYPE          = config.get("CLUSTER","DWH_NODE_TYPE")
        self._DWH_CLUSTER_IDENTIFIER = config.get("CLUSTER","CLUSTER_IDENTIFIER")
        self._DWH_DB                 = config.get("CLUSTER","DB_NAME")
        self._DWH_DB_USER            = config.get("CLUSTER","DB_USER")
        self._DWH_DB_PASSWORD        = config.get("CLUSTER","DB_PASSWORD")
        self._DWH_PORT               = config.get("CLUSTER","DB_PORT")
        self._DWH_IAM_ROLE_NAME      = config.get("IAM", "DWH_IAM_ROLE_NAME")
        self._DWH_IAM_POLICY         = config.get("IAM", "DWH_IAM_POLICY")
        self._REGION_NAME            = config.get("REGION", "REGION_NAME")
        self._redshift =  boto3.client('redshift', 
                                       region_name=self._REGION_NAME,
                                       aws_access_key_id=self._KEY,
                                       aws_secret_access_key=self._SECRET)
        self._iam = boto3.client('iam',aws_access_key_id=self._KEY,
                     aws_secret_access_key=self._SECRET,
                     region_name=self._REGION_NAME)
    
    def cluster_exists(self):
        '''
        Evaluates if the cluster was succesfully created on the account.
        '''
        try:
            self._redshift.describe_clusters(ClusterIdentifier=self._DWH_CLUSTER_IDENTIFIER)['Clusters']
            return True
        except redshift.exceptions.ClusterNotFoundFault:
            return False
    
    def create_cluster(self):
        '''
        Creates an AWS cluster using the parameters defined by the config file 
        '''
        IAM_ROLE = self._iam.get_role(RoleName=self._DWH_IAM_ROLE_NAME)['Role']['Arn']
        try:
            response = self._redshift.create_cluster(        
                #HW
                ClusterType=self._DWH_CLUSTER_TYPE,
                NodeType=self._DWH_NODE_TYPE,
                NumberOfNodes=int(self._DWH_NUM_NODES),

                #Identifiers & Credentials
                DBName=self._DWH_DB,
                ClusterIdentifier=self._DWH_CLUSTER_IDENTIFIER,
                MasterUsername=self._DWH_DB_USER,
                MasterUserPassword=self._DWH_DB_PASSWORD,

                #Roles (for s3 access)
                IamRoles=[IAM_ROLE]  
            )
        except Exception as e:
            print(e)

    def wait_for_cluster_creation(self):
        '''
        Helper function that keeps looping until the cluster shows to be available in the AWS account.
        '''
        AVAILABLE_STATUS = 'available'
        if self.cluster_exists():
            cluster = self.get_cluster()
        else:
            raise self._redshift.exceptions.ClusterNotFoundFault
        while cluster['ClusterStatus'] != AVAILABLE_STATUS:
            sleep(SLEEP_TIME)
            cluster = self.get_cluster()
    
    def get_cluster(self):
        '''
        Returns the Redshift cluster used throughout this ETL pipeline.
        '''
        if self.cluster_exists():
            return self._redshift.describe_clusters(ClusterIdentifier=self._DWH_CLUSTER_IDENTIFIER)['Clusters'][FIRST]
    
    def get_cluster_endpoint(self):
        '''
        Returns the Redshift cluster endpoint used when connecting to the database.
        '''
        cluster = self.get_cluster()
        return cluster['Endpoint']['Address']
    
    def get_cluster_iam_role(self):
        '''
        Returns the Redshift cluster IAM role used when loading data.
        '''
        cluster = self.get_cluster()
        return cluster['IamRoles'][FIRST]['IamRoleArn']
        
    def get_region_name(self):
        return self._REGION_NAME
    
    def create_iam_role(self):
        '''
        Creates a new IAM role that will be used throughout this ETL pipeline.
        '''
        try:
            dwhRole = self._iam.create_role(
                Path='/',
                RoleName=self._DWH_IAM_ROLE_NAME,
                Description = "Allows Redshift clusters to call AWS services on your behalf.",
                AssumeRolePolicyDocument=json.dumps(
                    {'Statement': [{'Action': 'sts:AssumeRole',
                       'Effect': 'Allow',
                       'Principal': {'Service': 'redshift.amazonaws.com'}}],
                     'Version': '2012-10-17'})
            )
        except self._iam.exceptions.EntityAlreadyExistsException:
            print("Role already exists. Did not create.")
    
    def attach_policy(self):
        self._iam.attach_role_policy(RoleName=self._DWH_IAM_ROLE_NAME,
                           PolicyArn=self._DWH_IAM_POLICY
                          )['ResponseMetadata']['HTTPStatusCode']
            
    def delete_iam_role(self):
        try:
            self._iam.detach_role_policy(RoleName=self._DWH_IAM_ROLE_NAME, PolicyArn=self._DWH_IAM_POLICY)
            self._iam.delete_role(RoleName=self._DWH_IAM_ROLE_NAME)
        except self._iam.exceptions.NoSuchEntityException:
            print("IAM Role does not exist.")
    
    def delete_cluster(self):
        if self.cluster_exists():
            self._redshift.delete_cluster(ClusterIdentifier=self._DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)