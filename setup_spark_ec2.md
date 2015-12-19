Setup Spark on an EC2-based cluster
==================

Create a key pair using this link
https://us-west-2.console.aws.amazon.com/ec2/v2/home?region=us-west-2#KeyPairs:sort=keyFingerprint

You will get a *.pem file
chmod 600 my-aws-key-pair.pem

Get Access Keys
https://console.aws.amazon.com/iam/home?nc2=h_m_sc#security_credential

From the rootkey.csv file get your
AWSAccessKeyId and AWSSecretKey

Create a cluster using 
elasticmapreduce

