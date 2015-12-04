#!/bin/bash

REGION=us-west-2
SSH_KEY_NAME=yan-usf
SSH_KEY_FILENAME=yan-usf.pem
CLUSTER_NAME=FILL
SPARK_DIR=~/spark-1.5.1/
export AWS_ACCESS_KEY_ID=FILL
export AWS_SECRET_ACCESS_KEY=FILL
cd ${SPARK_DIR}/ec2

./spark-ec2 -k ${SSH_KEY_NAME} -i ~/.ssh/${SSH_KEY_FILENAME} --region=${REGION} login ${CLUSTER_NAME}
