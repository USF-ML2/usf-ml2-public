#!/bin/bash

REGION=us-west-2
CLUSTER_NAME=FILL
SPARK_DIR=~/spark-1.5.1/
export AWS_ACCESS_KEY_ID=FILL
export AWS_SECRET_ACCESS_KEY=FILL

cd ${SPARK_DIR}/ec2

./spark-ec2 --region=${REGION} --delete-groups destroy ${CLUSTER_NAME}

