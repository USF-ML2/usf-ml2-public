Spark Tutorial: Part 1
==================

based on the tutorial by
Chris Paciorek UC Berkeley
http://www.stat.berkeley.edu/scf/paciorek-spark-2014.html



To get the airline dataset we will be using as a running example, you can download from [this
URL](http://www.stat.berkeley.edu/share/paciorek/1987-2008.csvs.tgz).
The demo code later in this document will do the downloading automatically.

# 1) Introduction

## 1.1) Hadoop, MapReduce, and Spark

The goal of this tutorial is to introduce Hadoop-style distributed computing for statistical use. We'll focus on using
MapReduce algorithms for data preprocessing and statistical model fitting. We'll make use of the Hadoop distributed file
system (HDFS) for storing data and of Amazon's EC2 for creating virtual Linux clusters on which to do our computation.

To do all this we'll be making use of [Spark](http://spark.apache.org), a project out of the AMPLab here at Berkeley
that aims to greatly increase the speed of Hadoop MapReduce by keeping data in memory when possible.

Spark allows one to write code in Java, Scala, and Python. We'll use the Python interface, called PySpark, as Python is
more widely known in the Statistics and Economics communities and should be easier to get started with for those who
don't know any of those languages.

### 1.2 Getting data onto your cluster and onto the HDFS

Here are some options.

#### Copying to the master node followed by copying to the HDFS

You can use `scp` to copy files to the master node. The easiest thing is to be logged onto the master node and scp from your local machine to the master. But you can also use the IP address from `/root/ephemeral-hdfs/conf/masters` to scp from your local machine to the master, making sure to use the `-i ~/.ssh/${SSH_KEY_FILENAME}` flag to point to your private key.

We'll copy the ASA Data Expo Airline dataset:

```
mkdir /mnt/airline
cd /mnt/airline
wget http://www.stat.berkeley.edu/share/paciorek/1987-2008.csvs.tgz
tar -xvzf 1987-2008.csvs.tgz
```

Note that transfer to your cluster is free, but transfer from your cluster can be expensive. Campus is setting up a portal to AWS to greatly reduce that cost. Email consult@stat.berkeley.edu or consult@econ.berkeley.edu for more information.

Once you have files on the master you can set up directories in the HDFS as desired and copy the dataset onto the HDFS as follows. Often your dataset will be a collection of (possibly zipped) plain text files, such as CSVs. You don't need to unzip the files - Spark can read data from zipped files.


```{r, engine='bash'}
export PATH=$PATH:/root/ephemeral-hdfs/bin/
hadoop fs -mkdir /data/airline
hadoop fs -copyFromLocal /mnt/airline/*bz2 /data/airline
hadoop fs -ls /data/airline
```

Note that the commands to interact with the HDFS are similar to UNIX commands (`mkdir`, `ls`, etc.) but they must follow the `hadoop fs -` syntax.

Note that in general you want your dataset to be split into multiple chunks but not so many that each chunk is tiny (HDFS has trouble when there are millions of files -- see [this explanation](http://blog.cloudera.com/blog/2009/02/the-small-files-problem/), nor so few that the dataset can't be distributed across the nodes of the distributed file system or cannot be distributed roughly equally in size across the nodes. However, in terms of the latter issues, one can "repartition" the data in Spark.

If you've done some operations and created a dataset (called *airline-sfo* here) on the HDFS that you want to transfer back to the master node, you can do the following. Note that this may give you back a dataset broken into pieces (one per Spark partition) so you may need to use UNIX tools to combine into one file.

```{r, engine='bash'}
hadoop fs -copyToLocal /data/airline-sfo /mnt/airline-sfo
```

#### Copying directly from Amazon's S3

If the files are stored on Amazon's S3, you can copy directly onto the HDFS. Here's how, using the Google ngrams dataset as an example. Note that we need Hadoop's MapReduce processes running to do this distributed copy operation and Spark does not start these by default. (For doing MapReduce within Spark, we do NOT need to do this.)

```
# start MapReduce processes
/root/ephemeral-hdfs/bin/start-mapred.sh
hadoop fs -mkdir /data/ngrams
hadoop distcp -D fs.s3n.awsAccessKeyId="<AWS_ACCESS_KEY_ID>" -D fs.s3n.awsSecretAccessKey="<AWS_SECRET_ACCESS_KEY>" \
   s3n://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/1gram hdfs:///data/ngrams
```

You'll need to use your AWS account information in place of the `<AWS_ACCESS_KEY_ID>` and `<AWS_SECRET_ACCESS_KEY>` strings here. Also check to see whether you need "s3" or "s3n" for the dataset you are interested in. Also, you may want to see if the dataset is stored in a particular AWS region as this may incur additional charges to transfer across regions. If so you may want to start up your cluster in the region in which the data reside.


