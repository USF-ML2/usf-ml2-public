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


