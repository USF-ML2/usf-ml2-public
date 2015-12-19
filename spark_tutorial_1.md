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

Note that transfer to your cluster is free, but transfer from your cluster can be expensive. 

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

#### Loading data directly from Amazon's S3

If your files are stored on Amazon's S3, you can load them directly to spark. We will cover this later.

# 2) Using Spark

Ok, now we're in a position to do computations with Spark.

We can run PySpark in the form of a Python command line interpreter session or in batch mode to run the code from a PySpark/Python code file.

To start PySpark in an interactive session, we do this
```{r, engine='bash'}
export PATH=${PATH}:/root/spark/bin
pyspark
```

## 2.1) Background

Spark is AMPLab's effort to create a version of Hadoop that uses memory as much as possible to reduce the speed of computation, particularly for iterative computations. So in addition to the distributed file system, you can think of Spark as providing you with access to distributed memory - the collective memory of all of the nodes in your cluster. However, that memory is still physically separated so you're not operating as if you had a single machine. But the abstractions provided by Spark allow you to do a lot of computations that would be a pain to program yourself even with access to a cluster and use of something like MPI for passing messages between machines.

## 2.2) Reading data into Spark and doing basic manipulations
#### Reading from HDFS

First we'll see how to read data into Spark from the HDFS and do some basic manipulations of the data.

```{r, eval=FALSE}
lines = sc.textFile('/data/airline')
lines.count()
```

The `textFile()` method reads the data from the airline directory, dealing with the compression and with the various files that data is split into.

Note that the first line seems to take no time at all. That is because Spark uses **lazy evaluation**, delaying evaluation of code until it needs to actually produce a result. So sometimes it can be hard to figure out how long each piece of a calculation takes because multiple pieces are being combined together. I'll sometimes do a `count()` just to force the evaluation of the previous command.  Transformations such as `map()` and `reduce()` do not trigger evaluation but actions such as `count()`, `collect()`, and `saveAsTextFile()` do.  Note also that serialization between Java and Python objects only occurs at barriers defined by actions, so if you have a series of transformations, you shouldn't incur repeated serialization.

You should see a bunch of logging messages that look like this:

```
14/10/31 01:15:53 INFO mapred.FileInputFormat: Total input paths to process : 22
14/10/31 01:15:53 INFO spark.SparkContext: Starting job: count at <stdin>:1
14/10/31 01:15:53 INFO scheduler.DAGScheduler: Got job 3 (count at <stdin>:1) with 22 output partitions (allowLocal=false)
14/10/31 01:15:53 INFO scheduler.DAGScheduler: Final stage: Stage 3(count at <stdin>:1)
14/10/31 01:15:53 INFO scheduler.DAGScheduler: Parents of final stage: List()
14/10/31 01:15:53 INFO scheduler.DAGScheduler: Missing parents: List()
14/10/31 01:15:53 INFO scheduler.DAGScheduler: Submitting Stage 3 (PythonRDD[16] at RDD at PythonRDD.scala:43), which has no missing parents
...
...
14/10/31 01:15:54 INFO storage.BlockManagerInfo: Added broadcast_8_piece0 in memory on ip-10-249-71-5.us-west-2.compute.internal:49964 (size: 4.7 KB, free: 3.1 GB)
14/10/31 01:15:54 INFO storage.BlockManagerInfo: Added broadcast_8_piece0 in memory on ip-10-249-25-57.us-west-2.compute.internal:37497 (size: 4.7 KB, free: 3.1 GB)
14/10/31 01:15:54 INFO storage.BlockManagerInfo: Added broadcast_8_piece0 in memory on ip-10-249-62-60.us-west-2.compute.internal:56086 (size: 4.7 KB, free: 3.1 GB)
14/10/31 01:15:57 INFO scheduler.TaskSetManager: Starting task 19.0 in stage 3.0 (TID 27, ip-10-249-31-110.us-west-2.compute.internal, ANY, 1243 bytes)
14/10/31 01:16:14 INFO scheduler.TaskSetManager: Finished task 0.0 in stage 3.0 (TID 6) in 21020 ms on ip-10-249-30-169.us-west-2.compute.internal (1/22)
14/10/31 01:16:29 INFO scheduler.TaskSetManager: Finished task 4.0 in stage 3.0 (TID 17) in 35919 ms on ip-10-249-15-15.us-west-2.compute.internal (2/22)
14/10/31 01:16:31 INFO scheduler.TaskSetManager: Finished task 9.0 in stage 3.0 (TID 14) in 38311 ms on ip-10-226-142-5.us-west-2.compute.internal (3/22)
14/10/31 01:16:37 INFO scheduler.TaskSetManager: Finished task 7.0 in stage 3.0 (TID 18) in 43942 ms on ip-10-249-30-169.us-west-2.compute.internal (4/22)
14/10/31 01:16:55 INFO scheduler.TaskSetManager: Finished task 2.0 in stage 3.0 (TID 10) in 62229 ms on ip-10-249-14-142.us-west-2.compute.internal (5/22)
...
```

There are 22 tasks because there are 22 data files. Each task is done as a single process on one of the nodes.

You should see there are about 123 million observations (rows) in the dataset (the dataset is an *RDD* object). More on RDDs in a bit.

#### What if the data are not one-line-per-row?

You can read in each file as an individual observation using `wholeTextFiles()`.

You can use `flatMap()` if each row contains multiple observations.

#### Reading into Spark from S3

If we wanted to read directly into Spark from S3 (again checking if we need s3 or s3n), we can do something like this, here reading the Google n-grams word co-occurrence dataset:

```
lines = sc.textFile("s3n://<AWS_ACCESS_KEY_ID>:<AWS_SECRET_ACCESS_KEY>@datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/1gram")
```

Note the ":" to separate the two parts of your AWS credential information.  That was to read from a set of files but you could also point to a specific individual file. And you may be able to use wildcards.

Note that if either of your AWD credential strings has a "/" in it, that can cause problems because it will be interpreted as a file path separator (":" would probably also cause similar problems). 

#### Repartitioning the input

To better distribute the data across the nodes, I'm going to repartition the dataset into more than 22 chunks.

```
lines = sc.textFile('/data/airline').repartition(192).cache()
```
And I also "cached" the RDD so that it is retained in memory for later processing steps.

We'll come back to both of these ideas in a bit.

### 2.2.2) Subsetting in Spark

The next thing we could do is subset the dataset using the `filter()` method. Here's an example:

```
sfo_lines = lines.filter(lambda line: "SFO" in line.split(',')[16]).cache()
sfo_lines.count()
```

The filtering function (in this case my anonymous (lambda) function) should produce a single boolean. I could have used a named function here too.

There are 2373910 observations corresponding to flights from SFO.

Note that the processing here shows 192 tasks, corresponding to the now 192 partitions.

### 2.2.3) Extracting data from an RDD

If the subset is sufficiently small we can pull it into Python as a standard Python object in the PySpark/Python session on our master node.

```{r, eval=FALSE}
sfo = sfo_lines.collect()
type(sfo)
# <type 'list'>
len(sfo)
# 2733910
sfo[1] # but remember Python indexing starts at 0
# u'1992,1,2,4,708,705,1243,1222,US,1118,NA,215,197,NA,21,3,SFO,MCI,1499,NA,NA,0,NA,0,NA,NA,NA,NA,NA'
```

Or I could save the subset as a new HDFS dataset that I could transfer back to the standard filesystem on the master using the HDFS `copyToLocal` command from the UNIX prompt on the master node.

```{r, eval=FALSE}
sfo_lines.saveAsTextFile('/data/airline-sfo')
```

### 2.2.4) Stratifying in Spark

Now suppose I want to use Spark to do some stratification. Here's a very basic stratification. We'll do it using MapReduce.

I'm going to group by departure/arrival airport pairs and then count the number of flights. I'll use a mapper, `createKeyValue()`, to create key-value pairs where the key is the route and the value is just a 1 and then I do a reduction, stratifying by key, where I simply count the records associated with each key by using `add` as the reduce function. In Python, the key-value pairs are just a 2-element tuple (I think a 2-element list would be fine), but the value could be arbitrarily more complicated than the scalar quantity 1 that I use here.

```{r, eval=FALSE}
from operator import add

def createKeyValue(line):
    vals = line.split(',')
    return(vals[16]+'-'+vals[17], 1)

routeCount = lines.map(createKeyValue).reduceByKey(add).collect()

mx = routeCount[0]
for i in xrange(len(routeCount)):
     if routeCount[i][1] > mx[1]:
        mx = routeCount[i]
```

Any guesses about the route with the most flights?

I wrote that as presented above to illustrate key-value pairs and using a map-reduce pair of functions, but for simply counting, I could have done it more efficiently like this:

```
routeCount2 = lines.map(createKeyValue).countByKey()
```

### 2.2.5) Getting a few elements for inspection and debugging

One useful thing is to be able to grab a single element or a few elements from an Spark dataset. This allows us to look at the structure of units in the dataset and test our code by applying our Python functions directly to the units.

```
oneLine = lines.take(1)
# test my createKeyValue() function:
createKeyValue(oneLine)
createKeyValue(oneLine[0])
keyValues = lines.map(createKeyValue).cache()
twoKeyValues = keyValues.take(2)
twoKeyValues
# [(u'AVP-PIT', 1), (u'LAX-IND', 1)]
twoKeyCounts = keyValues.reduceByKey(add).take(2)
# [(u'ORD-SEA', 101949), (u'MCI-MSY', 925)]
```

Make sure you know what kind of Python object is in use for each observation in the RDD - originally it's often just a text line from CSV, but after mapping it would often be a tuple or a list. It's good to check your code on individual elements or pairs of elements from the RDD obtained via `take()`. Note that sometimes things will be embedded in a list and you'll need to burrow into it as seen in the example where we had a list of length one containing the string.


## 2.3) Some fundamental ideas in MapReduce and Spark
Now that we've seen a few example computations, let's step back and discuss a few concepts.

### 2.3.1) RDDs

Spark datasets are called Resilient Distributed Datasets (RDDs). Spark operations generally take the form of a method applied to an RDD object and the methods can be chained together, as we've already seen.

Note also the *sc* object we used for reading from the HDFS was a *SparkContext* object that encapsulates information about the Spark setup. The `textfile()` function is a method used with SparkContext objects.


### 2.3.2) Caching

Spark by default will try to manipulate RDDs in memory if they fit in the collective memory of the nodes. You can tell Spark that a given RDD should be kept in memory so that you can quickly access it later by using the `cache()` method. To see the RDDs that are cached in memory, go to `http://<master_url>:4040` and click on the "Storage" tab.

If an RDD won't fit in memory, Spark will recompute it as needed, but this will involve a lot of additional computation. In some (but not all) cases in which there is not enough collective memory you may want it cached on disk. The [Spark programming guide](http://spark.apache.org/docs/latest/programming-guide.html#rdd-persistence) has more information.

### 2.3.3) Partitions and repartitioning

The number of chunks your dataset is broken into can greatly impact computational efficiency. When a map function is applied to the RDD, the work is broken into one task per partition.

You want each partition to be able to fit in the memory availalbe on a node, and if you have multi-core nodes, you want that as many partitions as there are cores be able to fit in memory.

For load-balancing you'll want at least as many partitions as total computational cores in your cluster and probably rather more partitions. The Spark documentation suggests 2-4 partitions (which they also seem to call *slices*) per CPU. Often there are 100-10,000 partitions. Another rule of thumb is that tasks should take at least 100 ms. If less than that, you may want to repartition to have fewer partitions.

As an example that we've already seen, the original airline dataset is in 22 partitions, one per input CSV file, and those partitions are different sizes because the data size varies by year. So it's a good idea to repartition the dataset as you're doing your initial manipulations on it, as we did above. But it did take a while to do that because of all the transferring of data across nodes.

### 2.3.4) MapReduce

MapReduce is a sort of meta-algorithm. If you can write your algorithm/computation as a MapReduce algorithm, then you should be able to implement it in Spark or in standard Hadoop, including Amazon's Elastic MapReduce framework.

The basic idea is that your computation be written as a one or more iterations over a *Map* step and a *Reduce* step. The map step takes each 'observation' (each unit that can be treated independently) and applies some transformation to it. Think of a mathematical mapping. The reduction step then takes the results and does some sort of aggregation operation that returns a summary measure. Sometimes the mapping step involves computing a key for each unit. The units that share the same key are then collected together and a reduction step may be done for each key value.

Actually, it's more general than that. We may have multiple map steps before any reduction or no reduction at all. Also `reduceByKey()` returns an RDD, so there could potentially be multiple reductions.

Reduction functions should be associative and commutative. It shouldn't matter what order the operations are done in or which observations are grouped with which others in order that the reduction can be done in parallel in a simple fashion.

We'll see a variety of MapReduce algorithms here so this should become more concrete.

In addition to map and reduce steps, we can explicitly pass shared data to all the nodes via a broadcast step. Broadcast variables allow the programmer to keep a read-only variable cached on each machine rather than shipping a copy of it with tasks. They can be used, for example, to give every node a copy of a large input dataset in an efficient manner.

## 2.4) Additional data processing in Spark

Now let's do some aggregation/summarization calculations. One might do this sort of thing in the process of reducing down a large dataset to some summary values on which to do the core analysis.

First we'll do some calculations involving sums/means that naturally fit within a MapReduce paradigm and then we'll do a median calculation, which is not associative and commutative so is a bit harder and wouldn't scale well.

### 2.4.1) Calculating means as a simple MapReduce task

Here we'll compute the mean departure delay by airline-month-origin-destination sets. So we'll create a compound key and then do reduction separately for each key value (stratum).

Our map function will exclude missing data and the header 'observations'.

```{r, engine='python'}
def mapper(line):
    vals = line.split(',')
    key = '-'.join([vals[x] for x in [8,1,16,17]])
    if vals[0] == 'Year' or vals[14] == 'NA':
       key = '0'
       delay = 0.0
       valid = 0.0
    else:
       delay = float(vals[14])
       valid = 1.0
    return(key, (delay, valid))


def reducer( (dep1, valid1), (dep2, valid2) ):
    return( (dep1 + dep2), (valid1 + valid2) )

mappedLines = lines.map(mapper).cache()
tmp = mappedLines.reduceByKey(reducer)
tmp
# PythonRDD[11] at RDD at PythonRDD.scala:43
results = tmp.collect()
results = tmp.collect()
results[0:3]
# [(u'EA-11-ATL-DEN', (1531.0, 265.0)), (u'NW-11-MSP-SDF', (3290.0, 882.0)), (u'DL-9-SEA-JFK', (5301.0, 577.0))]
means = [(val[0], val[1][0]/val[1][1]) for val in results if val[1][1] > 0.0]
means[0:3]
# [(u'EA-11-ATL-DEN', 5.777358490566038), (u'NW-11-MSP-SDF', 3.7301587301587302), (u'DL-9-SEA-JFK', 9.1871750433275565)]

```

Note that the result of the `reduceByKey()` is just another (small) RDD.


