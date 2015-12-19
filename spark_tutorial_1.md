Spark Tutorial: Part 1
==================

based on the tutorial by
Chris Paciorek UC Berkeley
http://www.stat.berkeley.edu/scf/paciorek-spark-2014.html

I am assuming you have a cluster in EMR.

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

Spark allows one to write code in Java, Scala, and Python. We'll use the Python interface, called PySpark.

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
hadoop fs -mkdir /data/
hadoop fs -copyFromLocal /mnt/airline/*bz2 /data/
hadoop fs -ls /data
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
pyspark
```

## 2.1) Background

Spark is AMPLab's effort to create a version of Hadoop that uses memory as much as possible to reduce the speed of computation, particularly for iterative computations. So in addition to the distributed file system, you can think of Spark as providing you with access to distributed memory - the collective memory of all of the nodes in your cluster. However, that memory is still physically separated so you're not operating as if you had a single machine. But the abstractions provided by Spark allow you to do a lot of computations that would be a pain to program yourself even with access to a cluster and use of something like MPI for passing messages between machines.

## 2.2) Reading data into Spark and doing basic manipulations
#### Reading from HDFS

First we'll see how to read data into Spark from the HDFS and do some basic manipulations of the data.

```{r, eval=FALSE}
lines = sc.textFile('/data')
lines.count()
```

The `textFile()` method reads the data from the airline directory, dealing with the compression and with the various files that data is split into.

Note that the first line seems to take no time at all. That is because Spark uses **lazy evaluation**, delaying evaluation of code until it needs to actually produce a result. So sometimes it can be hard to figure out how long each piece of a calculation takes because multiple pieces are being combined together. I'll sometimes do a `count()` just to force the evaluation of the previous command.  Transformations such as `map()` and `reduce()` do not trigger evaluation but actions such as `count()`, `collect()`, and `saveAsTextFile()` do.  Note also that serialization between Java and Python objects only occurs at barriers defined by actions, so if you have a series of transformations, you shouldn't incur repeated serialization.

You should see a bunch of logging messages that look like this:

```
..
..
15/12/19 22:07:15 INFO TaskSetManager: Finished task 16.0 in stage 0.0 (TID 16) in 76332 ms on ip-172-31-12-78.us-west-2.compute.internal (19/22)
15/12/19 22:07:23 INFO TaskSetManager: Finished task 19.0 in stage 0.0 (TID 20) in 84165 ms on ip-172-31-12-78.us-west-2.compute.internal (20/22)
15/12/19 22:07:23 INFO TaskSetManager: Finished task 20.0 in stage 0.0 (TID 17) in 84345 ms on ip-172-31-12-76.us-west-2.compute.internal (21/22)
15/12/19 22:07:25 INFO TaskSetManager: Finished task 21.0 in stage 0.0 (TID 21) in 85443 ms on ip-172-31-12-76.us-west-2.compute.internal (22/22)
15/12/19 22:07:25 INFO DAGScheduler: ResultStage 0 (count at <stdin>:1) finished in 85.482 s
15/12/19 22:07:25 INFO YarnScheduler: Removed TaskSet 0.0, whose tasks have all completed, from pool 
15/12/19 22:07:25 INFO DAGScheduler: Job 0 finished: count at <stdin>:1, took 85.549686 s
123534991
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
lines = sc.textFile('/data').repartition(192).cache()
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

```{r, eval=FALSE, engine='python'}
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

```{r, eval=FALSE, engine='python'}
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

Spark by default will try to manipulate RDDs in memory if they fit in the collective memory of the nodes. You can tell Spark that a given RDD should be kept in memory so that you can quickly access it later by using the `cache()` method. 

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

Note that we wrote our own reduce function because Python lists/tuples don't add in a vectorized fashion so we couldn't just use `operator.add()`, but if we had made the values in the key-value pairs to be *numpy* arrays we could have just done `reduceByKey(add)`.

### 2.4.2) Calculating medians as a non-standard MapReduce task

This will be more computationally intensive because we don't have an associative and commutative function for reduction. Instead we need all the values for a given key to calculate the summary statistic of interest. So we use `groupByKey()` and then apply the median function to the RDD where each key has as its value the entire set of values for that key from the mapper. I suspect that Spark/Hadoop experts would frown on what I do here as it wouldn't scale as the strata sizes increase, but it seems fairly effective for these sizes of strata.

```{r, eval=FALSE, engine='python'}
def medianFun(input):
    import numpy as np
    if len(input) == 2:
        if len(input[1]) > 0:
            med = np.median([val[0] for val in input[1] if val[1] == 1.0])
            return((input[0], med))
        else:
            return((input[0], -999))
    else:
        return((input[0], -9999))

output = mappedLines.groupByKey()
output.count()
# 186116
medianResults = output.map(medianFun).collect()
medianResults[0:3]
# [(u'EA-11-ATL-DEN', 0.0), (u'NW-11-MSP-SDF', -3.0), (u'DH-8-HPN-ORD', -7.0)]

# here's code to show the iterable object produced by groupByKey()
dat = output.take(1)
dat[0][0]
len(dat[0][1])
[val for val in dat[0][1]]
```

Note that the median calculation is another map function not a reduce, because it's just a 1-1 transformation of the RDD that is returned by `groupByKey()`. But of course the size of the value associated with each key is much larger before the application of the `medianFun()` map step.

### 2.4.3) Stratifying data and exporting

Now suppose we wanted to create multiple datasets, one per stratum. This does not fit all that well into the MapReduce/HDFS paradigm. We could loop through the unique values of the stratifying variable using `filter()` and `saveAsTextFile()` as seen above. There are not really any better ways to do this. In previous tries, I used `sortByKey()` first and that improved speed but I've been running into errors recently when doing that, with Spark losing access to some of the worker tasks (executors). Here I'll stratify by departure airport.

```{r, eval=FALSE, engine='python'}
def createKeyValue(line):
    vals = line.split(',')
    keyVal = vals[16]
    return(keyVal, line)

keyedLines = lines.map(createKeyValue).cache()

keys = keyedLines.countByKey().keys()

for curkey in keys:
    fn = '/data/' + curkey
    keyedLines.filter(lambda x: curkey == x[0]).map(lambda x: x[1]).repartition(1).saveAsTextFile(fn)
```

The `repartition(1)` ensures that the entire RDD for a given key is on a single partition so that we get a single file for each stratum instead of having each stratum split into pieces across the different nodes hosting the HDFS. The mapper simply pulls out the value for each key-value pair, discarding the key.


## 2.5) Doing simulation via a simple MapReduce calculation

We'll do a simulation to estimate the value of $\pi$ as an embarrassingly parallel calculation using MapReduce.

Here's the Python code

```{r, eval=FALSE, engine='python'}
import numpy.random as rand
from operator import add

samples_per_slice = 1000000
num_cores = 24
num_slices = num_cores * 20

def sample(p):
    rand.seed(p)
    x, y = rand.random(samples_per_slice), rand.random(samples_per_slice)
    return sum(x*x + y*y < 1)

count = sc.parallelize(xrange(0, num_slices), num_slices).map(sample).reduce(add)
print "Pi is roughly %f" % (4.0 * count / (num_slices*samples_per_slice))
```

Let's piece that apart. The `parallelize()` call basically takes the numbers {0,1,2,...,num_slices-1} and treats them as the input "dataset". These are the initial units and then the transformation (the mapper) is to do the random number generation and compute the partial sum. The second argument to `parallelize()` indicates how many partitions to create, so this will correspond to the number of tasks that will be carried out in a subsequent map step. Using `paralllelize()` to create an RDD from an existing Python list is another way to create an RDD, in addition to reading data from an external storage system such as the HDFS.

A few comments here. First we could have sampled a single random vector, thereby calling `parallelize()` across numslices*samples_per_slice, with one task per {x,y} pair. But it's much more computationally efficient to do a larger chunk of calculation in each process since there is overhead in running each process. But we want enough processes to make use of all the processors collectively in our Spark cluster and probably rather more than that in case some processors do the computation more slowly than others. Also, if the computation involved a large amount of memory we wouldn't want to exceed the physical RAM available to the processes that work simultaneously on a node. However, we also don't want too many tasks. For example, if we try to have as many as 50 million parallel tasks, Spark will hang. Somehow the overhead in creating that many tasks is too great, though I need to look further for a complete explanation.

## 2.6) Random number generation in parallel

A basic approach to deal with parallel random number generation (RNG) is to set  the seed based on the task id, e.g., passed in via `parallelize()` above.  This doesn't guarantee that the random number streams for the different tasks will not overlap, but it should be unlikely. Unfortunately Python does not provide the L'Ecuyer algorithm, or other algorithms to ensure non-overlapping streams, unlike R and Matlab.

## 2.7) Using PySpark in batch (non-interactive mode)

We can also do that calculation as a batch job.

Here's the code in the file *piCalc.py*:

```{r, eval=FALSE, engine='python'}
import sys
import numpy.random as rand
from operator import add
from pyspark import SparkContext
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print >> sys.stderr, "Usage: spark-submit piCalc.py <total_samples> <slices>"
        exit(-1)
    sc = SparkContext()
    total_samples = int(sys.argv[1]) if len(sys.argv) > 1 else 1000000
    num_slices = int(sys.argv[2]) if len(sys.argv) > 2 else 2
    samples_per_slice = round(total_samples / num_slices)

    def sample(p):
        rand.seed(p)
        x, y = rand.random(samples_per_slice), rand.random(samples_per_slice)
        return sum(x*x + y*y < 1)

    count = sc.parallelize(xrange(0, num_slices), num_slices).map(sample).reduce(add)
    print "Pi is roughly %f" % (4.0 * count / (num_slices*samples_per_slice))

```

Note that by starting Python via PySpark, the *sc* object is created and available to you, but for batch jobs we need to import *SparkContext* and instantiate the *sc* object ourselves.

And here's how we run it from the UNIX command line on the master node
```{r, eval=FALSE}
spark-submit piCalc.py 100000000 1000
```

<!--
# pyspark piCalc.py `cat /root/spark-ec2/cluster-url` 100000000 1000
-->

