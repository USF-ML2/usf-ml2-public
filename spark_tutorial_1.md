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

I won't assume knowledge of Python but will assume basic familiarity with operating in UNIX shell and with a scripting
languages similar to Python such as R or Matlab. We'll also make use of some basic concepts of object-oriented
programming.

