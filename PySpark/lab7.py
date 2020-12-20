# Bryan Francis
# Lab 7
# Average rating using spark
# <userid,movieid,rating>


import sys
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":

    # create spark context
    sc = SparkContext("local", "Avg rating")

    # read data from text file and split each line into workds
    val = sc.textFile("input.txt").map(lambda line:line.split(","))

    # count the occurance of each word
    result = val.map(lambda x: (str(x[0]), (str(x[1]), (float(x[2]), 1))))
    result = result.reduceByKey(lambda x,y: (x[1]+y[1], x[2]+y[2]))
    resultf = result.mapValues(lambda x: (x[0]/x[1], x[0]))

    # save result to an outputfile
    result.saveAsTextFile("output")
~                                                                                                                                                                                                           
~                                                                                                                                                                                                           
~                                                                                                                                                                                                           
~                                                                                                                                                                                                           
~                                                                                                                                                                                                           
~                                                                                                                                                                                                           
~                                                                                                                                                                                                           
~                                                                                                                                                                                                           
~                                                                                                                                                                                                           
~                                                                                                                                                                                                           
~                                                                                                                                                                                                           
~                                                                                                                                                                                                           
~                                                                                                                                                                                                           
~                         
