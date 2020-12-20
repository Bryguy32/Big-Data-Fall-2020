# Lab 6
# Bryan Francis
# Get mMax raitng of each movie
# <userid, movieid, rating>

import sys
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":

    # create spark context
    sc = SparkContext("local", "Lab 6")

    # read input file 
    val = sc.textFile("input-1.txt").map(lambda line:line.split(","))

    # change unicode to int
    prcssval = val.map(lambda x: (str(x[0]), str(x[1]), float(x[2])))

    prcssval = prcssval.map(lambda x: (str(x[1]), float(x[2])))
    result = prcssval.reduceByKey(lambda x,y: x if x>y else y)

    # save result to an outputfile
    result.saveAsTextFile("output")

~                                                                      
