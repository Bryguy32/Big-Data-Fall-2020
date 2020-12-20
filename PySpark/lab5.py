# Bryan Francis
# lab 5
# Find number of occurrences of each letter in dataset

import sys
from pyspark import SparkContext, SparkConf

if __name__=="__main__":

    sc = SparkContext("local", "PySpark word count")

    # Seperate words using space
    words = sc.textFile("input.txt").flatMap(lambda line:line.split(","))

    # count the occurance of each word
    wordCount = words.map(lambda x: (x, 1))

    # reducedbykey function to add all 1st associated with a word
    reducedWordcount = wordCount.reduceByKey(lambda a,b:a+b)

    # save the output into a textfile
    reducedWordcount.saveAsTextFile("output")

