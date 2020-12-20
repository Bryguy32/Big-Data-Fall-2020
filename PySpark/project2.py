# Bryan Francis
# Homework 2
# age, gender, heartCondition
# 1 = heart disease 0 = no disease
# 1= female 0 = male

# Download the Heart Disease data set from
# https://archive.ics.uci.edu/ml/machine-learning-databases/heart-disease/processed.hungarian.data
# Write Pyspark codes to answer the following questions:
# 1) What is the number of female patients with a heart problem within the age group of 40 to 60
# 2) What is the number of male patients with a heart problem for each age group of 40 of 60 

import sys
from pyspark import SparkContext, SparkConf
if __name__=="__main__":

    #Create spark context with necessary configuration
    sc = SparkContext("local", "Lab 2")

    #read data from text file and split each line into words
    val = sc.textFile("input.txt").map(lambda line: line.split(","))
    result = val.map(lambda x: (int(x[0]), int(x[1]), int(x[13])))
    result = result.map(lambda x: (1, (x[1], 1)) if (x[1] == 1 and x[0] > 40 and x[0] < 60 and x[2] == 1) else ((0, (x[1], 1)) if (x[1] == 0 and x[0] > 40 and x[0] < 60 and x[2] == 1) else (-1,(0,0))))
    result = result.reduceByKey(lambda x,y: (x[0], x[1]+y[1]))
    result = result.mapValues(lambda x: x[1])
    result = result.filter(lambda x: x[0] > 0)
    result.saveAsTextFile("output")
~                                      
