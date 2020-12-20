# Bryan Francis
# Project 10
# Dataframes in PySpark

# Download the Heart Disease data set from
# https://archive.ics.uci.edu/ml/machine-learning-databases/heart-disease/processed.hungarian.data
# Create  Pyspark Data Frame and perform appropriate operation to output -
# 1) Female patients with a heart problem within the age group of 40 to 60
# 2) Male patients with a heart problem for each age group of 40 of 60

# Perform Pyspark MapReduce on the data frames to find -
# 1) What is the number of female patients with a heart problem within the age group of 40 to 60
# 2) What is the number of male patients with a heart problem for each age group of 40 of 60 


import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

sc = SparkContext("local", "PySpark DF")
sqlContext = SQLContext(sc)

df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('input.txt')
df.show()

print("Female patients age group of 40 to 60")
female = df.filter((df.Sex == 0) & (df.Num == 1) & (df.Age >= 40) & (df.Age <= 60))
female.show()

print("Male patients age group of 40 to 60")
male = df.filter((df.Sex == 1) & (df.Age >= 40) & (df.Age <= 60) & (df.Num == 1))
male.show()

print("Female patients age group of 40 to 60:", (female.count()))
print("Male patients age group of 40 to 60:", (male.count()))

df.rdd \
        .map(lambda x: (int(x[0]), int(x[1]), int(x[13]))) \
        .map(lambda x: (1, x[1], (x[2], 1)) if (x[1] == 1 and x[0] > 40 and x[0] < 60 and x[2] == 1) else ((0, x[1], (x[2], 1)) if (x[1] == 0 and x[0] > 40 and x[0] < 60 and x[2] == 1) else (-1,(0,0,0)))) \
        .reduceByKey(lambda x,y: (x[0], x[1]+y[1])) \
        .mapValues(lambda x: x[1]) \
        .filter(lambda x: x[0] > 0) \
        .saveAsTextFile("output")
~                                                                                                                                                                                                           
~                                                                                                                                                                                                           
~                                                                                                                                                                                                           
~                                                                                                                                                                                                           
~                                                                                                                                                                                                           
~                                                                                                                                                                                                           
~                                                      
