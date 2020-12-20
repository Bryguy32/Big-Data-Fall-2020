# Bryan Francis
# Lab 8
# Random tree classifier to predict for input feature 
# if pet is suitable or not

import sys
from pyspark.sql.types import DoubleType
from pyspark.mllib.regresssion import LabeledPoint
from pyspark import SparkContext, SparkConf
from pyspark.mllib.tree import RandomForest
from pyspark.mllib.linalg import Vectors

def parsePoint(line):
    values = [float(x) for x in line.split(",")]
    return LabeledPoint(values[4], values[1:4])

sc = SparkContext("local", "Pyspark MLrandomForest")
data = sc.textFile("input.txt")

parsedData = data.map(parsePoint)

model = RandomForest.trainClassifier(parsedData, numClasses=2, categoricalFeaturesInfo={}, numTrees = 3, impurity = 'gini', maxDepth=3, maxBins=32)
testData_1 = ([10,1,3,0])
prediction = model.predict(testData_1)
f = open("OUTPUT", "w+")
f.write("PREDICTION: " + str(prediction) + "\n")
f.close()

