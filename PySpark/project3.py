# Bryan Francis
# Project 3

# Download the Heart Disease data set from
# https://archive.ics.uci.edu/ml/machine-learning-databases/heart-disease/processed.hungarian.data
# 1. Implement RandomForest machine learning method using Pyspark to predict 
# if a patient has a heart disease. Columns with no input ("?") should be replaced with 0.
# 2. Test your model for the following feature vector -
# [65,1,4,130,275,0,1,115,1,1,2,?,?]

import sys
import csv
from pyspark.sql.types import DoubleType
from pyspark.mllib.regression import LabeledPoint
from pyspark import SparkContext, SparkConf
from pyspark.mllib.tree import RandomForest
from pyspark.mllib.linalg import Vectors

lines = []

def fixInput(testData):


    with open('processedHungarian.txt') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter =',')

        for line in csv_reader:
            values = [float(0) if x == '?' else float(x) for x in line]
            lines.append(values)


    with open('input.txt', 'w') as csv_file:
        csv_writer = csv.writer(csv_file, delimiter =',')
        for i in lines:
            csv_writer.writerow(i)

def parsePoint(line):
    fixInput(line)

    with open('input.txt') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter =',')
        for line in csv_reader:
            values = [float(x) for x in line]
            break

    return LabeledPoint(values[12], values[0:12])

sc = SparkContext("local", "Project 3")
data = sc.textFile("processedHungarian.txt")

parsedData = data.map(parsePoint)

model = RandomForest.trainClassifier(parsedData, numClasses=2, categoricalFeaturesInfo= {}, numTrees = 3, impurity = 'gini', maxDepth=14, maxBins=32)
testData_1 = ([65,1,4,130,275,0,1,115,1,1,2,'?','?'])
prediction = model.predict(testData_1)
f = open("OUTPUT", "w+")
f.write("PREDICTION: " + str(prediction) + "\n")
f.close() 
