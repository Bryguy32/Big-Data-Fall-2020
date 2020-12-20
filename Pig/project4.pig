/* 
Bryan Francis
Project 4

Download the Heart Disease data set from
https://archive.ics.uci.edu/ml/machine-learning-databases/heart-disease/processed.hungarian.data

Write PIG codes to answer the following questions
1) What is the number of female patients with a heart problem within the age group of 40 to 60
2) What is the number of male patients with a heart problem for each age group of 40 of 60 
*/

A = LOAD 'input.txt' USING PigStorage(',') AS (age:int, sex:int, cp:int, Tres:int, chol:int, fbs:int, Rest:int, thali:int, exang:int, oldpeak:int, slope:int, ca:int, thal:int, disease:int);
DUMP A


Female = FILTER A BY (age >= 40) AND (age <= 60) AND (sex == 0) AND (disease == 1);
DUMP Female

Female_Group = GROUP Female ALL;
Female_Count = FOREACH Female_Group GENERATE COUNT(Female);

Male = FILTER A BY (age >= 40) AND (age <=60) AND (sex == 1) AND (disease == 1);
DUMP Male

Male_Group = GROUP Male ALL;
Male_Count = FOREACH Male_Group Generate COUNT(Male);

STORE Female_Count INTO 'Output';
STORE Male_Count INTO 'Output2'; 
