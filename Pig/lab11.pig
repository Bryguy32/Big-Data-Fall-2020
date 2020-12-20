/*
Bryan Francis
Lab 11
Write a Pig code to find number of occurrence of each letter in the following data set 
*/


A = LOAD 'input.txt' AS (line:chararray);
B = FOREACH A GENERATE FLATTEN(TOKENIZE(line)) as words;
C = FOREACH B GENERATE FLATTEN(TOKENIZE(REPLACE(words, '','|'), '|')) as letters;
D = GROUP C by letters;
E = FOREACH D GENERATE group, COUNT(C);
DUMP E; 
