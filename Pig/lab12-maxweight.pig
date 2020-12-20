/*
Bryan Francis 
Lab 12
Consider the following input file:
<id, name, age, weight>

0,Bert,30,90
1,Curt,28,81
2,Derp,43,79
3,Emil,30,85
4, Kurt, 28, 59

Write a PIG code to find <id, name, age> maximum weight. 

*/

A = LOAD 'input.txt' USING PigStorage(',') AS (id:int, name:chararray, age:int, weight:long);
DUMP A

B = GROUP A ALL;

C = FOREACH B GENERATE MAX(A.weight) as max_weight;

D = FILTER A BY weight == (long)C.max_weight;

E = FOREACH D GENERATE id, name, age;

STORE E INTO 'Output';

