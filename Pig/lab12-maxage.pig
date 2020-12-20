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

Write a PIG code to find <id,name> of the oldest (max age). 
*/

A = LOAD 'input.txt' USING PigStorage(',') AS (id:int, name:chararray, age:int, weight:long);
DUMP A

B = GROUP A ALL;

C = FOREACH B GENERATE MAX(A.age) as max_age;

D = FILTER A BY age == (int)C.max_age;

E = FOREACH D GENERATE id, name;

STORE E INTO 'Output';
