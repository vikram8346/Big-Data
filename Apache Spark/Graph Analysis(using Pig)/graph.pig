G = LOAD '$G' USING PigStorage(',') AS ( k:long, v:long ); /*Todo int/long*/

/*2 group by's will be used 
1st for finding the neighbours, 2nd for grouping by number of neighbours*/

/*Todo 1st group by*/

M1 = group G by k;
R1 = foreach M1 generate group, COUNT($1); /*TODO not working, changes required*/

/* 2nd group by*/

M2 = group R1 by $1;
R2 = foreach M2 generate group, COUNT($1); /*TODO changes required, SUM not working, Count(R1) also works*/
STORE R2 INTO '$O' USING PigStorage (',');
/*dump ;*/
