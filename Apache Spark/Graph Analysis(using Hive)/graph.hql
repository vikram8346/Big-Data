drop table inp_graph;
drop table out_graph;

create table inp_graph (
  k int,
  v int)
row format delimited fields terminated by ',' stored as textfile;

load data local inpath '${hiveconf:G}' overwrite into table inp_graph;


CREATE TABLE out_graph AS
SELECT k, count(*) as kc
FROM inp_graph
GROUP BY k;

SELECT kc,count(*)
FROM out_graph
GROUP BY kc;


