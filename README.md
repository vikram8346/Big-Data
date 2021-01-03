# Big-Data

Big Data: Cloud Computing projects on SDSC Comet Cluster

**Setup and Installation:**

*	On Ubuntu Linux, use command **apt install maven** or on Windows 10, install Windows Subsystem for Linux and then install Ubuntu>=20.0
*	Open terminal, use the command **sudo apt install openjdk-8-jdk maven**
*	To install Hadoop, use the commands: 

    **wget https://archive.apache.org/dist/hadoop/common/hadoop-2.6.5/hadoop-2.6.5.tar.gz**

    **tar xfz hadoop-2.6.5.tar.gz**
*	To install Apache Spark, use the commands:

    **wget https://archive.apache.org/dist/spark/spark-1.5.2/spark-1.5.2-bin-hadoop2.6.tgz**

    **tar xfz spark-1.5.2-bin-hadoop2.6.tgz**
*	On the project directory, to set JAVA_HOME to point to the Java installation, type the command **export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64**
*	Build the project using the command **mvn install**


**Running on SDSC Comet Cluster:**

*	Copy the project directory to the Comet
*	Build the project using **run *file-name*.build**
*	Running the project on Standalone mode:
  **sbatch *file-name*.local.run**
*	Running the project on Distributed mode:
  **sbatch *file-name*.distr.run**

**Map-Reduce:**

MapReduce is a programming model and an associated implementation for processing and generating large data sets. Users specify a map function that processes a key/value pair to generate a set of intermediate key/value pairs, and a reduce function that merges all intermediate values associated with the same intermediate key.

1.	**Graph Processing-** This project involved a graph processing algorithm that needs two map-reduce jobs. A directed graph is represented as a text file where each line represents a graph edge. For example, 20,40 represents the directed edge from node 20 to node 40. First, for each graph node, you compute the number of node neighbours. Then, you group the nodes by their number of neighbours and for each group you count how many nodes belong to this group. That is, the result will have lines such as: 10 30 which says that there are 30 nodes that have 10 neighbours.

    To run the project, use the commands:

    **rm -rf output**

    **~/hadoop-2.6.5/bin/hadoop jar target/*.jar Graph small-graph.txt output**

    The file output/part-r-00000 will contain the results.

2.	**Matrix Multiplication-** This project implements multiplication of two sparse matrices using map-reduce. There are two small sparse matrices 4*3 and 3*3 in the files M-matrix-small.txt and N-matrix-small.txt for testing in standalone mode and moderate-sized matrices 200*100 and 100*300 in the files M-matrix-large.txt and M-matrix-large.txt for testing in distributed mode.

    To run the project, use the commands:

    **rm –rf intermediate output**

    **~/hadoop-2.6.5/bin/hadoop jar target/*.jar Multiply M-matrix-small.txt N-matrix-small.txt intermediate output**

    The file output/part-r-00000 will contain the results.

3.	**Graph Partition-** This project implements a graph analysis using map-reduce. A directed graph is represented in the input text file using one line per graph vertex. For example, the line 1,2,3,4,5,6,7 represents the vertex with ID 1, which is linked to the vertices with IDs 2, 3, 4, 5, 6, and 7. The program partitions a graph into K clusters using multi-source BFS (breadth-first search). It selects K random graph vertices, called centroids, and then, at the first iteration, for each centroid, it assigns the centroid id to its unassigned neighbors. Then, at the second iteration. it assigns the centroid id to the unassigned neighbors of the neighbors, etc, in a breadth-first search fashion. After few repetitions, each vertex will be assigned to the centroid that needs the smallest number of hops to reach the vertex (the closest centroid).

    To run the project, use the commands:
    
    **rm –rf intermediate output**
    
    **~/hadoop-2.6.5/bin/hadoop jar target/*.jar GraphPartition large-graph.txt intermediate output**
    
    The file output/part-r-00000 will contain the results.


**Apache Spark:**

Apache Spark is an open-source distributed general-purpose cluster-computing framework. Spark provides an interface for programming entire clusters with implicit data parallelism and fault tolerance. Apache Spark has its architectural foundation in the resilient distributed dataset (RDD), a read-only multiset of data items distributed over a cluster of machines, that is maintained in a fault-tolerant way. Spark's RDDs function as a working set for distributed programs that offers a (deliberately) restricted form of distributed shared memory.

1.	**K-means Clustering-** This project involves data analysis using Apache Spark. It implements one step of the Lloyd's algorithm for K-Means clustering using Spark and Scala. The goal is to partition a set of points into k clusters of neighboring points. It starts with an initial set of k centroids. Then, it repeatedly partitions the input according to which of these centroids is closest and then finds a new centroid for each partition. That is, if you have a set of points P and a set of k centroids C, the algorithm repeatedly applies the following steps:

    a.	Assignment step: partition the set P into k clusters of points Pi, one for each centroid Ci, such that a point p belongs to Pi if it is closest to the centroid Ci among all centroids.
    
    b.	Update step: Calculate the new centroid Ci from the cluster Pi so that the x,y coordinates of Ci is the mean x,y of all points in Pi.
    
    The datasets used are random points on a plane in the squares (i*2+1,j*2+1)-(i*2+2,j*2+2), with 0≤i≤9 and 0≤j≤9 (so k=100 in k-means). The initial centroids in centroid.txt are the points (i*2+1.2,j*2+1.2). So the new centroids should be in the middle of the squares at (i*2+1.5,j*2+1.5).

    To run the project, use the commands:

    **rm –rf output**

    **~/spark-1.5.2-bin-hadoop2.6/bin/spark-submit --class KMeans target/*.jar points-small.txt centroids.txt**

2.	**Graph Partition(using Spark and Scala)-** This project re-implements the graph partition project(initially implemented with map-reduce) using Apache Spark. The graph can be represented as RDD[ ( Long, Long, List[Long] ) ], where the first Long is the graph node ID, the second Long is the assigned cluster ID (-1 if the node has not been assigned yet), and the List[Long] is the adjacent list (the IDs of the neighbours).

    To run the project, use the command:

    **~/spark-1.5.2-bin-hadoop2.6/bin/spark-submit --class Partition --master local[2] partition.jar small-graph.txt**

3.	**Graph Analysis(using Apache Pig)-** This project re-implements the Graph Processing project(initially done with map-reduce) using Apache Pig.

    To install and Pig, use the commands:

    **wget http://mirrors.gigenet.com/apache/pig/pig-0.16.0/pig-0.16.0.tar.gz**

    **tar xfz pig-0.16.0.tar.gz**

    To run the project, use the commands:

    **rm -rf output**

    **~/pig-0.16.0/bin/pig -x local -param G=small-graph.txt -param O=output graph.pig**

4.	**Graph Analysis(using Apache Hive)-** This project re-implements the Graph Processing project(initially done with map-reduce) using Apache Hive. The program calculates the number of incoming links for each graph vertex and sorts the nodes by the number of their incoming links in descending order, so that the first node is the one that has the most incoming links.

    To install hive, use the commands:

    **wget https://downloads.apache.org/hive/stable-2/apache-hive-2.3.7-bin.tar.gz**

    **tar xfz apache-hive-2.3.7-bin.tar.gz**

    **export HIVE_HOME=$HOME/apache-hive-2.3.7-bin**

    **export HADOOP_HOME=$HOME/hadoop-2.6.5**

    **export PATH=$HIVE_HOME/bin:$PATH**

    **export HIVE_OPTS="--hiveconf mapreduce.framework.name=local --hiveconf fs.default.name=file://$HOME –hiveconf hive.metastore.warehouse.dir=file://$HOME/warehouse --hiveconf javax.jdo.option.ConnectionURL=jdbc:derby:;databaseName=/$HOME/metastore_db;create=true"**

    To create an empty metastore database, use the commands:

    **rm -rf metastore_db  warehouse**

    **schematool -dbType derby –initSchema**

    To run the project, use the command:

    **hive -f graph.hql --hiveconf G=small-graph.txt**

5.	**Graph Processing(using pregel on graphX)-** This project re-implements the Graph Processing project(initially done with map-reduce) using Spark GraphX by partitioning the graph into 10 clusters.

    To run the project, use the command:

    **~/spark-1.5.2-bin-hadoop2.6/bin/spark-submit --class Partition --master local[2] target/*.jar small-graph.txt**

