Machine Learning
===================

This repository contains three projects:
* Entrance Task
* FlinkSalesman
* SparkSalesman

# Entrance Task #
Unigram Language Models: Calculates the probabilities that different search queries (i.e., 10) were drawn (stem) from Wikipedia articles

# Genetic Salesman #
FlinkSalesman and SparkSalesman offer implementations of a distributed genetic algorithm for the traveling salesman problem. The input problem file might be a tsv file from the real-problems directory or from the [TSPLib](http://comopt.ifi.uni-heidelberg.de/software/TSPLIB95/). The latter files have a .tsp extension and are expected to be accompanied by a .opt.tour file in the same directory containing the optimal route. This a benchmark for the outcome of the genetic algorithm.

## FlinkSalesman ##
An implementation of our genetic salesman algorithm in Apache Flink.

### Build ###
Building the project requires Java 7 and Maven.
In the FlinkSalesman directory, execute:
`mvn compile assembly:single`
The resulting jar can be found under target/flink-0.0.1-SNAPSHOT.jar

### Execute ###
```
Usage: java -cp flink-0.0.1-SNAPSHOT.jar geneticsalesman.GeneticSalesman [options] <input problem file>
  --parallelism:    
      Parallelism used for Spark. Default: 1
  --numberOfRuns:
      How often the algorithm is run. Default: 1
  --quickGenerations:    
      Specifies after how many iterations exchange between the cluster nodes should take place. Default: 50
  --generations: 
      Number of generations to run. Default: 2000
  --populationSize:
	  Total number of individuals. Default: 1000
  --exchange: 
      Specifies the exchange method. Possible Values: SELECTED_RANDOM, NONE, COMPLETE_RANDOM, ROUND_ROBIN. Default: ROUND_ROBIN
  --outFile: 
      the output file to write to. Required.
  --host
	  the host for the Flink execution. Default: Use ExecutionEnvironment.getExecutionEnvironment();
```

### Results ###
After each exchange step, the current progress is stored in a tab separated file. For instance, with quickGenerations=1000, this happens for generation 1000, 2000, 3000, ...

The format is:

```<generation>	<milliseconds so far>	<relative fitness compared to the given optimum from tsplib>	<best path found so far> => <length of the best path>```

## SparkSalesman ##

### Build ###
Building the project requires Java 7 and Maven.
In the FlinkSalesman directory, execute:
`mvn package`
The resulting jar can be found under target/spark-0.0.1-SNAPSHOT.jar

### Execute ###
Submit the spark-0.0.1-SNAPSHOT.jar according to the [Spark submit documentation](http://spark.apache.org/docs/latest/submitting-applications.html). As application arguments use:
 
```
Usage: ./bin/spark-submit ... spark-0.0.1-SNAPSHOT.jar [options] <input problem file>
  --numberOfRuns:
      How often the algorithm is run. Default: 1
  --quickGenerations:    
      Specifies after how many iterations exchange between the cluster nodes should take place. Default: 50
  --generations: 
      Number of generations to run. Default: 2000
  --populationSize:
	  Total number of individuals. Default: 1000
  --tournamentShuffle: 
      Specifies whether the tournament shuffle instead of coalesce() is used. Default: false.
  --outPath: 
      the folder to write statistics and illustrating kml files to be viewed in Google Maps. Required.
```

### Results ###
At the end of <outPath>/out.txt, after each exchange the current generation, the relative fitness, and the milliseconds it took are stored, averaged over all test runs:

```<generation>	<milliseconds so far>	<relative fitness compared to the given optimum from tsplib>```