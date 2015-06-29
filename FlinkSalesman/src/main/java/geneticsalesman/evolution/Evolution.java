package geneticsalesman.evolution;

import geneticsalesman.Path;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.util.Collector;

import com.google.common.collect.Lists;

public class Evolution {
	
	public static int POPULATION_SIZE=1000;

	public static List<Path> generateRandomGeneration(int numberOfCities, double[][] distances) {
		ArrayList<Path> generation=new ArrayList<>(POPULATION_SIZE);
    	for(int i=0;i<POPULATION_SIZE;i++)
    		generation.add(Path.createRandomPath(numberOfCities, distances));
    	return generation;
	}

	public static DataSet<Path> rouletteShuffle(DataSet<Path> generation) {
		/*generation=generation.mapPartition(new MapPartitionFunction<Path, Path>() {
			@Override
			public void mapPartition(Iterable<Path> values, Collector<Path> out) {
				List<Path> l=Lists.newArrayList(values);
				Random r=ThreadLocalRandom.current();
				int partitionId=r.nextInt(Integer.MAX_VALUE);
				
				for(Path p:l)
					p.setMark(partitionId);
				for(int i=0;i<5;i++)
					l.get(r.nextInt(l.size())).setMark(r.nextInt(Integer.MAX_VALUE));
				for(Path p:l)
					out.collect(p);
			}
		}).partitionByHash(new KeySelector<Path, Integer>() {
			@Override
			public Integer getKey(Path value) throws Exception {
				return value.getMark();
			}
			
		});*/
		generation=generation.mapPartition(new MapPartitionFunction<Path, Path>() {
			@Override
			public void mapPartition(Iterable<Path> values, Collector<Path> out) {
				List<Path> l=Lists.newArrayList(values);
				Random r=ThreadLocalRandom.current();
				
				for(Path p:l)
					p.setMark(0);
				for(int i=0;i<5;i++)
					l.get(r.nextInt(l.size())).setMark(1);
				for(Path p:l)
					out.collect(p);
			}
		});
		FilterOperator<Path> marked = generation.filter(new FilterFunction<Path>() {
			@Override
			public boolean filter(Path value) throws Exception {
				return value.getMark()==1;
			}
		});
		generation=generation.filter(new FilterFunction<Path>() {
			@Override
			public boolean filter(Path value) throws Exception {
				return value.getMark()==0;
			}
		}).union(marked);
		return generation;
	}

	public static DataSet<Path> evolve(DataSet<Path> generation, int generations, double[][] distances) {
		return generation.mapPartition(new Evolver(generations, distances));
	} 
	
}
