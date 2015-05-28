package geneticsalesman.evolution;

import geneticsalesman.Path;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;

public class Evolution {
	
	public static int POPULATION_SIZE=10000;

	public static List<Path> generateRandomGeneration(int numberOfCities, double[][] distances) {
		ArrayList<Path> generation=new ArrayList<>(POPULATION_SIZE);
    	for(int i=0;i<POPULATION_SIZE;i++)
    		generation.add(Path.createRandomPath(numberOfCities, distances));
    	return generation;
	}

	public static JavaRDD<Path> selectionCrossOver(JavaRDD<Path> generation, final Broadcast<double[][]> distances, int numberOfCities) {
		//select and cross over in each partition
		return generation.mapPartitions(RouletteCrossOver.getInstance(distances), true); //true -> preserve partitions
	}
	
	@SuppressWarnings("serial")
	public static JavaRDD<Path> mutate(JavaRDD<Path> generation, final Broadcast<double[][]> distances) {
		//select and cross over in each partition
		generation = generation.map(Mutate.getInstance(distances));
		
		return generation;
	}

	public static JavaRDD<Path> rouletteShuffle(JavaRDD<Path> generation, JavaSparkContext ctx) {
		generation=generation.mapPartitions(new FlatMapFunction<Iterator<Path>, Path>() {
			@Override
			public Iterable<Path> call(Iterator<Path> it) throws Exception {
				//collect paths to rangemap that maps probability to path
				List<Path> l=Lists.newArrayList(it);
				Path elite=null;
				RangeMap<Double, Path> list=TreeRangeMap.create();
				double probabilityCounter=0;
				
				for(Path p:l) {
					if(elite==null || p.getLength()<elite.getLength())
						elite=p;
					
					double prob=1000/p.getLength();
					list.put(Range.closed(probabilityCounter, probabilityCounter+prob), p);
					probabilityCounter+=prob;
				}
				
				//select some
				Random r=new Random();
				for(int i=0;i<5;i++)
					list.get(r.nextDouble()*probabilityCounter).setMarked(true);
				
				return l;
			}
		}).cache();
		
		JavaRDD<Path> cached = generation;
		
		int numberOfPartitions=generation.partitions().size();
		
		List<Path> selected = generation.filter(new Function<Path, Boolean>() {
			@Override
			public Boolean call(Path v1) throws Exception {
				return v1.isMarked();
			}
		}).collect();
		
		Collections.shuffle(selected);
		for(Path p:selected)
			p.setMarked(false);
		generation=generation.filter(new Function<Path, Boolean>() {
			@Override
			public Boolean call(Path v1) throws Exception {
				return !v1.isMarked();
			}
		}).union(ctx.parallelize(selected)).coalesce(numberOfPartitions, false);
		cached.unpersist();
		return generation;
	} 
	
}
