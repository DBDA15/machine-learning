package geneticsalesman.evolution;

import geneticsalesman.Path;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.spark.Accumulable;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeRangeMap;

public class Evolution {
	
	public static int POPULATION_SIZE=10000;

	public static List<Path> generateRandomGeneration(int numberOfCities, double[][] distances) {
		ArrayList<Path> generation=new ArrayList<>(POPULATION_SIZE);
    	for(int i=0;i<POPULATION_SIZE;i++)
    		generation.add(Path.createRandomPath(numberOfCities, distances));
    	return generation;
	}

	public static JavaRDD<Path> rouletteShuffle(JavaRDD<Path> generation, JavaSparkContext ctx) {
		final Accumulable<List<Path>, Path> best=ctx.accumulable(new ArrayList<Path>(), "Roulette Shuffle", ListCollector.getInstance());
		
		
		generation=generation.mapPartitions(new FlatMapFunction<Iterator<Path>, Path>() {
			@Override
			public Iterable<Path> call(Iterator<Path> it) throws Exception {
				//collect paths to rangemap that maps probability to path
				Set<Path> l=Sets.newHashSet(it);
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
				for(int i=0;i<5;i++) {
					Path e=list.get(r.nextDouble()*probabilityCounter);
					best.add(e);
					l.remove(e);
				}
				
				return l;
			}
		}).cache();
		
		generation.count();
		
		JavaRDD<Path> cached = generation;
		
		int numberOfPartitions=generation.partitions().size();
		
		List<Path> selected=best.value();
		
		Collections.shuffle(selected);
		
		generation=generation.union(ctx.parallelize(selected)).coalesce(numberOfPartitions, false);
		cached.unpersist();
		return generation;
	}

	public static JavaRDD<Path> evolve(JavaRDD<Path> generation, int generations, final Broadcast<double[][]> distances) {
		return generation.mapPartitions(Evolver.getInstance(generations, distances), true);
	} 
	
}
