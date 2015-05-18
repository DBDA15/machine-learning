package geneticsalesman;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;

public class Evolution {
	
	private static final int POPULATION_SIZE=1000;

	public static List<Path> generateRandomGeneration(int numberOfCities, double[][] distances) {
		ArrayList<Path> generation=new ArrayList<>(POPULATION_SIZE);
    	for(int i=0;i<POPULATION_SIZE;i++)
    		generation.add(Path.createRandomPath(numberOfCities, distances));
    	return generation;
	}

	public static JavaRDD<Path> selectionCrossOver(JavaRDD<Path> generation, final double[][] distances, int numberOfCities) {
		//select and cross over in each partition
		generation = generation.mapPartitions(new FlatMapFunction<Iterator<Path>, Path>() {
			@SuppressWarnings("null")
			@Override
			public Iterable<Path> call(Iterator<Path> t) throws Exception {
				//collect paths to rangemap that maps probability to path
				Path elite=null;
				RangeMap<Double, Path> list=TreeRangeMap.create();
				double probabilityCounter=0;
				int counter=0;
				
				while(t.hasNext()) {
					Path p=t.next();
					
					if(elite==null || p.getLength()<elite.getLength())
						elite=p;
					
					double prob=1000/p.getLength();
					list.put(Range.closed(probabilityCounter, probabilityCounter+prob), p);
					probabilityCounter+=prob;
					
					counter++;
				}
				
				//cross new children
				Random r=new Random();
				ArrayList<Path> nextGeneration = new ArrayList<>(counter);
				for(int i=0;i<counter-1;i++) { //one less because of elite
					nextGeneration.add(
							list.get(r.nextDouble()*probabilityCounter).cross(
									list.get(r.nextDouble()*probabilityCounter),
									distances));
				}
				
				//add elite at the end
				elite.setCurrentElite(true);
				nextGeneration.add(elite);
							
				return nextGeneration;
			}
		}, true); //true -> preserve partitions
		
		return generation;
	}
	
	@SuppressWarnings("serial")
	public static JavaRDD<Path> mutate(JavaRDD<Path> generation, final double[][] distances) {
		//select and cross over in each partition
		generation = generation.map((new Function<Path, Path>() {
			@Override
			public Path call(Path p) throws Exception {
				if(!p.isCurrentElite()) 
					p.mutate(distances);
				return p;
			}
		}));
		
		return generation;
	} 
	
	/**public static JavaRDD<Path> mutate(JavaRDD<Path> generation, final double[][] distances, int numberOfCities) {
		//select and cross over in each partition
		generation = generation.mapPartitions(new FlatMapFunction<Iterator<Path>, Path>() {
			@SuppressWarnings("null")
			@Override
			public Iterable<Path> call(Iterator<Path> t) throws Exception {
				//collect paths to rangemap that maps probability to path
				Path elite=null;
				List<Path> paths;
				int counter = 0;
				
				while(t.hasNext()) {
					Path p=t.next();
					
					if(elite==null || p.getLength()<elite.getLength())
						elite=p;

					paths.add(p);
				}
				
				for(p : paths) {
					if(!elite == p) 
						p.mutate
					nextGeneration.add(p);
				}
				
							
				return nextGeneration;
			}
		}, true); //true -> preserve partitions
		
		return generation;
	} */

	
}
