package geneticsalesman.evolution;

import geneticsalesman.Path;
import geneticsalesman.statistics.SPoint;
import geneticsalesman.statistics.StatisticsAccumulator;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;

public class Evolver extends RichMapPartitionFunction<Path, Path> {

	private int generations;
	private StatisticsAccumulator statisticsAccumulator;
	private int nextGenerationNumber;
	private static double[][] distances;
	
	public Evolver(int generations, double[][] distances, int nextGenerationNumber) {
		this.nextGenerationNumber=nextGenerationNumber;
		this.generations=generations;
		this.distances=distances;
	}
	
	@Override
	public void open(Configuration parameters) throws Exception {
		statisticsAccumulator=new StatisticsAccumulator();
		getRuntimeContext().addAccumulator(StatisticsAccumulator.NAME, statisticsAccumulator);
	}
	
	@Override
	public void mapPartition(Iterable<Path> t, Collector<Path> out) throws Exception {
		
		List<Path> currentGen=Lists.newArrayList(t);
		
		//do more than one generation at once
		for(int gen=0;gen<generations;gen++) {
			currentGen=rouletteCrossOver(currentGen);
			
			ListIterator<Path> it=currentGen.listIterator();
			while(it.hasNext())
				it.set(it.next().mutate(distances));
		}
		
		Path best=null;
		
		for(Path p:currentGen) {
			if(best==null || p.getLength()<best.getLength())
				best=p;
			out.collect(p);
		}
		if(best!=null)
			statisticsAccumulator.add(new SPoint(System.currentTimeMillis(), best, nextGenerationNumber));
		out.close();
	}
	
	private List<Path> rouletteCrossOver(List<Path> lastGen) {
		//collect paths to rangemap that maps probability to path
		Path elite=null;
		RangeMap<Double, Path> list=TreeRangeMap.create();
		double probabilityCounter=0;
		
		for(Path p:lastGen) {
			if(elite==null || p.getLength()<elite.getLength())
				elite=p;
			
			double prob=1000/p.getLength();
			list.put(Range.closed(probabilityCounter, probabilityCounter+prob), p);
			probabilityCounter+=prob;
		}
		
		//cross new children
		Random r=ThreadLocalRandom.current();
		ArrayList<Path> nextGeneration = new ArrayList<>(lastGen.size());
		for(int i=0;i<lastGen.size()-1;i++) { //one less because of elite
			nextGeneration.add(
					list.get(r.nextDouble()*probabilityCounter).cross(
							list.get(r.nextDouble()*probabilityCounter),
							distances));
		}
		
		//add elite at the end
		if(elite!=null) {
			elite.setMarked(true);
			nextGeneration.add(elite);
		}
		return nextGeneration;
	}
}
