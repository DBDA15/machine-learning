package geneticsalesman.evolution;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import geneticsalesman.Path;
import geneticsalesman.VariableByteEncoding;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;

public class Evolver implements FlatMapFunction<Iterator<Path>, Path>, Serializable{

	private static Evolver instance;
	
	public static Evolver getInstance(int generations, Broadcast<double[][]> distances) {
		if(instance==null)
			instance=new Evolver(generations, distances);
		return instance;
	}
	
	private Broadcast<double[][]> distances;
	private int generations;
	
	private Evolver(int generations, Broadcast<double[][]> distances) {
		this.distances=distances;
		this.generations=generations;
	}
	
	@Override
	public Iterable<Path> call(Iterator<Path> t) throws Exception {
		
		List<Path> currentGen=Lists.newArrayList(t);
		
		//do more than one generation at once
		for(int gen=0;gen<generations;gen++) {
			currentGen=rouletteCrossOver(currentGen);
			
			ListIterator<Path> it=currentGen.listIterator();
			while(it.hasNext())
				it.set(it.next().mutate(distances.getValue()));
		}				
		return currentGen;
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
		Random r=new Random();
		ArrayList<Path> nextGeneration = new ArrayList<>(lastGen.size());
		for(int i=0;i<lastGen.size()-1;i++) { //one less because of elite
			nextGeneration.add(
					list.get(r.nextDouble()*probabilityCounter).cross(
							list.get(r.nextDouble()*probabilityCounter),
							distances.getValue()));
		}
		
		//add elite at the end
		elite.setMarked(true);
		nextGeneration.add(elite);
		return nextGeneration;
	}

	private void writeObject(ObjectOutputStream out) throws IOException {
	    VariableByteEncoding.writeVNumber(out, generations);
	    out.writeObject(distances);
	}

	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
	    generations=VariableByteEncoding.readVInt(in);
	    distances=(Broadcast<double[][]>) in.readObject();
	}

}
