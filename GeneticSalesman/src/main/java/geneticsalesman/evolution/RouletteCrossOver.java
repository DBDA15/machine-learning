package geneticsalesman.evolution;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import geneticsalesman.Path;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;

public class RouletteCrossOver implements FlatMapFunction<Iterator<Path>, Path>, Serializable{

	private static RouletteCrossOver instance;
	
	public static RouletteCrossOver getInstance(Broadcast<double[][]> distances) {
		if(instance==null)
			instance=new RouletteCrossOver(distances);
		return instance;
	}
	
	private final Broadcast<double[][]> distances;
	
	private RouletteCrossOver(Broadcast<double[][]> distances) {
		this.distances=distances;
	}
	
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
		Random r=ThreadLocalRandom.current();
		ArrayList<Path> nextGeneration = new ArrayList<>(counter);
		for(int i=0;i<counter-1;i++) { //one less because of elite
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
	    out.defaultWriteObject();
	}

	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
	    in.defaultReadObject();
	}

}
