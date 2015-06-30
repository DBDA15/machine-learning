package geneticsalesman.evolution;

import geneticsalesman.Config;
import geneticsalesman.Exchange;
import geneticsalesman.Path;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.DataSet;

public class Evolution {
	
	public static List<Path> generateRandomGeneration(Config config, int numberOfCities, double[][] distances) {
		ArrayList<Path> generation=new ArrayList<>(config.getPopulationSize());
    	for(int i=0;i<config.getPopulationSize();i++)
    		generation.add(Path.createRandomPath(numberOfCities, distances));
    	return generation;
	}

	public static DataSet<Path> evolve(DataSet<Path> generation, int generations, double[][] distances) {
		return generation.mapPartition(new Evolver(generations, distances));
	}

	public static DataSet<Path> exchange(Config config, DataSet<Path> generation) {
		return Exchange.valueOf(config.getExchange()).exchange(generation);
	} 
	
}
