package geneticsalesman;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class GeneticSalesman {

	public static void main(String[] args) throws FileNotFoundException, IOException {
	    // get job parameters
	    final String citiesFile = args[0];
	    String kmlPath = null;
	    if(args.length == 2) 
	    	kmlPath = args[1];
	    ArrayList<City> citiesList=new ArrayList<>();
	    try(BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(citiesFile),StandardCharsets.UTF_8))) {
	    	String l;
	    	int id = 0;
	    	while((l=in.readLine())!=null) {
	    		String[] parts=StringUtils.split(l, '\t');
	    		if(parts.length == 5 && Integer.parseInt(parts[2])>50000)
					citiesList.add(new City(id++, parts[1], Double.parseDouble(parts[3]), Double.parseDouble(parts[4])));
	    	}
	    }
	    
	    final City[] cities=citiesList.toArray(new City[citiesList.size()]);
	    citiesList=null;
	    System.out.println("COLLECTED CITIES");
	    
	    final double[][] distances=new double[cities.length][cities.length];
	    for(int i=0;i<cities.length;i++) {
	    	for(int j=0;j<cities.length;j++) {
	    		distances[i][j]=cities[i].distanceTo(cities[j]);
	    	}
	    }
	    System.out.println("CALCULATED DISTANCES");
	    // initialize spark environment
	    SparkConf config = new SparkConf().setAppName(GeneticSalesman.class.getName());
	    config.set("spark.hadoop.validateOutputSpecs", "false");
	    try(JavaSparkContext ctx = new JavaSparkContext(config)) {
	    		    	
	    	JavaRDD<Path> generation = ctx.parallelize(Evolution.generateRandomGeneration(cities.length, distances));
	    	Path globalBest = null;
	    	
	    	//MAJOR LOOP THAT IS ALSO PRINTING STUFF
	    	for(int i=0;i<100;i++) {
	    		System.out.println("Generation "+(10*i)+":");
	    		
	    		//MINOR GENERATION LOOP IS ONLY BUILDING A PLAN THAT IS EXECUTED ONCE 
	    		for(int j=0;j<20;j++) {
	    		
			    	//crossover
			    	generation = Evolution.selectionCrossOver(generation, distances, cities.length);
			    	
			    	//mutation
			    	generation = Evolution.mutate(generation, distances);
	    		}
	    		
	    		//TODO here we have to implement some kind of global austausch thingy
	    		//simple solution is this total shuffle
	    		generation=generation.coalesce(generation.partitions().size(), true);
	    		
	    		generation.cache();
	    		
	    		Path best=generation.min(Path.COMPARATOR);
	    		globalBest = best;
	    		System.out.println("\tElite: "+best.getLength()+"\t"+best);
	    		System.out.println("\tGeneration Size: "+generation.count());
	    	}
	    	
	    	System.out.println("Found : "+globalBest.toString());
	    	if(kmlPath != null) {
	    		Helper.KMLExport.exportPath(globalBest, cities, kmlPath);
	    	}
	    }
	}
}