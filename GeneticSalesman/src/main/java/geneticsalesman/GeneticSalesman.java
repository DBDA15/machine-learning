package geneticsalesman;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class GeneticSalesman {
	
	private final static int QUICK_GENERATIONS = 20;
	private final static double STOP_WHEN_GOOD_ENOUGH = 0.99;
	private final static boolean TOURNAMENT_SHUFFLE = true;

	public static void main(String[] args) throws FileNotFoundException, IOException {
	    // get job parameters
	    final String citiesFile = args[0];
	    String kmlPath = null;
	    if(args.length == 2) 
	    	kmlPath = args[1];
	    Problem problem=InputParser.parse(citiesFile);
	    
	   
	    // initialize spark environment
	    SparkConf config = new SparkConf().setAppName(GeneticSalesman.class.getName());
	    config.set("spark.hadoop.validateOutputSpecs", "false");
	    try(JavaSparkContext ctx = new JavaSparkContext(config)) {
	    	JavaRDD<Path> generation = ctx.parallelize(Evolution.generateRandomGeneration(problem.getSize(), problem.getDistances()));
	    	Path globalBest = null;
	    	int generationsWithoutChangeCounter=0;
	    	long time=System.nanoTime();
	    	
	    	//MAJOR LOOP THAT IS ALSO PRINTING STUFF
	    	for(int i=0;globalBest==null || problem.getOptimal().getLength()/globalBest.getLength()<STOP_WHEN_GOOD_ENOUGH;i++) {
	    		System.out.println("Generation "+(QUICK_GENERATIONS*i)+":");
	    		
	    		//MINOR GENERATION LOOP IS ONLY BUILDING A PLAN THAT IS EXECUTED ONCE 
	    		for(int j=0;j<QUICK_GENERATIONS;j++) {
	    		
			    	//crossover
			    	generation = Evolution.selectionCrossOver(generation, problem.getDistances(), problem.getSize());
			    	
			    	//mutation
			    	generation = Evolution.mutate(generation, problem.getDistances());
	    		}
	    		
	    		if(TOURNAMENT_SHUFFLE) {
	    			generation=Evolution.tournamentShuffle(generation, ctx);
	    		}
	    		else
	    			generation=generation.coalesce(generation.partitions().size(), true);
	    		
	    		generation=generation.cache();
	    		
	    		Path best=generation.min(Path.COMPARATOR);
	    		
	    		if(globalBest==null || best.getLength()<globalBest.getLength())
	    			generationsWithoutChangeCounter=0;
	    		else
	    			generationsWithoutChangeCounter+=QUICK_GENERATIONS;
	    		
	    		globalBest = best;
	    		System.out.println("\tElite:\t"+best+" ("+(int)(problem.getOptimal().getLength()/globalBest.getLength()*100)+")");
	    	}
	    	
	    	System.out.println("Found:\t"+globalBest);
	    	if(problem.getOptimal()!=null) {
	    		System.out.println("Opt.:\t"+problem.getOptimal());
	    		System.out.println("Found Length:\t"+(problem.getOptimal().getLength()/globalBest.getLength()));
	    	}
	    	System.out.println("Required:\t"+((System.nanoTime()-time)/1000000l)+" ms");
	    	//export result
	    	if(kmlPath != null)
	    		Helper.KMLExport.exportPath(globalBest, problem, kmlPath);
	    }
	}
}