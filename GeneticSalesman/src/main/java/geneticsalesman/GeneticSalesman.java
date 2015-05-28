package geneticsalesman;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

public class GeneticSalesman {
	
	private final static int QUICK_GENERATIONS = 20;
	private final static double STOP_WHEN_GOOD_ENOUGH = 0.98;
	private final static boolean TOURNAMENT_SHUFFLE = false;

	public static void main(String[] args) throws FileNotFoundException, IOException {
	    // get job parameters
	    final String citiesFile = args[0];
	    String kmlPath = null;
	    if(args.length == 2) 
	    	kmlPath = args[1];
	    Problem problem;
	    try(InputParser in=new InputParser()) {
	    	problem=in.parse(citiesFile);
	    }
	    
	   
	    // initialize spark environment
	    SparkConf config = new SparkConf().setAppName(GeneticSalesman.class.getName());
	    config.set("spark.hadoop.validateOutputSpecs", "false");
	    try(JavaSparkContext ctx = new JavaSparkContext(config)) {
	    	System.out.println("Default Parallelism:\t"+ctx.sc().defaultParallelism());
	    	System.out.println("Executor Size:\t"+ctx.sc().getExecutorStorageStatus().length);
	    	Evolution.POPULATION_SIZE*=ctx.sc().defaultParallelism()/Math.sqrt(problem.getSize());
	    	System.out.println("Population Size:\t"+Evolution.POPULATION_SIZE);
	    	JavaRDD<Path> generation = ctx.parallelize(Evolution.generateRandomGeneration(problem.getSize(), problem.getDistances()));
	    	Broadcast<double[][]> distanceBroadcast = ctx.broadcast(problem.getDistances());
	    	JavaRDD<Path> lastGeneration=null;
	    	Path globalBest = null;
	    	int generationsWithoutChangeCounter=0;
	    	long time=System.nanoTime();
	    	
	    	//MAJOR LOOP THAT IS ALSO PRINTING STUFF
	    	for(int i=0;globalBest==null || problem.getOptimal().getLength()/globalBest.getLength()<STOP_WHEN_GOOD_ENOUGH;i++) {
	    		System.out.println("Generation "+(QUICK_GENERATIONS*i)+":");
	    		
	    		//MINOR GENERATION LOOP IS ONLY BUILDING A PLAN THAT IS EXECUTED ONCE 
	    		for(int j=0;j<QUICK_GENERATIONS;j++) {
	    		
			    	//crossover
			    	generation = Evolution.selectionCrossOver(generation, distanceBroadcast, problem.getSize());
			    	
			    	//mutation
			    	generation = Evolution.mutate(generation, distanceBroadcast);
	    		}
	    		
	    		if(TOURNAMENT_SHUFFLE) {
	    			generation=Evolution.rouletteShuffle(generation, ctx);
	    		}
	    		else
	    			generation=generation.coalesce(generation.partitions().size(), true);
	    		
	    		generation=generation.cache();
	    		
	    		Path best=generation.min(Path.COMPARATOR);
	    		
	    		if(lastGeneration!=null)
	    			lastGeneration.unpersist();
	    		lastGeneration=generation;
	    		
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