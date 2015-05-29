package geneticsalesman;

import geneticsalesman.evolution.Evolution;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import com.google.common.primitives.Ints;

public class GeneticSalesman {
	
	private final static int QUICK_GENERATIONS = 100;
	private final static double STOP_WHEN_GOOD_ENOUGH = 0.80;
	private final static boolean TOURNAMENT_SHUFFLE = false;
	private static final int NUMBER_OF_RUNS = 10;

	public static void main(String[] args) throws FileNotFoundException, IOException, URISyntaxException {
		String outPath;
		if(args.length == 2)
			outPath = args[1]+"/";
		else
			outPath = "";
		try (BufferedWriter writer = Helper.Output.writer(outPath + "out.txt")) {
		
		    // get job parameters
		    final String citiesFile = args[0];
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
		    	
		    	int[] requiredMS=new int[NUMBER_OF_RUNS];
		    	
		    	for(int testRun=0;testRun<NUMBER_OF_RUNS;testRun++) {
		    		out
			    	JavaRDD<Path> lastGeneration=null;
			    	Path globalBest = null;
			    	long time=System.nanoTime();
			    	
			    	//MAJOR LOOP THAT IS ALSO PRINTING STUFF
			    	for(int i=0;globalBest==null || problem.getOptimal().getLength()/globalBest.getLength()<STOP_WHEN_GOOD_ENOUGH;i++) {
			    		System.out.println("\tGeneration "+(QUICK_GENERATIONS*i)+":");
			    		
			    		
			    		generation=Evolution.evolve(generation, QUICK_GENERATIONS, distanceBroadcast);
			    		
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
			    		
			    		globalBest = best;
			    		int generationNumber = QUICK_GENERATIONS*i;
			    		long timeDiff = (System.nanoTime()-time)/1000000000L;
			    		double percentage = problem.getOptimal().getLength()/globalBest.getLength()*100;
			    		
			    		writer.write("\t"+generationNumber + ","+ timeDiff + "," + percentage + "\n");
			    		System.out.println("\t\t"+percentage);
			    	}
			    	
			    	System.out.println("\tFound:\t"+globalBest);
			    	if(problem.getOptimal()!=null) {
			    		System.out.println("\tOpt.:\t"+problem.getOptimal());
			    		System.out.println("\tFound Length:\t"+(problem.getOptimal().getLength()/globalBest.getLength()));
			    	}
			    	requiredMS[testRun]=Ints.checkedCast((System.nanoTime()-time)/1000000l);
			    	System.out.println("\tRequired:\t"+requiredMS[testRun]+" ms");
			    	//export result
			    	
			    	try(BufferedWriter kmlWriter =  Helper.Output.writer(outPath + "out"+testRun+".kml")) { 
			    		Helper.KMLExport.exportPath(globalBest, problem, kmlWriter);
			    	}
			    }
		    }
	    }
	}
}