package geneticsalesman;

import geneticsalesman.evolution.Evolution;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Writer;
import java.net.URISyntaxException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

public class GeneticSalesman {
	
	private final static int QUICK_GENERATIONS = 100;
	private final static double STOP_WHEN_GOOD_ENOUGH = 0.98;
	private final static boolean TOURNAMENT_SHUFFLE = false;

	public static void main(String[] args) throws FileNotFoundException, IOException, URISyntaxException, InterruptedException {
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
		    	Thread.sleep(2000);
		    	out("Default Parallelism:\t"+ctx.sc().defaultParallelism(), writer);
		    	out("Executor Size:\t"+ctx.sc().getExecutorStorageStatus().length, writer);
		    	out("Executor mem size:\t"+ctx.sc().getExecutorMemoryStatus().size(), writer);
		    	//Evolution.POPULATION_SIZE*=/*ctx.sc().defaultParallelism()*/2d/Math.sqrt(problem.getSize());
		    	out("Population Size:\t"+Evolution.POPULATION_SIZE, writer);
		    	JavaRDD<Path> generation = ctx.parallelize(Evolution.generateRandomGeneration(problem.getSize(), problem.getDistances()));
		    	Broadcast<double[][]> distanceBroadcast = ctx.broadcast(problem.getDistances());
		    	JavaRDD<Path> lastGeneration=null;
		    	Path globalBest = null;
		    	long time=System.nanoTime();
		    	
		    	//MAJOR LOOP THAT IS ALSO PRINTING STUFF
		    	for(int i=0;globalBest==null || problem.getOptimal().getLength()/globalBest.getLength()<STOP_WHEN_GOOD_ENOUGH;i++) {
		    		out("Generation "+(QUICK_GENERATIONS*i)+":", writer);
		    		
		    		
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
		    		
		    		out(generationNumber + ","+ timeDiff + "," + percentage, writer);
		    	}
		    	
		    	out("Took: "+(System.nanoTime()-time)*1000000000 + "s", writer);
		    	
		    	out("Found:\t"+globalBest, writer);
		    	if(problem.getOptimal()!=null) {
		    		out("Opt.:\t"+problem.getOptimal(), writer);
		    		out("Found Length:\t"+(problem.getOptimal().getLength()/globalBest.getLength()), writer);
		    	}
		    	out("Required:\t"+((System.nanoTime()-time)/1000000l)+" ms", writer);
		    	//export result
		    	
		    	try(BufferedWriter kmlWriter =  Helper.Output.writer(outPath + "out.kml")) { 
		    		Helper.KMLExport.exportPath(globalBest, problem, kmlWriter);
		    	}
		    }
	    }
	}
	
	public static void out(String text, Writer writer) throws IOException {
		writer.write(text + "\n");
		writer.flush();
		System.out.println(text);
	}
}