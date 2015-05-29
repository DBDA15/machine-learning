package geneticsalesman;

import geneticsalesman.evolution.Evolution;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Writer;
import java.net.URISyntaxException;
import java.util.Arrays;

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
		    SparkConf config = new SparkConf().setAppName(GeneticSalesman.class.getSimpleName()+" on "+(int)(Evolution.POPULATION_SIZE/Math.sqrt(problem.getSize()))+" @ "+citiesFile);
		    config.set("spark.hadoop.validateOutputSpecs", "false");
		    try(JavaSparkContext ctx = new JavaSparkContext(config)) {
		    	Evolution.POPULATION_SIZE/=Math.sqrt(problem.getSize());
		    	out("Population Size:\t"+Evolution.POPULATION_SIZE, writer);
				JavaRDD<Path> generation = ctx.parallelize(Evolution.generateRandomGeneration(problem.getSize(), problem.getDistances()));
		    	Broadcast<double[][]> distanceBroadcast = ctx.broadcast(problem.getDistances());
		    	
		    	int[] requiredMS=new int[NUMBER_OF_RUNS];
		    	
		    	for(int testRun=0;testRun<NUMBER_OF_RUNS;testRun++) {
		    		out("Testrun "+testRun, writer);
			    	JavaRDD<Path> lastGeneration=null;
			    	Path globalBest = null;
			    	long time=System.nanoTime();
			    	
			    	//MAJOR LOOP THAT IS ALSO PRINTING STUFF
			    	for(int i=0;globalBest==null || problem.getOptimal().getLength()/globalBest.getLength()<STOP_WHEN_GOOD_ENOUGH;i++) {
			    		out("\tGeneration "+(QUICK_GENERATIONS*i)+":", writer);
			    		
			    		
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
			    		out("\t\t"+percentage, writer);
			    	}
			    	
			    	out("\tFound:\t"+globalBest, writer);
			    	if(problem.getOptimal()!=null) {
			    		out("\tOpt.:\t"+problem.getOptimal(), writer);
			    		out("\tFound Length:\t"+(problem.getOptimal().getLength()/globalBest.getLength()), writer);
			    	}
			    	requiredMS[testRun]=Ints.checkedCast((System.nanoTime()-time)/1000000l);
			    	out("\tRequired:\t"+requiredMS[testRun]+" ms", writer);
			    	//export result
			    	
			    	try(BufferedWriter kmlWriter =  Helper.Output.writer(outPath + "out"+testRun+".kml")) { 
			    		Helper.KMLExport.exportPath(globalBest, problem, kmlWriter);
		    		}
			    }
		    	
		    	out("Median Time:\t"+median(requiredMS)+"ms", writer);
		    }
	    }
	}
	
	public static int median(int[] values) {
		Arrays.sort(values);
		if (values.length % 2 == 0)
		    return (values[values.length/2] + values[values.length/2 - 1])/2;
		else
		    return values[values.length/2];
	}
	
	public static void out(String text, Writer writer) throws IOException {
		writer.write(text + "\n");
		writer.flush();
		System.out.println(text);
	}
}