package geneticsalesman;

import geneticsalesman.evolution.Evolution;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Writer;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

public class GeneticSalesman {
	
	private final static boolean TOURNAMENT_SHUFFLE = false;
	private static final int NUMBER_OF_RUNS = 1;
	private static final int QUICK_GENERATIONS = 100;
	
	public static class GenerationPopulationPair{
		public int generations;
		public int population;
		public GenerationPopulationPair(int generations, int population) {
			this.generations = generations;
			this.population = population;
		}
	}

	public static void main(String[] args) throws FileNotFoundException, IOException, URISyntaxException {
		GenerationPopulationPair[] pairs = new GenerationPopulationPair[] {
				new GenerationPopulationPair(20000, 5)
				//new GenerationPopulationPair(1600000, 5)
		};
		String outPath;
		if(args.length == 2)
			outPath = args[1]+"/";
		else
			outPath = "";
		try (BufferedWriter writer = Helper.Output.writer(outPath + "out.txt")) {
			double[] resultPercentages = new double[pairs.length];
			for(int i=0;i<pairs.length;i++) {
				out("---STARTING WITH PAIR", writer);
				resultPercentages[i]=run(args[0], outPath, pairs[i], writer);
				out("---FINISHED!", writer);
			}
			out("generations,population,avgPercentage",writer);
			for(int i=0;i<pairs.length;i++) {
				out(pairs[i].generations+","+pairs[i].population+","+resultPercentages[i],writer);
			}
	    }
	}

	public static double run(String citiesFile, String outPath, GenerationPopulationPair pair, BufferedWriter writer) throws IOException, URISyntaxException {
		Evolution.POPULATION_SIZE = pair.population;
		// get job parameters
		Problem problem;
		try(InputParser in=new InputParser()) {
			problem=in.parse(citiesFile);
		}
		
   
		// initialize spark environment
		SparkConf config = new SparkConf().setAppName(GeneticSalesman.class.getSimpleName()+" on "+(int)(Evolution.POPULATION_SIZE/Math.sqrt(problem.getSize()))+" @ "+citiesFile);
		config.set("spark.hadoop.validateOutputSpecs", "false");
		double[] results = new double[NUMBER_OF_RUNS];
		
		try(JavaSparkContext ctx = new JavaSparkContext(config)) {
			
			Broadcast<double[][]> distanceBroadcast = ctx.broadcast(problem.getDistances());
				    		
			for(int testRun=0;testRun<NUMBER_OF_RUNS;testRun++) {

				JavaRDD<Path> generation = ctx.parallelize(Evolution.generateRandomGeneration(problem.getSize(), problem.getDistances()));
				out("Testrun "+testRun, writer);
		    	JavaRDD<Path> lastGeneration=null;
		    	Path globalBest = null;
		    	long time=System.nanoTime();
		    	
		    	//MAJOR LOOP THAT IS ALSO PRINTING STUFF
		    	for(int generationNumber=0; generationNumber<=pair.generations; generationNumber+=QUICK_GENERATIONS) {
		    		out("\tGeneration "+(generationNumber)+":", writer);
		    		
		    		
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
		    		long timeDiff = (System.nanoTime()-time)/1000000000L;
		    		double percentage = problem.getOptimal().getLength()/globalBest.getLength()*100;
		    		
		    		writer.write("\t"+generationNumber + ","+ timeDiff + "," + percentage + "\n");
		    		out("\t\t"+percentage, writer);
		    	}
		    	results[testRun] = problem.getOptimal().getLength()/globalBest.getLength();
		    	out("\tFound:\t"+globalBest, writer);
		    	if(problem.getOptimal()!=null) {
		    		out("\tOpt.:\t"+problem.getOptimal(), writer);
		    		out("\tFound Length:\t"+(problem.getOptimal().getLength()/globalBest.getLength()), writer);
		    	}
		    	//out("\tRequired:\t"+progressOfTestRuns[testRun]+" ms", writer);
		    	//export result
		    	
		    	try(BufferedWriter kmlWriter =  Helper.Output.writer(outPath + "out"+testRun+".kml")) { 
		    		Helper.KMLExport.exportPath(globalBest, problem, kmlWriter);
				}
		    }
			return median(results);
		}
	}
	
	public static double median(double[] progressesForGen) {
		Arrays.sort(progressesForGen);
		if (progressesForGen.length % 2 == 0)
		    return (progressesForGen[progressesForGen.length/2] + progressesForGen[progressesForGen.length/2 - 1])/2;
		else
		    return progressesForGen[progressesForGen.length/2];
	}
	
	public static int median(int[] progressesForGen) {
		Arrays.sort(progressesForGen);
		if (progressesForGen.length % 2 == 0)
		    return (progressesForGen[progressesForGen.length/2] + progressesForGen[progressesForGen.length/2 - 1])/2;
		else
		    return progressesForGen[progressesForGen.length/2];
	}
	
	
	public static void out(String text, Writer writer) throws IOException {
		writer.write(text + "\n");
		writer.flush();
		System.out.println(text);
	}
}