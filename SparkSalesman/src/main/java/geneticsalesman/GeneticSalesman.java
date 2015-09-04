package geneticsalesman;

import geneticsalesman.evolution.Evolution;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Writer;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;

import com.beust.jcommander.JCommander;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

public class GeneticSalesman {

	public static void main(String[] args) throws FileNotFoundException, IOException, URISyntaxException {
		Config config=new Config();
		new JCommander(config, args);

		try (BufferedWriter writer = Helper.Output.writer(config.getOutPath() + "out.txt")) {
			double result = run(config, writer);
			out("average percentage: "+result,writer);
	    }
	}

	public static double run(Config config, BufferedWriter writer) throws IOException, URISyntaxException {
		Evolution.POPULATION_SIZE = config.getPopulationSize();
		Problem problem;
		try(InputParser in=new InputParser()) {
			problem=in.parse(config.getProblem());
		}
		
		// initialize spark environment
		SparkConf sparkConfig = new SparkConf().setAppName(GeneticSalesman.class.getSimpleName()+" on "+(int)(Evolution.POPULATION_SIZE/Math.sqrt(problem.getSize()))+" @ "+config.getProblem());
		sparkConfig.set("spark.hadoop.validateOutputSpecs", "false");
		double[] results = new double[config.getNumberOfRuns()];
		
		try(JavaSparkContext ctx = new JavaSparkContext(sparkConfig)) {
			
			Broadcast<double[][]> distanceBroadcast = ctx.broadcast(problem.getDistances());
				    		
			for(int testRun=0;testRun<config.getNumberOfRuns();testRun++) {

				JavaRDD<Path> generation = ctx.parallelize(Evolution.generateRandomGeneration(problem.getSize(), problem.getDistances()));
				out("Testrun "+testRun, writer);
		    	JavaRDD<Path> lastGeneration=null;
		    	Path globalBest = null;
		    	long time=System.nanoTime();
		    	
		    	//MAJOR LOOP THAT IS ALSO PRINTING STUFF
		    	for(int generationNumber=0; generationNumber<=config.getGenerations(); generationNumber+=config.getQuickGenerations()) {
		    		out("\tGeneration "+(generationNumber)+":", writer);
		    		
		    		
		    		generation=Evolution.evolve(generation, config.getQuickGenerations(), distanceBroadcast);
		    		
		    		if(config.isTournamentShuffle()) {
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
		    	
		    	try(BufferedWriter kmlWriter =  Helper.Output.writer(config.getOutPath() + "out"+testRun+".kml")) { 
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