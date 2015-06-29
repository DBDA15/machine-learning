package geneticsalesman;

import geneticsalesman.evolution.Evolution;
import geneticsalesman.statistics.Statistics;
import geneticsalesman.statistics.StatisticsAccumulator;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Arrays;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.slf4j.LoggerFactory;

public class GeneticSalesman {
	
	private final static boolean TOURNAMENT_SHUFFLE = true;
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

	public static void main(String[] args) throws Exception {
		GenerationPopulationPair[] pairs = new GenerationPopulationPair[] {
				new GenerationPopulationPair(20000, 1000)
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

	public static double run(String citiesFile, String outPath, GenerationPopulationPair pair, BufferedWriter writer) throws Exception {
		Evolution.POPULATION_SIZE = pair.population;
		// get job parameters
		final Problem problem;
		try(InputParser in=new InputParser()) {
			problem=in.parse(citiesFile);
		}
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.addDefaultKryoSerializer(Path.class, Path.Serializer.class);
		env.addDefaultKryoSerializer(Statistics.class, Statistics.Serializer.class);
		env.getConfig().disableSysoutLogging();
		//or ExecutionEnvironment.createRemoteEnvironment(String host, int port, String... jarFiles)
		double[] results = new double[NUMBER_OF_RUNS];
		
		for(int testRun=0;testRun<NUMBER_OF_RUNS;testRun++) {
			DataSet<Path> generation = env
				.fromCollection(Evolution.generateRandomGeneration(problem.getSize(), problem.getDistances()))
				.name("Generation 0");
			long baseTime=System.currentTimeMillis();
			out("Testrun "+testRun, writer);
			
			IterativeDataSet<Path> iterationStart=generation.iterate(pair.generations/QUICK_GENERATIONS);
			generation=iterationStart;
	    	
    		generation=Evolution.evolve(generation, QUICK_GENERATIONS, problem.getDistances());
    		
    		if(TOURNAMENT_SHUFFLE)
    			generation=Evolution.rouletteShuffle(generation);
    		else
    			generation=generation.partitionByHash(RandomSelector.<Path>getInstance());
	    		
	    	generation=iterationStart.closeWith(generation);
	    	/*generation.filter(new FilterFunction<Path>() {
				@Override
				public boolean filter(Path value) throws Exception {
					return false;
				}
			}).print();*/
	    	generation.reduce(MinReduce.getInstance()).printOnTaskManager("BEST:\t");
	    	//results[testRun] = problem.getOptimal().getLength()/globalBest.getLength();
	    	JobExecutionResult res=env.execute("Genetic Salesman "+Evolution.POPULATION_SIZE+" quick generations");
	    	
	    	Statistics result=(Statistics)res.getAccumulatorResult(StatisticsAccumulator.NAME);
	    	out("\n"+result.toString(problem.getOptimal().getLength(), baseTime), writer);
	    	
	    	long[] timeNeeded=new long[result.getPoints().size()-1];
	    	for(int i=0;i<timeNeeded.length;i++)
	    		timeNeeded[i]=result.getPoints().get(i+1).getTime()-result.getPoints().get(i).getTime();
	    	out("Median Time/Generation:\t"+(float)median(timeNeeded)/QUICK_GENERATIONS, writer);
	    	/*
	    	out("\tFound:\t"+globalBest, writer);
	    	if(problem.getOptimal()!=null) {
	    		out("\tOpt.:\t"+problem.getOptimal(), writer);
	    		out("\tFound Length:\t"+(problem.getOptimal().getLength()/globalBest.getLength()), writer);
	    	}
	    	out("\tRequired:\t"+progressOfTestRuns[testRun]+" ms", writer);
	    	//export result
	    	
	    	try(BufferedWriter kmlWriter =  Helper.Output.writer(outPath + "out"+testRun+".kml")) { 
	    		Helper.KMLExport.exportPath(globalBest, problem, kmlWriter);
			}*/
	    }
		return median(results);
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
	
	public static long median(long[] progressesForGen) {
		Arrays.sort(progressesForGen);
		if (progressesForGen.length % 2 == 0)
		    return (progressesForGen[progressesForGen.length/2] + progressesForGen[progressesForGen.length/2 - 1])/2;
		else
		    return progressesForGen[progressesForGen.length/2];
	}
	
	
	public static void out(String text, Writer writer) throws IOException {
		writer.write(text + "\n");
		writer.flush();
		LoggerFactory.getLogger(GeneticSalesman.class).warn(text);
	}
}