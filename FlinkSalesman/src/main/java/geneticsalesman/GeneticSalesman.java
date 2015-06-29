package geneticsalesman;

import geneticsalesman.evolution.Evolution;
import geneticsalesman.statistics.Statistics;
import geneticsalesman.statistics.StatisticsAccumulator;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Writer;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.ReduceOperator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;

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
		//or ExecutionEnvironment.createRemoteEnvironment(String host, int port, String... jarFiles)
		double[] results = new double[NUMBER_OF_RUNS];
		
		for(int testRun=0;testRun<NUMBER_OF_RUNS;testRun++) {
			DataSet<Path> generation = env
				.fromCollection(Evolution.generateRandomGeneration(problem.getSize(), problem.getDistances()))
				.name("Generation 0");
			long baseTime=System.currentTimeMillis();
			out("Testrun "+testRun, writer);
	    	
	    	//MAJOR LOOP THAT IS ALSO PRINTING STUFF
	    	for(int generationNumber=0; generationNumber<=pair.generations; generationNumber+=QUICK_GENERATIONS) {
	    		
	    		
	    		generation=Evolution.evolve(generation, QUICK_GENERATIONS, generationNumber+QUICK_GENERATIONS, problem.getDistances());
	    		
	    		/*if(TOURNAMENT_SHUFFLE) {
	    			generation=Evolution.rouletteShuffle(generation, ctx);
	    		}
	    		else*/
	    		
	    		generation=generation.partitionByHash(RandomSelector.<Path>getInstance()).name("Generation "+(generationNumber+QUICK_GENERATIONS));
	    		
	    		/*ReduceOperator<Path> best = generation.reduce(MinReduce.getInstance());
	    		best.print();*/
	    	}
	    	generation.filter(new FilterFunction<Path>() {
				@Override
				public boolean filter(Path value) throws Exception {
					return false;
				}
			}).print();
	    	//results[testRun] = problem.getOptimal().getLength()/globalBest.getLength();
	    	JobExecutionResult res=env.execute("Genetic Salesman "+Evolution.POPULATION_SIZE+" quick generations");
	    	
	    	Statistics result=(Statistics)res.getAccumulatorResult(StatisticsAccumulator.NAME);
	    	out(result.toString(problem.getOptimal().getLength(), baseTime), writer);
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
	
	
	public static void out(String text, Writer writer) throws IOException {
		writer.write(text + "\n");
		writer.flush();
		System.out.println(text);
	}
}