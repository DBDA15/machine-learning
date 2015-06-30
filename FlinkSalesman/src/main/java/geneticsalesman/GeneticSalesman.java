package geneticsalesman;

import geneticsalesman.evolution.Evolution;
import geneticsalesman.statistics.Statistics;
import geneticsalesman.statistics.StatisticsAccumulator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.util.Arrays;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;

public class GeneticSalesman {
	
	public static class GenerationPopulationPair{
		public int generations;
		public int population;
		public GenerationPopulationPair(int generations, int population) {
			this.generations = generations;
			this.population = population;
		}
	}

	public static void main(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		Config config=new Config();
		new JCommander(config, args);
		try (BufferedWriter writer = Helper.Output.writer(config.getOutFile())) {
			//double[] resultPercentages = new double[pairs.length];
			/*for(int i=0;i<pairs.length;i++) {
				out("---STARTING WITH PAIR", writer);
				resultPercentages[i]=run(config, writer);
				out("---FINISHED!", writer);
			}
			out("generations,population,avgPercentage",writer);
			for(int i=0;i<pairs.length;i++) {
				out(pairs[i].generations+","+pairs[i].population+","+resultPercentages[i],writer);
			}*/
			run(config, writer);
	    }
	}

	public static double run(Config config, BufferedWriter writer) throws Exception {
		// get job parameters
		final Problem problem;
		try(InputParser in=new InputParser()) {
			problem=in.parse(config.getProblem().get(0));
		}
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.addDefaultKryoSerializer(Path.class, Path.Serializer.class);
		env.addDefaultKryoSerializer(Statistics.class, Statistics.Serializer.class);
		env.getConfig().disableSysoutLogging();
		//or ExecutionEnvironment.createRemoteEnvironment(String host, int port, String... jarFiles)
		double[] results = new double[config.getNumberOfRuns()];
		
		for(int testRun=0;testRun<config.getNumberOfRuns();testRun++) {
			DataSet<Path> generation = env
				.fromCollection(Evolution.generateRandomGeneration(config, problem.getSize(), problem.getDistances()))
				.name("Generation 0");
			long baseTime=System.currentTimeMillis();
			out("Testrun "+testRun, writer);
			
			IterativeDataSet<Path> iterationStart=generation.iterate(config.getGenerations()/config.getQuickGenerations());
			generation=iterationStart;
	    	
    		generation=Evolution.evolve(generation, config.getQuickGenerations(), problem.getDistances());
    		
    		generation=Evolution.exchange(config, generation);
	    		
	    	generation=iterationStart.closeWith(generation);

	    	generation.reduce(MinReduce.getInstance()).printOnTaskManager("BEST:\t");
	    	//results[testRun] = problem.getOptimal().getLength()/globalBest.getLength();
	    	JobExecutionResult res=env.execute("Genetic Salesman "+config.getQuickGenerations()+" quick generations");
	    	
	    	Statistics result=(Statistics)res.getAccumulatorResult(StatisticsAccumulator.NAME);
	    	out("\n"+result.toString(problem.getOptimal().getLength(), baseTime), writer);
	    	
	    	long[] timeNeeded=new long[result.getPoints().size()-1];
	    	for(int i=0;i<timeNeeded.length;i++)
	    		timeNeeded[i]=result.getPoints().get(i+1).getTime()-result.getPoints().get(i).getTime();
	    	out("Median Time/Generation:\t"+(float)median(timeNeeded)/config.getQuickGenerations(), writer);
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