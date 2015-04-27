package entrance;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import com.google.common.base.Joiner;

import scala.Tuple2;

public class Topic4Entrance {
	
	private static final String[][] QUERIES = {
		{"Robin", "Williams"},
		{"World", "Cup"},
		{"Ebola"},
		{"Malaysia", "Airlines"},
		{"ALS", "Ice", "Bucket", "Challenge"},
		{"Flappy", "Bird"},
		{"Conchita", "Wurst"},
		{"ISIS"},
		{"Frozen"},
		{"Sochi", "Olympics"}
	};

	public static void main(String[] args) {
	    // get job parameters
	    final String wikipediaTitleFile = args[0];
	    
	    // initialize spark environment
	    SparkConf config = new SparkConf().setAppName(Topic4Entrance.class.getName());
	    config.set("spark.hadoop.validateOutputSpecs", "false");
	    try(JavaSparkContext ctx = new JavaSparkContext(config)) {
	    	//load lines from file
	    	JavaRDD<String> titles = ctx.textFile(wikipediaTitleFile, 1);
	    	
	    	long numberOfTitles=titles.count();
	    	System.out.println("COUNTED TITLES");
	    	
	    	JavaRDD<String> words = titles.flatMap(new FlatMapFunction<String, String>() {
				@Override
				public Iterable<String> call(String v) throws Exception {
					return Arrays.asList(StringUtils.split(v, '_'));
				}
			});
	    	System.out.println("LISTED WORDS");
	    	
	    	JavaPairRDD<String, Integer> wordPairs = words.mapToPair(new PairFunction<String, String, Integer>() {
	    		@Override
	    		public Tuple2<String, Integer> call(String s) {
	    			return new Tuple2<String, Integer>(s, 1);
	    		}
	    	});
	    	System.out.println("CREATED PAIRS");

	    	JavaPairRDD<String, Integer> countedWords = wordPairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
	    		@Override
	    		public Integer call(Integer i1, Integer i2) {
	    			return i1 + i2;
	    		}
	    	});
	    	System.out.println("COUNTED WORD OCCURENCES");
	    	
	    	long numberOfWords=countedWords.count();
	    	
	    	for(String[] q:QUERIES) {
	    		double p=1d;
	    		for(String qw:q) {
	    			List<Integer> counted=countedWords.lookup(qw);
	    			int count=(counted.isEmpty())?0:counted.get(0);
	    			p*=(double)(count+1)/(numberOfWords+numberOfTitles);
	    		}
	    		
	    		System.out.println(Joiner.on(' ').join(q)+"\t"+p);
	    	}
	    }
	}
}