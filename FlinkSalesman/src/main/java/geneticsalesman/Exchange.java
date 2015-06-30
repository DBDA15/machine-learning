package geneticsalesman;

import io.netty.util.internal.ThreadLocalRandom;

import java.io.Serializable;
import java.util.List;
import java.util.Random;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.util.Collector;

import com.google.common.collect.Lists;

public enum Exchange {
	
	COMPLETE_RANDOM {
		@Override
		public DataSet<Path> exchange(DataSet<Path> generation) {
			return generation.partitionByHash(new RandomSelector());
		}
	},
	ROUND_ROBIN {
		@Override
		public DataSet<Path> exchange(DataSet<Path> generation) {
			return generation.partitionByHash(new RoundRobinSelector());
		}
	},
	SELECTED_RANDOM {
		@Override
		public DataSet<Path> exchange(DataSet<Path> generation) {
			generation=generation.mapPartition(new MapPartitionFunction<Path, Path>() {
				@Override
				public void mapPartition(Iterable<Path> values, Collector<Path> out) {
					List<Path> l=Lists.newArrayList(values);
					Random r=ThreadLocalRandom.current();
					
					for(Path p:l)
						p.setMark(0);
					for(int i=0;i<5;i++)
						l.get(r.nextInt(l.size())).setMark(1);
					for(Path p:l)
						out.collect(p);
				}
			});
			DataSet<Path> marked = generation.filter(new FilterFunction<Path>() {
				@Override
				public boolean filter(Path value) throws Exception {
					return value.getMark()==1;
				}
			});
			generation=generation.filter(new FilterFunction<Path>() {
				@Override
				public boolean filter(Path value) throws Exception {
					return value.getMark()==0;
				}
			}).union(marked).mapPartition(new MapPartitionFunction<Path, Path>() {

				@Override
				public void mapPartition(Iterable<Path> values, Collector<Path> out)
						throws Exception {
					List<Path> l=Lists.newArrayList(values);
					for(Path p:l)
						out.collect(p);
				}
			});
			return generation;
		}
	};
	
	public static class RandomSelector implements KeySelector<Path, Integer>, Serializable {
		private static RandomSelector INSTANCE = null;
		public static RandomSelector getInstance() {
			if(INSTANCE==null)
				INSTANCE=new RandomSelector();
			return INSTANCE;
		}
		
		@Override
		public Integer getKey(Path value) throws Exception {
			return ThreadLocalRandom.current().nextInt(20);
		}
	}
	public static class RoundRobinSelector implements KeySelector<Path, Integer>, Serializable {
		private static RoundRobinSelector INSTANCE = null;
		public static RoundRobinSelector getInstance() {
			if(INSTANCE==null)
				INSTANCE=new RoundRobinSelector();
			return INSTANCE;
		}
		private int id=0;
		@Override
		public Integer getKey(Path value) throws Exception {
			return id++;
		}
	}

	public abstract DataSet<Path> exchange(DataSet<Path> generation);
}