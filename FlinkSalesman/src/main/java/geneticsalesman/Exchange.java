package geneticsalesman;

import java.io.Serializable;
import java.util.List;
import java.util.Random;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.util.Collector;

import com.google.common.collect.Lists;

import io.netty.util.internal.ThreadLocalRandom;

public enum Exchange {
	
	NONE {
		@Override
		public DataSet<Path> exchange(DataSet<Path> generation) {
			return generation;
		}
	},
	COMPLETE_RANDOM {
		@Override
		public DataSet<Path> exchange(DataSet<Path> generation) {
			return generation.partitionByHash(new RandomSelector()).rebalance();
		}
	},
	ROUND_ROBIN {
		@Override
		public DataSet<Path> exchange(DataSet<Path> generation) {
			return generation.partitionByHash(new RoundRobinSelector()).rebalance();
		}
	},
	SELECTED_RANDOM {
		@Override
		public DataSet<Path> exchange(DataSet<Path> generation) {
			generation=generation.mapPartition(new MapPartitionFunction<Path, Path>() {
				@Override
				public void mapPartition(Iterable<Path> values, Collector<Path> out) {
					List<Path> l=Lists.newArrayList(values);
					Random r=new Random();
					
					for(Path p:l)
						p.setMarked(false);
					for(int i=0;i<5;i++)
						l.get(r.nextInt(l.size())).setMarked(true);
					for(Path p:l)
						out.collect(p);
				}
			});
			DataSet<Path> marked = generation.filter(new FilterFunction<Path>() {
				@Override
				public boolean filter(Path value) throws Exception {
					return value.isMarked();
				}
			}).partitionByHash(new RoundRobinSelector());
			generation=generation.filter(new FilterFunction<Path>() {
				@Override
				public boolean filter(Path value) throws Exception {
					return !value.isMarked();
				}
			}).union(marked).rebalance();
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
			return new Random().nextInt(20);
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
			int ret=id++;
			if(id==20)
				id=0;
			return ret;
		}
	}

	public abstract DataSet<Path> exchange(DataSet<Path> generation);
}