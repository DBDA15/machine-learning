package geneticsalesman;

import io.netty.util.internal.ThreadLocalRandom;

import java.io.Serializable;

import org.apache.flink.api.java.functions.KeySelector;

public class RandomSelector<T> implements KeySelector<T, Integer>, Serializable {

	private static RandomSelector<?> INSTANCE = null;

	public static <T> RandomSelector<T> getInstance() {
		if(INSTANCE==null)
			INSTANCE=new RandomSelector<T>();
		return (RandomSelector<T>) INSTANCE;
	}
	
	@Override
	public Integer getKey(T value) throws Exception {
		return ThreadLocalRandom.current().nextInt(20);
	}

}
