package geneticsalesman;

import org.apache.flink.api.common.functions.ReduceFunction;

public class MinReduce implements ReduceFunction<Path>{

	private static MinReduce INSTANCE;
	
	public static ReduceFunction<Path> getInstance() {
		if(INSTANCE==null)
			INSTANCE=new MinReduce();
		return INSTANCE;
	}

	@Override
	public Path reduce(Path a, Path b) throws Exception {
		if(a.getLength()<b.getLength())
			return a;
		else
			return b;
	}

}
