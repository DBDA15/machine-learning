package geneticsalesman.statistics;

import org.apache.flink.api.common.accumulators.Accumulator;

public class StatisticsAccumulator implements Accumulator<SPoint, Statistics> {

	public static final String NAME = "statistics";
	private Statistics current = new Statistics();

	@Override
	public void add(SPoint value) {
		current.add(value);
	}

	@Override
	public Statistics getLocalValue() {
		return current;
	}

	@Override
	public void resetLocal() {
		current=new Statistics();
	}

	@Override
	public StatisticsAccumulator clone() {
		StatisticsAccumulator c=new StatisticsAccumulator();
		c.current=current.clone();
		return c;
	}

	@Override
	public void merge(Accumulator<SPoint, Statistics> other) {
		this.current=Statistics.merge(current, other.getLocalValue());
	}
	
}