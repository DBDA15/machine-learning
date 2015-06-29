package geneticsalesman.statistics;

import geneticsalesman.Path;

import java.io.Serializable;

public class SPoint implements Serializable {
	
	private final long time;
	private final Path best;
	private final int generation;
	
	public SPoint(long time, Path best, int generation) {
		this.time = time;
		this.best = best;
		this.generation = generation;
	}
	public long getTime() {
		return time;
	}
	public Path getBest() {
		return best;
	}
	public int getGeneration() {
		return generation;
	}
	
	@Override
	protected SPoint clone() {
		return new SPoint(time, best, generation);
	}
	public static SPoint merge(SPoint a, SPoint b) {
		return new SPoint(Math.max(a.time, b.time), a.best.getLength()<b.best.getLength()?a.best:b.best, a.generation);
	}
	
	public String toString(double bestLength, long baseTime) {
		return generation+"\t"+(time-baseTime)+"\t"+(int)(100*bestLength/best.getLength())+"\t"+best.toString();
	}
}
