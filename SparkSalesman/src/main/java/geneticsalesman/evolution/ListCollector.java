package geneticsalesman.evolution;

import geneticsalesman.Path;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.AccumulableParam;

public class ListCollector implements AccumulableParam<List<Path>, Path> {

	private static final ListCollector instance=new ListCollector();
	
	public static ListCollector getInstance() {
		return instance;
	}
	
	@Override
	public List<Path> addAccumulator(List<Path> l, Path e) {
		l.add(e);
		return l;
	}

	@Override
	public List<Path> addInPlace(List<Path> l1, List<Path> l2) {
		l1.addAll(l2);
		return l1;
	}

	@Override
	public List<Path> zero(List<Path> arg0) {
		return new ArrayList<Path>();
	}


}
