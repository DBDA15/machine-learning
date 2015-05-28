package geneticsalesman.evolution;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import geneticsalesman.Path;
import geneticsalesman.VariableByteEncoding;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

public class Mutate implements Function<Path, Path>, Serializable {
	private static Mutate instance;
	
	public static Mutate getInstance(Broadcast<double[][]> distances) {
		if(instance==null)
			instance=new Mutate(distances);
		return instance;
	}
	
	private final Broadcast<double[][]> distances;
	
	private Mutate(Broadcast<double[][]> distances) {
		this.distances=distances;
	}
	
	@Override
	public Path call(Path p) throws Exception {
		return p.mutate(distances.getValue());
	}
	
	private void writeObject(ObjectOutputStream out) throws IOException {
	    out.defaultWriteObject();
	}

	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
	    in.defaultReadObject();
	}
}
