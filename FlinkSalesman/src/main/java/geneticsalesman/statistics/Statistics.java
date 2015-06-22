package geneticsalesman.statistics;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class Statistics implements Serializable {
	
	private List<SPoint> points=new ArrayList<>();
	
	public static Statistics merge(Statistics a, Statistics b) {
		Statistics n=a.clone();
		for(SPoint p:b.points)
			n.add(p);
		return n;
	}
	
	@Override
	protected Statistics clone() {
		Statistics s=new Statistics();
		for(SPoint p:points)
			s.add(p.clone());
		return s;
	}

	public void add(SPoint value) {
		ListIterator<SPoint> it=points.listIterator();
		while(it.hasNext()) {
			SPoint n=it.next();
			if(n.getGeneration()==value.getGeneration()) {
				it.set(SPoint.merge(value, n));
				return;
			}
			if(n.getGeneration()>value.getGeneration()) {
				it.previous();
				it.add(value);
				return;
			}
		}
		points.add(value);
	}
	
	public String toString(double bestLength, long baseTime) {
		StringBuilder sb =new StringBuilder();
		sb.append("gen\tms\tperc\tbest\n");
		for(SPoint p:points)
			sb.append(p.toString(bestLength, baseTime)).append('\n');
		return sb.toString();
	}
	
	public static class Serializer extends com.esotericsoftware.kryo.Serializer<Statistics> {

		@Override
		public void write(Kryo kryo, Output output, Statistics s) {
			output.writeInt(s.points.size());
			for(SPoint p:s.points) {
				kryo.writeObject(output, p);
			}
		}

		@Override
		public Statistics read(Kryo kryo, Input input, Class<Statistics> type) {
			Statistics s=new Statistics();		
			int size=input.readInt();
			List<SPoint> l=new ArrayList<>(size);
			for(int i=0;i<size;i++)
				l.add(kryo.readObject(input, SPoint.class));
			s.points=l;
			return s;
		}
		
	}
}
