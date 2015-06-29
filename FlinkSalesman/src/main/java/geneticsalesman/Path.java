package geneticsalesman;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.primitives.Ints;

public class Path implements Comparable<Path>, Serializable {
	private int[] path;
	private double distance;
	private int mark;
	
	Path(int[] path, double distance) {
		this.path=path;
		this.distance=distance;
	}

	private static double calculateLength(int[] path, double[][] distances) {
		double distance=distances[path[path.length-1]][path[0]];
		for(int i=0;i<path.length-1;i++)
			distance+=distances[path[i]][path[i+1]];
		return distance;
	}
	
	public double getLength() {
		return distance;
	}
	
	public static Path createRandomPath(int length, double[][] distances) {
		ArrayList<Integer> l=new ArrayList<>(length);
		for(int i=1;i<length;i++)
			l.add(i);
		Collections.shuffle(l);
		
		int[] p=new int[length];
		for(int i=0;i<length-1;i++)
			p[i+1]=l.get(i);
		
		p=normalize(p);
		
		return new Path(p, calculateLength(p, distances));
	}
	
	@Override
	public String toString() {
		if(path.length>15)
			return Arrays.toString(Arrays.copyOf(path, 15))+"... => "+distance;
		else
			return Arrays.toString(path)+" => "+distance;
	}
	
	public String toString(City[] cities) {
		StringBuilder sb=new StringBuilder();
		for(int i:path)
			sb.append(cities[i].getName()).append(" -> ");
		sb.delete(sb.length()-4, sb.length());
		return sb.toString();
	}
	
	public static final Comparator<Path> COMPARATOR = new PathComparator();

	public Path cross(Path p2, double[][] distances) {
		int cuttingPoint=ThreadLocalRandom.current().nextInt(path.length);
		LinkedHashSet<Integer> set=new LinkedHashSet<>(path.length);
		for(int i=0;i<cuttingPoint;i++)
			set.add(path[i]);
		for(int v:p2.path)
			if(set.add(v)); //add all missing elements in the order of p2
				
		int[] p=normalize(Ints.toArray(set));
		return new Path(p, calculateLength(p, distances));
	}
	
	private static class PathComparator implements Comparator<Path>, Serializable {
		@Override
		public int compare(Path o1, Path o2) {
			return Double.compare(o1.getLength(),o2.getLength());
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(path);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Path other = (Path) obj;
		if (!Arrays.equals(path, other.path))
			return false;
		return true;
	}

	public int[] getIDs() {
		return path;
	}

	public Path mutate(double[][] distances) {
		Random r=ThreadLocalRandom.current();
		int[] newPath = Arrays.copyOf(path, path.length);
		double newDistance = distance;
		
		while(r.nextDouble()<0.6) {
			double modificationDecider = r.nextDouble();
			if(modificationDecider<0.33) {
				swapSequenceMutation(r, newPath);	
			}
			else if(modificationDecider<0.66){
				swapSinglePairMutation(r, newPath);
			}
			else {
				viceVersaMutation(r, newPath);
			}
			newPath = normalize(newPath);
		}
		newDistance = calculateLength(newPath, distances);
		return new Path(newPath, newDistance);
	}

	private void viceVersaMutation(Random r, int[] newPath) {
		int pos1 = r.nextInt(newPath.length);
		int length = r.nextInt(newPath.length);
		int pos2 = (pos1 + length) % newPath.length;
		while(Math.abs(pos1-pos2)>1){
			int temp = newPath[pos1];
			newPath[pos1] = newPath[pos2];
			newPath[pos2] = temp;
			pos1++;pos2--;
			if(pos1 >= newPath.length)
				pos1-=newPath.length;
			if(pos2 < 0) 
				pos2+=newPath.length;
		}
		if(pos2-pos1==1) {
			int temp = newPath[pos1];
			newPath[pos1] = newPath[pos2];
			newPath[pos2] = temp;
		}
	}

	private void swapSinglePairMutation(Random r, int[] newPath) {
		int pos1 = r.nextInt(newPath.length);
		int pos2 = r.nextInt(newPath.length-1);
		if(pos2 >= pos1)
			pos2++;
		int temp = newPath[pos1];
		newPath[pos1] = newPath[pos2];
		newPath[pos2] = temp;
	}

	private void swapSequenceMutation(Random r, int[] newPath) {
		int swapLength = r.nextInt(path.length/2)+1;
		int swap1Position = r.nextInt(path.length);
		int numPossibleSwap2StartPositions = path.length-(2*swapLength-1);
		int swap2Position = (swap1Position + swapLength + r.nextInt(numPossibleSwap2StartPositions)) % path.length;

		for(int i = 0; i<swapLength; i++) {
			int temp = newPath[(swap1Position+i)%path.length];
			newPath[(swap1Position+i)%path.length] = newPath[(swap2Position+i)%path.length];
			newPath[(swap2Position+i)%path.length] = temp;
		}
	}

	private static int[] normalize(int[] path) {
		int[] newPath=path;
		//rotate
		if(path[0] != 0) { 
			newPath = new int[path.length];
			int zeroPos = 0;
			while(path[zeroPos] != 0)
				zeroPos++;
			System.arraycopy(path, zeroPos, newPath, 0, path.length-zeroPos);
			System.arraycopy(path, 0 , newPath, path.length-zeroPos, zeroPos);
		}
		
		//reverse if required so that p[1]<p[length-1]
		if(newPath[1]>newPath[path.length-1]) {
			path=newPath;
			newPath=new int[path.length];
			for(int i=1;i<path.length;i++)
				newPath[i]=path[path.length-i];
		}
		return newPath;
	}

	public int getMark() {
		return mark;
	}
	
	public void setMark(int mark) {
		this.mark = mark;
	}

	public static Path createNormalizedPath(int[] p, double[][] distances) {
		int[] r = normalize(p);
		return new Path(r, calculateLength(r, distances));
	}
	
	@Override
	public int compareTo(Path o) {
		return Double.compare(getLength(), o.getLength());
	}
	
	public static class Serializer extends com.esotericsoftware.kryo.Serializer<Path>{
		@Override
		public void write(Kryo kryo, Output output, Path p) {
			VariableByteEncoding.writeVNumber(output,  p.mark);
			output.writeDouble(p.distance);
			VariableByteEncoding.writeVInts(output, p.path);
		}

		@Override
		public Path read(Kryo kryo, Input input, Class<Path> type) {
			int mark=VariableByteEncoding.readVInt(input);
			double dist=input.readDouble();
			Path p=new Path(VariableByteEncoding.readVInts(input), dist);
			p.mark=mark;
			return p;
		}
	}
}
