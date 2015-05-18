package geneticsalesman;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.concurrent.ThreadLocalRandom;

import com.google.common.primitives.Ints;

public class Path implements Serializable {
	private final int[] path;
	private double distance;
	private boolean currentElite;
	
	private Path(int[] path, double distance) {
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
		
		return new Path(p, calculateLength(p, distances));
	}
	
	@Override
	public String toString() {
		return Arrays.toString(path);
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
				
		int[] p=Ints.toArray(set);
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
		int modifiablePathLength = path.length-1; //0 position remains unchanged
		int swapLength = (int)(Math.random()*(int)(modifiablePathLength/2)+1);
		int swap1Position = (int)(Math.random()*modifiablePathLength+1);
		int numPossibleSwap2StartPositions = modifiablePathLength-(2*swapLength-1);
		int swap2Position = swap1Position + swapLength + (int)(Math.random()*numPossibleSwap2StartPositions);
		if(swap2Position >= path.length) 
			swap2Position = swap2Position - modifiablePathLength;
		for(int i = 0; i<swapLength; i++) {
			int temp = path[swap1Position];
			path[swap1Position++] = path[swap2Position];
			path[swap2Position++] = temp;
			if(swap1Position == path.length) 
				swap1Position = swap1Position - modifiablePathLength;
			if(swap2Position == path.length) 
				swap2Position = swap2Position - modifiablePathLength;
		}
		return new Path(path, calculateLength(path, distances));
	}

	public boolean isCurrentElite() {
		return currentElite;
	}
	
	public void setCurrentElite(boolean currentElite) {
		this.currentElite = currentElite;
	}
	
	
}
