package geneticsalesman;

import java.util.List;

public class Problem {

	private final City[] cities;
	private final double[][] distances;
	private Path optimal;
	
	public Problem(List<City> citiesList) {
		this(citiesList, null);
	}

	public Problem(List<City> citiesList, int[] optimalPath) {
		cities=citiesList.toArray(new City[citiesList.size()]);
	    citiesList=null;
	    distances=new double[cities.length][cities.length];
	    for(int i=0;i<cities.length;i++) {
	    	for(int j=0;j<cities.length;j++) {
	    		distances[i][j]=cities[i].distanceTo(cities[j]);
	    	}
	    }
	    
	    if(optimalPath!=null) {
	    	optimal=Path.createNormalizedPath(optimalPath, distances);
	    }
	}

	public int getSize() {
		return cities.length;
	}

	public double[][] getDistances() {
		return distances;
	}

	public City[] getCities() {
		return cities;
	}

	public Path getOptimal() {
		return optimal;
	}

}
