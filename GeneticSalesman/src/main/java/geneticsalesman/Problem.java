package geneticsalesman;

import java.util.List;

public class Problem {

	private final City[] cities;
	private final double[][] distances;
	
	public Problem(List<City> citiesList) {
		cities=citiesList.toArray(new City[citiesList.size()]);
	    citiesList=null;
	    distances=new double[cities.length][cities.length];
	    for(int i=0;i<cities.length;i++) {
	    	for(int j=0;j<cities.length;j++) {
	    		distances[i][j]=cities[i].distanceTo(cities[j]);
	    	}
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

}
