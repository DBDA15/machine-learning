package geneticsalesman;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class InputParser implements Closeable {

	private FileSystem fs;

	public Problem parse(String citiesFile) throws IOException {
		if(citiesFile.endsWith(".tsv"))
			return parseDefault(citiesFile);
		else if(citiesFile.endsWith(".tsp"))
			return parseTSP(citiesFile);
		else
			throw new IllegalArgumentException(citiesFile);
	}

	private Problem parseTSP(String citiesFile) throws IOException {
		ArrayList<City> citiesList=new ArrayList<>();
		HashMap<String, Integer> nameIdMap=new HashMap<>();
	    try(BufferedReader in = new BufferedReader(new InputStreamReader(createInputStream(citiesFile),StandardCharsets.UTF_8))) {
	    	String l;
	    	int id = 0;
	    	while((l=in.readLine())!=null) {
	    		if(l.startsWith(" ")) {
	    			String[] parts=StringUtils.split(l, ' ');
	    			if(parts.length==3) {
		    			double x=Double.parseDouble(parts[1]);
		    			double y=Double.parseDouble(parts[2]);
		    			double latitude=((int)x + 5.0 * (x-(int)x)/ 3.0);
		    			double longitude=((int)y + 5.0 * (y-(int)y)/ 3.0);
		    			if(parts[0].matches("\\d+"))
		    				parts[0]=Integer.toString(Integer.parseInt(parts[0]));
		    			City c=new City(id++, parts[0], longitude, latitude);
		    			nameIdMap.put(c.getName(), c.getId());
		    			citiesList.add(c);
	    			}
	    		}
	    	}
	    }
	    
	    //parse optimal solution
	    int[] path=new int[citiesList.size()];
	    try(BufferedReader in = new BufferedReader(new InputStreamReader(createInputStream(citiesFile.replace(".tsp", ".opt.tour")),StandardCharsets.UTF_8))) {
	    	String l;
	    	int counter=0;
	    	while((l=in.readLine())!=null) {
	    		if(l.matches("\\d+")) {
	    			path[counter++]=nameIdMap.get(l);
	    		}
	    	}
	    }
	    return new Problem(citiesList, path);
	}

	private Problem parseDefault(String citiesFile) throws IOException {
		ArrayList<City> citiesList=new ArrayList<>();
	    try(BufferedReader in = new BufferedReader(new InputStreamReader(createInputStream(citiesFile),StandardCharsets.UTF_8))) {
	    	String l;
	    	int id = 0;
	    	while((l=in.readLine())!=null) {
	    		String[] parts=StringUtils.split(l, '\t');
	    		if(parts.length == 5 && Integer.parseInt(parts[2])>50000)
					citiesList.add(new City(id++, parts[1], Double.parseDouble(parts[3]), Double.parseDouble(parts[4])));
	    	}
	    }
	    return new Problem(citiesList);
	}

	private InputStream createInputStream(String file) throws IOException {
		if(file.startsWith("hdfs://")) {
			if(fs==null)
				fs = FileSystem.get(new Configuration());
			return fs.open(new Path(file));
		}
		else
			return new FileInputStream(file);
	}

	@Override
	public void close() throws IOException {
		if(fs!=null)
			fs.close();
	}

}
