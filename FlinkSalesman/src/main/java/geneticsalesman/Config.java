package geneticsalesman;

import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

@Parameters(separators="=")
public class Config {
	@Parameter(names="--numberOfRuns")
	private int numberOfRuns=1;
	
	@Parameter(names="--quickGenerations")
	private int quickGenerations=50;
	
	@Parameter(names="--generations")
	private int generations=2000;
	
	@Parameter(names="--populationSize")
	private int populationSize=1000;
	
	@Parameter(names="--exchange")
	private String exchange=Exchange.SELECTED_RANDOM.name();
	
	@Parameter(required=true)
	private List<String> problem;
	
	@Parameter(names="--outFile", required=true)
	private String outFile;
	
	//host:port
	@Parameter(names="--host")
	private String host;
	
	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public List<String> getJars() {
		return jars;
	}

	public void setJars(List<String> jars) {
		this.jars = jars;
	}

	@Parameter(names="--jar")
	private List<String> jars;

	public int getNumberOfRuns() {
		return numberOfRuns;
	}

	public int getQuickGenerations() {
		return quickGenerations;
	}

	public int getPopulationSize() {
		return populationSize;
	}

	public String getExchange() {
		return exchange;
	}

	public List<String> getProblem() {
		return problem;
	}

	public String getOutFile() {
		return outFile;
	}
	
	public int getGenerations() {
		return generations;
	}
}
