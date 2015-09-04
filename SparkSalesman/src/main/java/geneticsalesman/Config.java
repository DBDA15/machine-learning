package geneticsalesman;

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
	
	@Parameter(names="--tournamentShuffle")
	private boolean tournamentShuffle=false;
	
	@Parameter(required=true)
	private String problem;
	
	@Parameter(names="--outPath", required=true)
	private String outPath;

	public int getNumberOfRuns() {
		return numberOfRuns;
	}

	public int getQuickGenerations() {
		return quickGenerations;
	}

	public int getPopulationSize() {
		return populationSize;
	}

	public void setNumberOfRuns(int numberOfRuns) {
		this.numberOfRuns = numberOfRuns;
	}

	public void setQuickGenerations(int quickGenerations) {
		this.quickGenerations = quickGenerations;
	}

	public void setGenerations(int generations) {
		this.generations = generations;
	}

	public void setPopulationSize(int populationSize) {
		this.populationSize = populationSize;
	}

	public void setProblem(String problem) {
		this.problem = problem;
	}

	public void setOutPath(String outPath) {
		this.outPath = outPath;
	}

	public boolean isTournamentShuffle() {
		return tournamentShuffle;
	}
	
	public void setTournamentShuffle(boolean tournamentShuffle) {
		this.tournamentShuffle = tournamentShuffle;
	}

	public String getProblem() {
		return problem;
	}

	public String getOutPath() {
		return outPath;
	}
	
	public int getGenerations() {
		return generations;
	}
}
