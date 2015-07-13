import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.io.Files;

public class Main {

	public static void main(String[] args) throws IOException {
		File inputFolder = new File(args[0]);
		if(!(inputFolder.exists() && inputFolder.isDirectory())) 
			throw new IllegalArgumentException("input folder invalid");
		File outputFolder = new File(args[1]);
		outputFolder.mkdirs();
		
		for(File input : inputFolder.listFiles()) {
			System.out.println(input.getName());
			List<String> inLines = Files.readLines(input, Charsets.UTF_8);
			ArrayList<ArrayList<SPoint>> spointAll = new ArrayList<ArrayList<SPoint>>();
			ArrayList<SPoint> spoints = null;
			for(String line : inLines) {
				if(line.startsWith("Testrun")) {
					spoints = new ArrayList<SPoint>();
					spointAll.add(spoints);
				}
				if(line.length()>0 && Character.isDigit(line.charAt(0))) {
					String[] split = line.split("\t");
					spoints.add(new SPoint(Integer.parseInt(split[0]),Integer.parseInt(split[1]),Integer.parseInt(split[2])));
				}
			}
			
			if(spointAll.get(spointAll.size()-1).size()==0) {
				spointAll.remove(spointAll.size()-1);
			}
			
			ArrayList<String> outLines = new ArrayList<String>();
			
			int numTestRuns = spointAll.size();
			if(numTestRuns<10)
				System.out.println("warning : only "+numTestRuns + " test runs found");
			if(numTestRuns > 0) {
				for(int pointID=0; pointID<spointAll.get(0).size(); pointID++) {
					int[] ms = new int[numTestRuns];
					int[] perc = new int[numTestRuns];
					int[] gen = new int[numTestRuns];
					for(int i=0; i<numTestRuns; i++) {
						SPoint point = spointAll.get(i).get(pointID);
						ms[i] = point.getMs();
						perc[i] = point.getPerc();
						gen[i] = point.getGen();
					}
					assertAllEqual(gen);
					int avgPerc = median(perc);
					int avgMS = median(ms);
					outLines.add(gen[0] + "\t" + avgMS + "\t" + avgPerc);
				}
				
				Files.write(Joiner.on("\n").join(outLines), new File(outputFolder, input.getName() + ".txt"), Charsets.UTF_8);
			}
			
		}
		
		
	}

	private static int median(int[] numArray) {
		Arrays.sort(numArray);
		int median;
		if (numArray.length % 2 == 0)
		    median = (numArray[numArray.length/2] + numArray[numArray.length/2 - 1])/2;
		else
		    median =  numArray[numArray.length/2];
		return median;
	}

	private static void assertAllEqual(int[] gen) {
		int first = gen[0];
		for(int i : gen) {
			if(i!=first)
				throw new RuntimeException("found different generations on same level");
		}
	}

}
