package geneticsalesman;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;

import org.apache.spark.api.java.function.Function2;

import com.mycila.xmltool.XMLDoc;
import com.mycila.xmltool.XMLTag;

public interface Helper {
	public static class MinMax implements Serializable {
		
		public double getMin() {
			return min;
		}
		public double getMax() {
			return max;
		}
		public MinMax() {}
		public MinMax(double min, double max) {
			this.min=min;
			this.max=max;
		}
		public static final Function2<MinMax, Path, MinMax> GENERATE = new Function2<MinMax, Path, MinMax>() {
			@Override
			public MinMax call(MinMax v1, Path v2) throws Exception {
				double l=v2.getLength();
				double min=(l<v1.min)?l:v1.min;
				double max=(l>v1.max)?l:v1.max;
				
				return new MinMax(min, max);
			}
		};
		public static final Function2<MinMax, MinMax, MinMax> AGGREGATE = new Function2<MinMax, MinMax, MinMax>() {
			
			@Override
			public MinMax call(MinMax v1, MinMax v2) throws Exception {
				double min=(v2.min<v1.min)?v2.min:v1.min;
				double max=(v2.max>v1.max)?v2.max:v1.max;
				return new MinMax(min, max);
			}
		};
		private double min=Double.MAX_VALUE;
		private double max=Double.MIN_VALUE;
	}
	
	public static class KMLExport {
		
		public static void exportPath(Path path, Problem problem, String filename) throws IOException {
			City[] cities=problem.getCities();
			
			XMLTag xml = XMLDoc.newDocument(false)
					.addDefaultNamespace("http://www.opengis.net/kml/2.2")
					.addRoot("kml");
			XMLTag docTag = xml.addTag("Document");
				docTag.addTag("Style").addAttribute("id","optimum").addTag("LineStyle")
				    	.addTag("width").setText("2")
				    	.addTag("color").setText("ff33ccff")
				    	.gotoParent().gotoParent()
			    	.addTag("Style").addAttribute("id","found").addTag("LineStyle")
				    	.addTag("width").setText("2")
				    	.addTag("color").setText("ffffffff")
				    	.gotoParent().gotoParent();
			    StringBuilder lineCoordinates = new StringBuilder();
			    
			    //add points
				for(int id : path.getIDs()) 
					addCity(cities[id], docTag, lineCoordinates);
				//close circle
				addCity(cities[0], null, lineCoordinates); 	
				
				//add path
				addPath(docTag, "found", lineCoordinates);
				
				if(problem.getOptimal() != null) {
					StringBuilder optimumCoordinates = new StringBuilder();
					for(int id : problem.getOptimal().getIDs()) 
						addCity(cities[id], null, optimumCoordinates);
					addCity(cities[0], null, lineCoordinates); 
					addPath(docTag, "optimum", optimumCoordinates);
				}
				
				
			try(BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filename), StandardCharsets.UTF_8))) {
				out.write(xml.toString());
			}
		}

		public static void addPath(XMLTag docTag, String marker, StringBuilder lineCoordinates) {
			docTag.addTag("Placemark")
				.addTag("styleUrl").setText("#"+marker)
				.addTag("name").setText(marker)
				.addTag("LineString")
					.addTag("coordinates")
						.setText(lineCoordinates.toString()).gotoParent().gotoParent();
		}

		private static void addCity(City city, XMLTag docTag, StringBuilder lineCoordinates) {
			String cityCoordinates = city.getLongitude()+","+city.getLatitude();
			lineCoordinates.append(cityCoordinates).append("\n");
			if(docTag!=null) docTag.addTag("Placemark")
				.addTag("name").setText(city.getName())
				.addTag("Point")
					.addTag("coordinates")
						.setText(cityCoordinates)
					.gotoParent()
				.gotoParent();
		}
	}
}
