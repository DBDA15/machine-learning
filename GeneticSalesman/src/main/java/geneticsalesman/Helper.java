package geneticsalesman;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;

import org.apache.spark.api.java.function.Function2;
import org.github.power.io.Out;

import com.mycila.xmltool.XMLDoc;
import com.mycila.xmltool.XMLDocBuilder;
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
		
		public static void exportPath(Path path, City[] cities, String filename) throws IOException {
			XMLTag xml = XMLDoc.newDocument(false)
					.addDefaultNamespace("http://www.opengis.net/kml/2.2")
					.addRoot("kml");
			XMLTag docTag = xml.addTag("Document");
				docTag.addTag("LineStyle")
				    	.addTag("width").setText("3")
				    	.addTag("color").setText("ff33ccff")
				    	.gotoParent();
			    StringBuilder lineCoordinates = new StringBuilder();
			    
			    //add points
				for(int id : path.getIDs()) {
					addCity(cities[id], docTag, lineCoordinates);
				}
				//close circle
				addCity(cities[0], docTag, lineCoordinates); 	
				
				//add path
				docTag.addTag("Placemark")
		    		.addTag("LineString")
		    			.addTag("coordinates")
		    				.setText(lineCoordinates.toString());
				
			try(BufferedWriter out = Out.file(filename).fromWriter(StandardCharsets.UTF_8)) {
				out.write(xml.toString());
			}
		}

		private static void addCity(City city, XMLTag docTag, StringBuilder lineCoordinates) {
			String cityCoordinates = city.getLatitude() + "," + city.getLongitude();
			lineCoordinates.append(cityCoordinates).append("\n");
			docTag.addTag("Placemark")
				.addTag("name").setText(city.getName())
				.addTag("Point")
					.addTag("coordinates")
						.setText(cityCoordinates)
					.gotoParent()
				.gotoParent();
		}
	}
}
