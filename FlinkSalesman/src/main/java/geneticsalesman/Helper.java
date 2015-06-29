package geneticsalesman;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.mycila.xmltool.XMLDoc;
import com.mycila.xmltool.XMLTag;

public interface Helper {
	
	public static class KMLExport {
		
		public static void exportPath(Path path, Problem problem, BufferedWriter kmlWriter) throws IOException {
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
				
				
			kmlWriter.write(xml.toString());
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
	
	public static class Output {
		public static BufferedWriter writer(String path) throws IOException, URISyntaxException {
			OutputStream os;
			if(path.startsWith("hdfs")) {
				Configuration configuration = new Configuration();
				FileSystem hdfs = FileSystem.get( new URI( "hdfs://" + path.split("/")[2]), configuration );
				org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(path);
				if ( hdfs.exists( file )) { hdfs.delete( file, true ); } 
				os = hdfs.create(file);
			}
			else {
				File file = new File(path);
				if(path.contains("/"))
					file.getParentFile().mkdirs();
				os = new FileOutputStream(file);
			}
			return new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
		}
	}
}
