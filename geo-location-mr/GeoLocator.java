package geolocation;


import java.io.IOException;
import java.net.URLDecoder;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class GeoLocator {
	
	public static class GeoMapper extends Mapper<LongWritable, Text, Text, Text> {


		 public static String GEO_RSS_URI = "http://www.georss.org/georss/point";
		 private Text geoLocationKey = new Text();
		 private Text geoLocationName = new Text();
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			
				String dataRow = value.toString();
				
				// since these are tab separated files lets tokenize on tab
			    StringTokenizer dataTokenizer = new StringTokenizer(dataRow, "\t");
			    String articleName = dataTokenizer.nextToken();
			    String pointType = dataTokenizer.nextToken();
			    String geoPoint = dataTokenizer.nextToken();
			    
			 // we know that this data row is a GEO RSS type point.
			    if (GEO_RSS_URI.equals(pointType)) {
			    	
			    	// now we process the GEO point data.
		            StringTokenizer st = new StringTokenizer(geoPoint, " ");
		            String strLat = st.nextToken();
		            String strLong = st.nextToken();
		            
		            double lat = Double.parseDouble(strLat);
		            double lang = Double.parseDouble(strLong);
		            
		            long roundedLat = Math.round(lat);
		            long roundedLong = Math.round(lang);
		            
		            String locationKey = "(" + String.valueOf(roundedLat) + "," + String.valueOf(roundedLong) + ")";
		            String locationName = URLDecoder.decode(articleName, "UTF-8");
		            
		            locationName = locationName.replace("_", " ");
		            geoLocationKey.set(locationKey);
					geoLocationName.set(locationName);
					
					context.write(geoLocationKey, geoLocationName);
			    	
			    	
			    }

			  }

		
	}

	
	public static class GeoReducer extends Reducer<Text, Text, Text, Text> {
		
		 private Text outputKey = new Text();
		 private Text outputValue = new Text();
		
		@Override
		  public void reduce(Text anagramKey, Iterable<Text> anagramValues, Context context) throws IOException, InterruptedException {

			
			// in this case the reducer just creates a list so that the data can
		    // used later
		    String outputText = "";
		    
		    while(anagramValues.iterator().hasNext()) {
		    	
	            Text locationName = anagramValues.iterator().next();
	            outputText = outputText + locationName.toString() + " ,";
	        }
		    
		    outputKey.set(anagramKey.toString());
		    outputValue.set(outputText);
		    context.write(outputKey, outputValue);

		  }
	}

	 public static void main(String[] args) throws Exception {

		    Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "Geo Locator");
		    
		    job.setJarByClass(GeoLocator.class);
		    
		    job.setMapperClass(GeoMapper.class);
		    job.setReducerClass(GeoReducer.class);
		    
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		    
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
}
