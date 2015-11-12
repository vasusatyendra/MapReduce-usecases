package xmlmapreduce;



import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.*; 
 
import java.io.IOException; 

public class XmlInputFormat extends TextInputFormat  {
	
	private static final Logger log = (Logger) LoggerFactory.getLogger(XmlInputFormat.class); 
		 
		 
		  @Override 
		  public RecordReader<LongWritable, Text> createRecordReader( 
		      InputSplit split, TaskAttemptContext context) { 
		    try { 
		      return new XmlRecordReader((FileSplit) split, 
		          context.getConfiguration()); 
		    } catch (IOException ioe) { 
		      log.warn("Error while creating XmlRecordReader", ioe); 
		      return null; 
		    } 

		  }
}


