
    package invertexindex;  

    import org.apache.hadoop.fs.Path;  
    import org.apache.hadoop.io.Text;  
    import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
    import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  
    import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;  
    import org.apache.hadoop.mapreduce.Job;  
      
    import org.apache.hadoop.conf.Configured;  
    import org.apache.hadoop.conf.Configuration;  
    import org.apache.hadoop.util.Tool;  
    import org.apache.hadoop.util.ToolRunner;  
      
    public class InvertedIndex extends Configured implements Tool {  
      
      public int run(String[] args) throws Exception {  
      
        if (args.length != 2) {  
          System.out.printf("Usage: InvertedIndex \n");  
          return -1;  
        }  
      
        Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "Inverted Index");
	    
	    job.setJarByClass(InvertedIndex.class);
          
      
        /* 
         * We are using a KeyValueText file as the input file. 
         * Therefore, we must call setInputFormatClass. 
         * There is no need to call setOutputFormatClass, because the 
         * application uses a text file for output. 
         */  
        job.setInputFormatClass(KeyValueTextInputFormat.class);  // Here setup our customized input format  
      
        FileInputFormat.setInputPaths(job, new Path(args[0]));  
        FileOutputFormat.setOutputPath(job, new Path(args[1]));  
      
        job.setMapperClass(IndexMapper.class);  
        job.setReducerClass(IndexReducer.class);  
      
        job.setOutputKeyClass(Text.class);  
        job.setOutputValueClass(Text.class);  
      
        boolean success = job.waitForCompletion(true);  
        return success ? 0 : 1;  
      }  
      
      public static void main(String[] args) throws Exception {  
        int exitCode = ToolRunner.run(new Configuration(), new InvertedIndex(), args);  
        System.exit(exitCode);  
      }  
    }  
