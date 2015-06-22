

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class WordCount extends Configured implements Tool {

   static public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
      final private static LongWritable ONE = new LongWritable(1);
      private Text tokenValue = new Text();

      @Override
      protected void map(LongWritable offset, Text text, Context context) throws IOException, InterruptedException {
         for (String token : text.toString().split("\\|")) {
            tokenValue.set(token);
            context.write(tokenValue, ONE);
         }
      }
   }

   static public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
      private LongWritable total = new LongWritable();

      @Override
      protected void reduce(Text token, Iterable<LongWritable> counts, Context context)
            throws IOException, InterruptedException {
         long n = 0;
         for (LongWritable count : counts)
            n += count.get();
         total.set(n);
         context.write(token, total);
         System.out.println("from combiner/reducer");
      }
   }
   
   static public class WordNamePartitioner extends Partitioner<Text, LongWritable>
   {
       @Override
       public int getPartition(Text key, LongWritable value, int numPartitions)
       {
    	   System.out.println("from partitioner");
           return Math.abs(key.hashCode() * 127) % numPartitions;
       }
   }
   

   public int run(String[] args) throws Exception {
      Configuration configuration = getConf();

      Job job = new Job(configuration, "Word Count");
      job.setJarByClass(WordCount.class);

      job.setMapperClass(WordCountMapper.class);
      job.setCombinerClass(WordCountReducer.class);
      job.setPartitionerClass(WordNamePartitioner.class);
      job.setReducerClass(WordCountReducer.class);
      
      job.setNumReduceTasks(5);
      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(LongWritable.class);

      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      return job.waitForCompletion(true) ? 0 : -1;
   }

   public static void main(String[] args) throws Exception {
      System.exit(ToolRunner.run(new WordCount(), args));
   }
}
