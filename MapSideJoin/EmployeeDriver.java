


import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class EmployeeDriver extends Configured implements Tool{

		public static void main(String[] args) throws Exception
		{
			ToolRunner.run(new EmployeeDriver(),args);
		}

		@Override
		public int run(String[] args) throws Exception {

			Configuration config = new Configuration();
			//config.addResource(new Path("/usr/local/hadoop/conf/core-site.xml"));
			
			 
			
			//DistributedCache.setLocalFiles(config, "/home/cloudera/workspace/mapsidejoin/cache/dept.csv");
			//DistributedCache.setLocalFiles(config, "");
			DistributedCache.addCacheFile(new URI("/user/training/cache/dept.csv"),config);
			DistributedCache.addCacheFile(new URI("/user/training/cache/emp-dept.csv"),config);
			Job job = Job.getInstance(config);
			job.setJarByClass(EmployeeDriver.class);
			//job.setCacheFiles(new Path("/home/cloudera/workspace/mapsidejoin/cache/emp-dept.csv").toUri());
			
			job.setMapperClass(EmployeeMapper.class);

			job.setInputFormatClass(TextInputFormat.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			job.setNumReduceTasks(0);

			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));

			/*FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));*/


			job.waitForCompletion(true);



			return 0;
		}


	}



