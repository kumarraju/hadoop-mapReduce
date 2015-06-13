import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class EmployeeMapper extends Mapper<LongWritable,Text, Text, Text>{
	
	private URI[] Uris;
	private HashMap<String,String> deptTable = new HashMap<String,String>();
	private HashMap<String,String> emp_deptTable = new HashMap<String,String>();
	
	@Override
	public void setup(Context context) throws IOException
	{
		Uris = DistributedCache.getCacheFiles(context.getConfiguration());
		//paths = DistributedCache.getCacheFiles(context.getConfiguration());
		//System.out.println("files:"+ paths);
		//Path path = new Path(paths[0]);
		for(URI uri:Uris){
			System.out.println(uri.toString());
		}
		//System.out.println(paths[0]);
		FileSystem fs = FileSystem.get(context.getConfiguration());
		FSDataInputStream in = fs.open(new Path(Uris[0]));
		BufferedReader br  = new BufferedReader(new InputStreamReader(in));
        String line="";
        System.out.println("Reading deptTable");
        while((line = br.readLine())!=null)
        {
                String splits[] = line.split(",");
                //splits[0]= dept_id splits[1] =dept_name
                deptTable.put(splits[0], splits[1]);
                System.out.println(splits[0]+":"+ splits[1]);
        }
        
        in = fs.open(new Path(Uris[1]));
        br = new BufferedReader(new InputStreamReader(in));
        line="";
        System.out.println("Reading emp-deptTable");
        while((line = br.readLine())!=null)
        {
                String splits[] = line.split(",");
              //splits[0]= emp_id splits[1] =dept_id
                emp_deptTable.put(splits[0], splits[1]);
                System.out.println(splits[0]+":"+ splits[1]);
        }
        
		br.close();
		in.close();
		
		
	}
	@Override
	public void map(LongWritable key, Text val,Context context) throws IOException,InterruptedException
	{
		
		String[] splits = val.toString().split(",");
		
		//splits[0]=emp_id splits[1]=name splits[2]=city
		
		if(emp_deptTable.containsKey(splits[0]))
		{
			context.write(new Text(splits[0]+":"+emp_deptTable.get(splits[0])), new Text(deptTable.get(emp_deptTable.get(splits[0]))));
		}
		else
		{
			context.write(new Text(splits[0]),new Text("No dept Code"));
		}
		
		
	}

}


