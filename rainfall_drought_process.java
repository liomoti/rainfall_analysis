/*rainfall_drought_process.java
 * written by: moti shaul, reut shaul, ori ivgi
 *                    .'\   /`.
 *                 .'.-.`-'.-.`.
 *           ..._:   .-. .-.   :_...
 *         .'    '-.(o ) (o ).-'    `.
 *        :  _    _ _`~(_)~`_ _    _  :
 *       :  /:   ' .-=_   _=-. `   ;\  :
 *       :   :|-.._  '     `  _..-|:   :
 *        :   `:| |`:-:-.-:-:'| |:'   :
 *         `.   `.| | | | | | |.'   .'
 *           `.   `-:_| | |_:-'   .'
 *        jgs  `-._   ````    _.-'
 *                 ``-------''
 */
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import rainfall_mnx_process.IntSumReducer;
import rainfall_mnx_process.TokenizerMapper;

public class rainfall_drought_process {
	public static class DroughtMapper extends Mapper<
	LongWritable, // Type of key_in (1st parameter of method map()
	Text, // Type of value_in (2nd parameter of method map()
	Text, // Type of key_out (1st parameter of methods Context.write()
	Text> // Type of value_out (2nd paramater of method Context.write()
{
private String temp_value = ""; //this variable use make the pairs
private String year_t = ""; //this variable use make the pairs
public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	StringTokenizer s = new StringTokenizer(value.toString());
	//we run over the text input file to get the data
	//we store the year 
	if (s.hasMoreTokens())
		year_t = (s.nextToken()).toString();
	//continue to run over the text input file
	for (int i = 0; i < 14; i++) {
		if (s.hasMoreTokens() && i == 12) { //i=12 for taking the avg value
			temp_value = (s.nextToken()).toString(); // take the rainfall value
			//we make the pairs:
			//example: (getDrought, {year}+"-"+{rainfall average value})
			context.write(new Text("Drought years"), new Text(year_t+"-"+temp_value));
		}//if					
	} //for
}//map
}//DroughtMapper
	public static class DroughtReducer extends Reducer<
	Text, // Type of key_in (1st parameter of method reduce()
	Text, // Type of key_out (2nd parameter of method reduce()
	Text, // Type of key_out (1st parameter of methods Context.write()
	IntWritable> // Type of value_out (2nd paramater of method Context.write()
	{
	private int count_years = 0;
	private int sum_avg = 0;
	private double final_avg = 0;
	private int year_1=0;
	private IntWritable sum_drought_years_intw = new IntWritable(0);
	Map<Integer, Integer> rainfall_avg_by_year = new TreeMap<Integer, Integer>();
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {			
			//the reduce merge all the pairs to <K1, List(V1)>
			Iterator<Text> ite = values.iterator();
			String last_year="";
			int sum_avg_drought=0;
			int count_drought=0;
			while (ite.hasNext()) {
				//seperate the string, get year and avg
				String[] str = ite.next().toString().split("-");			
				//count the year, sum the avg
				count_years++;
				sum_avg=sum_avg+Integer.parseInt(str[1]);	
				//we move pairs of (year,avg) to TreeMap to sort them
				rainfall_avg_by_year.put(Integer.parseInt(str[0]), Integer.parseInt(str[1]));
			}//while
			//calculating the final avg
			final_avg = sum_avg / count_years;						
			  for(Entry<Integer, Integer> entry : rainfall_avg_by_year.entrySet()) 
			  {			  	
				if(entry.getValue() < final_avg) //this year can be drought year
				{
					count_drought++;
					if(count_drought==1)
					{
						//save the first year
						year_1=entry.getKey();
						sum_avg_drought=entry.getValue();//add avg to sum
					}
					else if(count_drought==2)
					{
						//save the avg of the 2nd year
						sum_avg_drought=entry.getValue();//add avg to sum
					}
					else if(count_drought==3)
					{
						last_year=entry.getKey().toString(); //take the current last year
						sum_avg_drought=entry.getValue(); //add avg to sum
					}
					else if(count_drought>3)
					{
						sum_avg_drought=entry.getValue(); //add avg to sum
						last_year=entry.getKey().toString(); //take the current last year
					}
				}//if the year is drought year
				else { //this year cannot be drought year
					if(count_drought>2) //we had a drought years before
					{
						sum_avg_drought = sum_avg_drought*12;
						sum_drought_years_intw.set(sum_avg_drought);
						context.write(new Text("Drought years: " + year_1 + "-" + last_year+" "),sum_drought_years_intw);
					}
					count_drought=0; //reset the counter
				}//else
			  }//for		
		}//reduce
	}//DroughtReducer
	public static void main(String[] args) throws Exception {
		// create a configuration object for the job
		Configuration conf = new Configuration();
		// set a name of the job
		Job job = Job.getInstance(conf, "rainfall drought process");

		job.setJarByClass(rainfall_mnx_process.class);
		// specify name of Mapper and Reducer class
		job.setMapperClass(DroughtMapper.class);
		job.setReducerClass(DroughtReducer.class);

		// specify data type of output key and value
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

	    // set input and output directories using command line arguments,
	    // arg[0] = name of input directory on HDFS, and
	    // arg[1] = name of output directory to be created to store the output file
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}//main
}
