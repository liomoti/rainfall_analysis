/*rainfall_mnx_process.java
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
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class rainfall_mnx_process {
	public static class TokenizerMapper extends Mapper<
			LongWritable, // Type of key_in (1st parameter of method map()
			Text, // Type of value_in (2nd parameter of method map()
			Text, // Type of key_out (1st parameter of methods Context.write()
			IntWritable> // Type of value_out (2nd paramater of method Context.write()
	{
		private IntWritable temp_intw = new IntWritable(0);
		private Text year_t = new Text();
		private String max_month_date ="";
		private String min_month_date ="";
		private int max_month = 0 ;
		private int min_month = 999 ;	
		private IntWritable max_month_intw = new IntWritable(0);
		private IntWritable min_month_intw = new IntWritable(999);
		private IntWritable all_avg_intw = new IntWritable(0);
		private int all_avg_int = 0;
		private int count_years = 0;
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer s = new StringTokenizer(value.toString());
			int i=0;
			//we run over the text input file to get the data
			//we store the year 
			if (s.hasMoreTokens())
				year_t.set(s.nextToken());
			//continue to run over the text input file
			for (i = 0; i < 12; i++) {
				if (s.hasMoreTokens()) {
					temp_intw.set(Integer.parseInt(s.nextToken())); // take the rainfall value
				}
				context.write(year_t, temp_intw); //make pair for the Reduce function (year , rainfall amount)
				// checking the max rainfall for month
				if (temp_intw.get() > max_month) 
				{
					max_month = temp_intw.get();
					int i_temp = i+1;
					max_month_date = Integer.toString(i_temp)+"/"+year_t.toString();
				}
				// checking the min rainfall for month
				if (temp_intw.get() < min_month) 
				{
					min_month = temp_intw.get();
					int i_temp = i+1;
					min_month_date = Integer.toString(i_temp)+"/"+year_t.toString();
				}
			} //for
			if (s.hasMoreTokens())
				all_avg_int += Integer.parseInt(s.nextToken());
			count_years++; //counting the years = how many lines in the text file
		} //map function
		protected void cleanup(Context context) throws IOException, InterruptedException {
			//convert int values to hadoop intwritable values
			max_month_intw.set(max_month);
			min_month_intw.set(min_month);
			all_avg_int = all_avg_int/count_years;
			all_avg_intw.set(all_avg_int);
			//print the results
			context.write(new Text("The Average of all the years is: "), all_avg_intw);			
			context.write(new Text("Max rainfall month is: " + max_month_date + ", the value is:"), max_month_intw);
			context.write(new Text("Min rainfall month is: " + min_month_date + ", the value is:"), min_month_intw);
		} //cleanup function
	}

	public static class IntSumReducer extends Reducer<
			Text, // Type of key_in (1st parameter of method reduce()
			IntWritable, // Type of key_out (2nd parameter of method reduce()
			Text, // Type of key_out (1st parameter of methods Context.write()
			IntWritable> // Type of value_out (2nd paramater of method Context.write()
	{
		private IntWritable result = new IntWritable();
		private String max_year_date = "";
		private String min_year_date = "";
		private int min_year = 999;
		private int max_year = 0;
		private IntWritable max_year_intw = new IntWritable(0);
		private IntWritable min_year_intw = new IntWritable(999);	
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) { //Aggregation of the values
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result); //making the pairs: (year , rainfall amount)
			if (result.get() > max_year) { // checking the max rainfall for year
				max_year = result.get();
				max_year_date = key.toString();
			}
			if (result.get() < min_year) { // checking the min rainfall for year
				min_year = result.get();
				min_year_date = key.toString();
			}
		}
		protected void cleanup(Context context) throws IOException, InterruptedException {
			//convert int values to hadoop intwritable values
			max_year_intw.set(max_year); 
			min_year_intw.set(min_year);
			//print the result
			context.write(new Text("Min rainfall year is: " + min_year_date + ", the value is:"), min_year_intw);
			context.write(new Text("Max rainfall year is: " + max_year_date + ", the value is:"), max_year_intw);
		}//cleanup
	}//reducer

	public static void main(String[] args) throws Exception {
		// create a configuration object for the job
		Configuration conf = new Configuration();
		// set a name of the job
		Job job = Job.getInstance(conf, "rainfall min-max process");

		job.setJarByClass(rainfall_mnx_process.class);
		// specify name of Mapper and Reducer class
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);

		// specify data type of output key and value
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

	    // set input and output directories using command line arguments,
	    // arg[0] = name of input directory on HDFS, and
	    // arg[1] = name of output directory to be created to store the output file
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}