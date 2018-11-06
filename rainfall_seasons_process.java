/*rainfall_seasons_process.java
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

public class rainfall_seasons_process {
	public static class TokenizerMapper extends Mapper<
			LongWritable, // Type of key_in (1st parameter of method map()
			Text, // Type of value_in (2nd parameter of method map()
			Text, // Type of key_out (1st parameter of methods Context.write()
			IntWritable> // Type of value_out (2nd paramater of method Context.write()
	{
		private IntWritable temp_intw = new IntWritable(0); //this variable use make the pairs
		private Text year_t = new Text(); //this variable use make the pairs

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer s = new StringTokenizer(value.toString());
			//we run over the text input file to get the data
			//we store the year 
			if (s.hasMoreTokens())
				year_t.set(s.nextToken());
			//continue to run over the text input file
			for (int i = 0; i < 14; i++) {
				if (s.hasMoreTokens()) {
					temp_intw.set(Integer.parseInt(s.nextToken())); // take the rainfall value
				}
				//we adding the season name to each pair
				//example: (spring-2018, {rainfall value})
				if (i == 0 || i == 1 || i == 11)
					context.write(new Text("winter-" + year_t), temp_intw);
				if (i == 2 || i == 3 || i == 4)
					context.write(new Text("spring-" + year_t), temp_intw);
				if (i == 5 || i == 6 || i == 7)
					context.write(new Text("summer-" + year_t), temp_intw);
				if (i == 8 || i == 9 || i == 10)
					context.write(new Text("fall-" + year_t), temp_intw);
			} //for
		}//map
	}//TokenizerMapper

	public static class IntSumReducer extends Reducer<
			Text, // Type of key_in (1st parameter of method reduce()
			IntWritable, // Type of key_out (2nd parameter of method reduce()
			Text, // Type of key_out (1st parameter of methods Context.write()
			IntWritable> // Type of value_out (2nd paramater of method Context.write()
	{
		private int max_season_value = 0;
		private int min_season_value = Integer.MAX_VALUE;
		private IntWritable result = new IntWritable();
		private String max_s_date = "";
		private String min_s_date = "";
		private IntWritable max_season_intw = new IntWritable(0);
		private IntWritable min_season_intw = new IntWritable(999);

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			//we sum all the months value that belongs to the same year and season
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum); //this value is the rainfall of each season in each year
			context.write(key, result); //now the pairs will be like: (spring-2018, {rainfall value of all the year})
			//find the max season value and save it
			if (sum > max_season_value) {
				max_season_value = sum;
				max_s_date = key.toString();
			}//end if
			//find the min season value and save it
			if (sum < min_season_value) {
				min_season_value = sum;
				min_s_date = key.toString();
			}//end if
		}//reduce

		public void cleanup(Context context) throws IOException, InterruptedException {			
			//convert int values to hadoop intwritable values
			max_season_intw.set(max_season_value);
			min_season_intw.set(min_season_value);
			//print the max and min season values
			context.write(new Text("Max rainfall season is: " + max_s_date + ", the value is:"), max_season_intw);
			context.write(new Text("Min rainfall season is: " + min_s_date + ", the value is:"), min_season_intw);
		}
	}

	public static void main(String[] args) throws Exception {
		// create a configuration object for the job
		Configuration conf = new Configuration();
		// set a name of the job
		Job job = Job.getInstance(conf, "rainfall process");

		job.setJarByClass(rainfall_seasons_process.class);
		
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