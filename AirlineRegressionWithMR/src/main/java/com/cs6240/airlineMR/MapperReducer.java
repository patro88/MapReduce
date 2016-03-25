package com.cs6240.airlineMR;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.cs6240.exception.InsaneInputException;
import com.cs6240.exception.InvalidFormatException;
import com.opencsv.CSVParser;

/*
 * author: Shakti Patro , Kavyashree nagendrakumar
 */

/*
 * Class: To set configuaration and job properties to run mapreduce job
 */
public class MapperReducer extends Configured implements Tool{

	// Uses external library to parse CSV
	static CSVParser csv_parser = new CSVParser();
	static String pathToR = "/usr/local/bin/";

	@Override
	public int run(String[] args) throws Exception {

		Job job = new Job(getConf(), this.getClass().toString());
		job.setJobName("MapperReducer");
		job.setJarByClass(MapperReducer.class);

		Path inputpath = new Path(args[1]);
		Path outputpath = new Path(args[2]);
		FileInputFormat.addInputPath(job, inputpath);
		FileOutputFormat.setOutputPath(job, outputpath);

		job.setMapperClass(MapClass.class);
		job.setMapOutputKeyClass(CompositeGroupKey.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(CompositeGroupKey.class);
		job.setOutputValueClass(DoubleWritable.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	/**
	 * For each line of input, break the line into words and emit them as
	 * (<b>airline-month</b>, <b>avg price</b>).
	 */
	public static class MapClass extends Mapper<LongWritable, Text, CompositeGroupKey, DoubleWritable> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			if(key.get() > 0) {
				String[] flightDetails = csv_parser.parseLine(value.toString());
				//System.out.println(flightDetails.length+",...............");
				if(flightDetails.length == 110) {
					try {
						AirlineDetailsPojo airline = new AirlineDetailsPojo(flightDetails);
						AirlineSanity.sanityCheck(airline);
						String aircode = airline.getCarrier();
						String year = airline.getYear().toString();
						String elapsedtime=String.valueOf(airline.getActualElapsedTime());
						double price = airline.getPrice();					
						CompositeGroupKey compositekey = new CompositeGroupKey(aircode, year, elapsedtime);
						context.write(compositekey, new DoubleWritable(price));

					} catch (InvalidFormatException e) {
						System.err.println("Error*******");
					} catch (InsaneInputException e) {
						System.err.println("Error*******");
					}
				}
			}
		}

	}


	/**
	 * A reducer class that just emits the average, m of the input values.
	 */
	public static class Reduce extends Reducer<CompositeGroupKey, DoubleWritable, CompositeGroupKey, DoubleWritable> {

		@Override
		public void reduce(CompositeGroupKey key, Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException {
			List<Double> pricecache = new ArrayList<Double>();
			for (DoubleWritable v : values) {
				pricecache.add(v.get());
			}
			double medianvalue=getMean(pricecache);
			context.write(key, new DoubleWritable(medianvalue));
		}
	}
	
	
	/*
	 * @author : Kartik Mahaley, Pankaj Tripathi Function Name : getMean Purpose
	 * : For a list of price value it returns mean.
	 */
	static Double getMean(List<Double> values) {
		Double sum = 0.0, mean = 0.0;
		Integer count = 0;
		for (double v : values) {
			sum += v;
			count++;
		}
		mean = sum / count;
		return mean;
	}
	
	/**
	 * print the usage information
	 * 
	 */
	static int printUsage() {
		System.out.println("AirlineFare [-m <maps>] [-r <reduces>] <input> <output>");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}
}
