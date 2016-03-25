package com.cs6240.airline;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.cs6240.exception.InsaneInputException;
import com.cs6240.exception.InvalidFormatException;

/*
 * author: Shakti Patro , Kavyashree nagendrakumar
 */

/*
 * Class: To set configuaration and job properties to run mapreduce job
 */
public class MapReduceJob extends Configured implements Tool{

	static Set<String> flightsActiveIn2015 = new HashSet<String>();
	static String operation = "mean";

	@Override
	public int run(String[] args) throws Exception {

		JobConf conf = new JobConf(getConf(), MapReduceJob.class);
		conf.setJobName("airlinefares");

		conf.setMapOutputKeyClass(CompositeGroupKey.class);
		conf.setMapOutputValueClass(DoubleWritable.class);
		conf.setOutputKeyClass(CompositeGroupKey.class); 
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(MapClass.class);
		//conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		List<String> other_args = new ArrayList<String>();
		for(int i=0; i < args.length; ++i) {
			try {
				if ("-m".equals(args[i])) {
					conf.setNumMapTasks(Integer.parseInt(args[++i]));
				} else if ("-r".equals(args[i])) {
					conf.setNumReduceTasks(Integer.parseInt(args[++i]));
				} else {
					other_args.add(args[i]);
				}
			} catch (NumberFormatException except) {
				System.err.println("ERROR: Integer expected instead of " + args[i]);
				return printUsage();
			} catch (ArrayIndexOutOfBoundsException except) {
				System.err.println("ERROR: Required parameter missing from " +
						args[i-1]);
				return printUsage();
			}
		}
		// Make sure there are exactly 2 parameters left.
		if (other_args.size() != 3) {
			System.err.println("ERROR: Wrong number of parameters: " +
					other_args.size() + " instead of 2.");
			return printUsage();
		}
		FileInputFormat.setInputPaths(conf, other_args.get(0));
		FileOutputFormat.setOutputPath(conf, new Path(other_args.get(1)));
		MapReduceJob.operation = other_args.get(2);

		JobClient.runJob(conf);
		
		return 0;
	}

	/**
	 * For each line of input, break the line into words and emit them as
	 * (<b>airline-month</b>, <b>avg price</b>).
	 */
	public static class MapClass extends MapReduceBase
	implements Mapper<LongWritable, Text, CompositeGroupKey, DoubleWritable> {
		public void map(LongWritable key, Text value,
				OutputCollector<CompositeGroupKey, DoubleWritable> output,
				Reporter reporter) throws IOException {

			// do not process header row
			if(key.get() > 0) {
				String[] flightDetails = null;
				String line = parseCityName(value.toString()).replaceAll("\"", "");
				flightDetails = line.split(",");

				if(flightDetails.length == 110) {
					try {
						AirlineDetailsPojo airline = new AirlineDetailsPojo(flightDetails);
						AirlineSanity.sanityCheck(airline);
						if(airline.getYear() == 2015) {
							flightsActiveIn2015.add(airline.getCarrier());
						}
						CompositeGroupKey combo = new CompositeGroupKey(airline.getCarrier(), airline.getMonth().toString());
						output.collect(combo, new DoubleWritable(airline.getPrice()));
//						Text keyVal = new Text(airline.getCarrier() + "\t" + airline.getMonth().toString());
//						output.collect(keyVal, new DoubleWritable(airline.getPrice()));
					} catch (InvalidFormatException e) {
						e.printStackTrace();
					} catch (InsaneInputException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}

	/**
	 * A reducer class that just emits the average, m of the input values.
	 */
	public static class Reduce extends MapReduceBase
	implements Reducer<CompositeGroupKey, DoubleWritable, CompositeGroupKey, DoubleWritable> {

		@Override
		public void reduce(CompositeGroupKey key, Iterator<DoubleWritable> values,
				OutputCollector<CompositeGroupKey, DoubleWritable> output,
				Reporter reporter) throws IOException {
			
			List<Double> valueList = new ArrayList<Double>();
			while (values.hasNext()) {
				valueList.add(values.next().get());
			}
			double calculatedPrice ;
			if(operation.equals("mean")) {
				calculatedPrice = Calculations.calculateMean(valueList);
			} else if(operation.equals("fastmedian")){
				calculatedPrice = Calculations.calculateFastMedian(valueList);
			} else {
				calculatedPrice = Calculations.calculateMedian(valueList);
			}
			
			output.collect(key, new DoubleWritable(calculatedPrice));
//			String carrierCode = key.toString().substring(0, 2);
//			if(flightsActiveIn2015.contains(carrierCode)) {
//				output.collect(new Text(carrierCode), new DoubleWritable(calculatedPrice));
//			}
			
		}
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


	/**
	 * 
	 * @param row : the whole row of a csv input 
	 * @return row without a quote in the individual columns.
	 */
	private static String parseCityName(String row) {
		StringBuilder builder = new StringBuilder(row);

		//below steps are done to replace any "comma" inside the data with a "semicolon"
		//code referred from stack overflow
		boolean inQuotes = false;
		for (int currentIndex = 0; currentIndex < builder.length(); currentIndex++) {
			char currentChar = builder.charAt(currentIndex);
			if (currentChar == '\"') inQuotes = !inQuotes; // toggle state
			if (currentChar == ',' && inQuotes) {
				builder.setCharAt(currentIndex, ';'); 
			}
		}
		return builder.toString();
	}

}
