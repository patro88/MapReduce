package com.cs6240.airlineMR;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
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

import com.cs6240.exception.InsaneInputException;
import com.cs6240.exception.InvalidFormatException;
/*
 * @author : Shakti Patro
 * Class Name : GraphPlotter.java
 */
public class MedianMapReducer extends Configured implements Tool {

	public int run(String[] args) throws Exception {

		Configuration configuration = new Configuration();
		configuration.set("carrier", args[3]);
		Job job = new Job(configuration, "MedianMapReducer");
		job.setJarByClass(MedianMapReducer.class);

		Path inputpath = new Path(args[1]);
		Path outputpath = new Path(args[2]);
		FileInputFormat.addInputPath(job, inputpath);
		FileOutputFormat.setOutputPath(job, outputpath);
		job.setMapperClass(MedianMapper.class);
		job.setReducerClass(MedianReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class MedianMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
		/*
		 * @author : Patro, Shakti 
		 * Function Name : map
		 * Purpose : returns key as year and week of the year
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// do not process header row
			if (key.get() > 0) {
				String[] flightDetails = null;
				String line = parseCityName(value.toString()).replaceAll("\"", "");
				flightDetails = line.split(",");

				if (flightDetails.length == 110) {
					try {
						AirlineDetailsPojo airline = new AirlineDetailsPojo(flightDetails);
						AirlineSanity.sanityCheck(airline);
						String query = context.getConfiguration().get("carrier");
						if (airline.getCarrier().equals(query)) {
							String year = airline.getYear().toString();
							String week = getWeekOfDate(airline.getfLDate());
							double price = airline.getPrice();
							context.write(new Text(year + "\t" + week), new DoubleWritable(price));
						}

					} catch (InvalidFormatException e) {
						e.printStackTrace();
					} catch (InsaneInputException e) {
						e.printStackTrace();
					} catch (ParseException e) {
						e.printStackTrace();
					}
				}

			}

		}
	}

	public static class MedianReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		/*
		 * @author : Kartik Mahaley, Pankaj Tripathi 
		 * Function Name : reduce
		 * Purpose : for a given key, takes input as list of the prices and returns median for it
		 */
		@Override
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException {
			List<Double> pricecache = new ArrayList<Double>();
			for (DoubleWritable v : values) {
				pricecache.add(v.get());
			}
			double medianValue = getMedian(pricecache);
			context.write(key, new DoubleWritable(medianValue));
		}

	}

	/*
	 * @author : Kartik Mahaley, Pankaj Tripathi 
	 * Function Name : getWeekOfDate
	 * Purpose : takes date as string and returns week of the year
	 */
	static String getWeekOfDate(String flDate) throws ParseException {
		Date date = null;
		if (flDate.contains("/")) {
			SimpleDateFormat dateFormatter1 = new SimpleDateFormat("MM/dd/yyyy");
			date = dateFormatter1.parse(flDate);
		} else if (flDate.contains("-")) {

			SimpleDateFormat dateFormatter2 = new SimpleDateFormat("yyyy-MM-dd");
			date = dateFormatter2.parse(flDate);
		}
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		int week = cal.get(Calendar.WEEK_OF_YEAR);
		return String.valueOf(week);
	}

	/*
	 * @author : Kartik Mahaley, Pankaj Tripathi Function Name : getMedian
	 * Purpose : For a list of price value it returns median.
	 */
	static Double getMedian(List<Double> values) {
		Collections.sort(values);
		Double median;
		if (values.size() % 2 == 0)
			median = (values.get(values.size() / 2) + values.get(values.size() / 2 - 1)) / 2;
		else
			median = values.get(values.size() / 2);
		return median;
	}

	private static String parseCityName(String row) {
		StringBuilder builder = new StringBuilder(row);

		// below steps are done to replace any "comma" inside the data with a
		// "semicolon"
		// code referred from stack overflow
		boolean inQuotes = false;
		for (int currentIndex = 0; currentIndex < builder.length(); currentIndex++) {
			char currentChar = builder.charAt(currentIndex);
			if (currentChar == '\"')
				inQuotes = !inQuotes; // toggle state
			if (currentChar == ',' && inQuotes) {
				builder.setCharAt(currentIndex, ';');
			}
		}
		return builder.toString();
	}

}
