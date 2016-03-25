package com.cs6240.airlineMR;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import com.opencsv.CSVParser;

/*
 * author: Shakti Patro , Kavyashree nagendrakumar
 *
 * Class: To set configuaration and job properties to run mapreduce job
 */
public class MapperReducer extends Configured implements Tool{

	static CSVParser csv_parser = new CSVParser();
	final static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
	
	@Override
	public int run(String[] args) throws Exception {

		Job job1 = new Job(getConf(), this.getClass().toString());
		job1.setJobName("MapperReducerTrain");
		job1.setJarByClass(MapperReducer.class);
		MultipleOutputs.addNamedOutput(job1, "model", TextOutputFormat.class, CompositeGroupKey.class, CompositeGroupValue.class);
		LazyOutputFormat.setOutputFormatClass(job1, TextOutputFormat.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[3]+"/train"));
		job1.setMapperClass(MapClass.class);
		job1.setMapOutputKeyClass(CompositeGroupKey.class);
		job1.setMapOutputValueClass(CompositeGroupValue.class);
		job1.setReducerClass(ReduceClass.class);
		job1.setOutputKeyClass(CompositeGroupKey.class);
		job1.setOutputValueClass(CompositeGroupValue.class);
		job1.submit();


		Job job2 = new Job(getConf(), this.getClass().toString());
		job2.setJobName("MapperReducerTest");
		job2.setJarByClass(MapperReducer.class);
		MultipleOutputs.addNamedOutput(job2, "model", TextOutputFormat.class, CompositeGroupKey.class, CompositeGroupValue.class);
		LazyOutputFormat.setOutputFormatClass(job2, TextOutputFormat.class);
		FileInputFormat.addInputPath(job2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(args[3] + "/test"));
		job2.setMapperClass(MapClass.class);
		job2.setMapOutputKeyClass(CompositeGroupKey.class);
		job2.setMapOutputValueClass(CompositeGroupValue.class);
		job2.setReducerClass(ReduceClass.class);
		job2.setOutputKeyClass(CompositeGroupKey.class);
		job2.setOutputValueClass(CompositeGroupValue.class);
		job2.submit();
		
		Job job3 = new Job(getConf(), this.getClass().toString());
		job3.setJobName("MapperReducerValidate");
		job3.setJarByClass(MapperReducer.class);
		MultipleOutputs.addNamedOutput(job3, "model", TextOutputFormat.class, CompositeGroupKey.class, Text.class);
		LazyOutputFormat.setOutputFormatClass(job3, TextOutputFormat.class);
		FileInputFormat.addInputPath(job3, new Path(args[2]));
		FileOutputFormat.setOutputPath(job3, new Path(args[3] + "/validate"));
		job3.setMapperClass(ValidationMapper.class);
		job3.setMapOutputKeyClass(CompositeGroupKey.class);
		job3.setMapOutputValueClass(Text.class);
		job3.setReducerClass(ValidateReducer.class);
		job3.setOutputKeyClass(CompositeGroupKey.class);
		job3.setOutputValueClass(Text.class);
		job3.submit();

		while(!job1.isComplete() || !job2.isComplete() || !job3.isComplete()) {}
		return 0;
	}

	/**
	 * 
	 * @author Shakti
	 * map class for the mapreduce app
	 * map method aggregates all results from files into a List
	 * It used WholeFileInputReader to read whole file at a time 
	 * Clean up method is called at the end to iterate through the lists 
	 * and calculate connections and missed connections based on the conditions
	 *
	 */
	public static class MapClass extends Mapper<LongWritable, Text, CompositeGroupKey, CompositeGroupValue> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] flightDetails = csv_parser.parseLine(value.toString());
			String[] finalFlights=new String[110];
			if(flightDetails.length == 112) {
				System.arraycopy(flightDetails, 1, finalFlights, 0, 110);
			}else {
				finalFlights = flightDetails;
			}
			if(finalFlights.length == 110) {
				try {
					AirlineDetailsPojo airline = null;
					if(context.getJobName().equals("MapperReducerTrain")) {
						airline = new AirlineDetailsPojo(finalFlights);
						AirlineSanity.sanityCheck(airline);
					}else {
						airline = new AirlineDetailsPojo(finalFlights, true);
					}
					
					String carrier = airline.getCarrier();
					String year = String.valueOf(airline.getYear());
					String flightNumber = airline.getFlightNumber();
					String month = String.valueOf(airline.getMonth()); 
					String dayOfWeek = airline.getDayOfWeek();
					String dayOfMonth = airline.getDayOfMonth();
					String hourOfDay = String.valueOf(airline.getActualArrivalTime()/100);
					String origin = airline.getOrigin(); 
					String destination = airline.getDestination(); 
					String distance = airline.getDistance(); 
					String flightDate = airline.getfLDate();
					String crsDepTime = String.valueOf(airline.getCrsDepartureTime());
					String crsElapsedTime = String.valueOf(airline.getCrsElapsedTime());
					String delay = String.valueOf(airline.getArrivalDelayMinutes()); 
					Date formattedFLDate = toDate(flightDate);
					int daysUntilNearestHoliday = Utils.closerDate(formattedFLDate, Utils.getHolidays(formattedFLDate)) ;

					CompositeGroupValue outputValue = new CompositeGroupValue(carrier, origin, format.format(formattedFLDate), destination, 
							flightNumber, dayOfMonth, dayOfWeek, hourOfDay, crsDepTime, crsElapsedTime, 
							distance,  Integer.toString(daysUntilNearestHoliday), delay);
					CompositeGroupKey outputKey = new CompositeGroupKey(year, month);
					context.write(outputKey, outputValue);
				} 
				catch (Exception e) {}
			}
		}
	}


	/**
	 * 
	 * @author Shakti
	 *	Reducer Class for the app
	 *	It accumulates all the connections for the flight and returns the sum 
	 *	
	 */
	public static class ReduceClass extends Reducer<CompositeGroupKey, CompositeGroupValue, CompositeGroupKey, CompositeGroupValue> {

		private MultipleOutputs<CompositeGroupKey, CompositeGroupValue> multipleOutputs;
		@Override
		public void setup(Context context){
			multipleOutputs = new MultipleOutputs<CompositeGroupKey, CompositeGroupValue>(context);
		}
		@Override
		public void reduce(CompositeGroupKey key, Iterable<CompositeGroupValue> values, Context context)
				throws IOException, InterruptedException {

			for (CompositeGroupValue v : values) {
				multipleOutputs.write("model",key, v , key.year+"_"+key.month);
			}
		}
		@Override
		protected void cleanup(
				Reducer<CompositeGroupKey, CompositeGroupValue, CompositeGroupKey, CompositeGroupValue>.Context context)
						throws IOException, InterruptedException {
			multipleOutputs.close();
			super.cleanup(context);
		}
	}


	public static class ValidationMapper extends Mapper<LongWritable, Text, CompositeGroupKey, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] line = csv_parser.parseLine(value.toString());
			String[] flightData = line[0].split("_");
			CompositeGroupKey opkey = new CompositeGroupKey(flightData[1].split("-")[0], flightData[1].split("-")[1].replaceAll("^0", ""));
			context.write(opkey, new Text(line[0]+"\t"+line[1]));
		}
	}
	
	
	/**
	 * 
	 * @author Shakti
	 *	Reducer Class for the app
	 *	It accumulates all the connections for the flight and returns the sum 
	 *	
	 */
	public static class ValidateReducer extends Reducer<CompositeGroupKey, Text, CompositeGroupKey, Text> {

		private MultipleOutputs<CompositeGroupKey, Text> multipleOutputs;
		@Override
		public void setup(Context context){
			multipleOutputs = new MultipleOutputs<CompositeGroupKey, Text>(context);
		}
		@Override
		public void reduce(CompositeGroupKey key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			for (Text v : values) {
				multipleOutputs.write("model",key, v , key.year+"_"+key.month);
			}
		}
		@Override
		protected void cleanup(
				Reducer<CompositeGroupKey, Text, CompositeGroupKey, Text>.Context context)
						throws IOException, InterruptedException {
			multipleOutputs.close();
			super.cleanup(context);
		}
	}
	

	private static Date toDate(String d) throws ParseException {
		Date date = null;
		if(StringUtils.isNotEmpty(d)){
			if (d.contains("/")) {
				d.replace("/", "-");
			}
			date = format.parse(d);
		}
		return date;
	}
}
