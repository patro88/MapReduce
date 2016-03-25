package com.cs6240.airlineMR;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import com.cs6240.exception.InsaneInputException;
import com.cs6240.exception.InvalidFormatException;
import com.opencsv.CSVParser;

/*
 * author: Shakti Patro , Kavyashree nagendrakumar
 *
 * Class: To set configuaration and job properties to run mapreduce job
 */
public class MapperReducer extends Configured implements Tool{

	static CSVParser csv_parser = new CSVParser();
	static long ONE_MINS_IN_MILLIS = (60*1000);
	static long SIX_HOURS_IN_MILLIS = (6*60*ONE_MINS_IN_MILLIS);
	static long THIRTY_MINS_IN_MILLIS = (30*ONE_MINS_IN_MILLIS);

	@Override
	public int run(String[] args) throws Exception {

		Job job = new Job(getConf(), this.getClass().toString());
		job.setJobName("MapperReducer");
		job.setJarByClass(MapperReducer.class);
		job.setInputFormatClass(WholeFileInputFormat.class);
		WholeFileInputFormat.addInputPath(job, new Path(args[0]));
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(MapClass.class);
		job.setMapOutputKeyClass(CompositeGroupKey.class);
		job.setMapOutputValueClass(CompositeGroupValue.class);
		job.setReducerClass(ReduceClass.class);
		//job.setCombinerClass(ReduceClass.class);
		job.setOutputKeyClass(CompositeGroupKey.class);
		job.setOutputValueClass(CompositeGroupValue.class);
		job.setNumReduceTasks(10);
		return job.waitForCompletion(true) ? 0 : 1;
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
	public static class MapClass extends Mapper<NullWritable, BytesWritable, CompositeGroupKey, CompositeGroupValue> {

		@Override
		protected void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
			
			Map<CompositeGroupKey, List<List<CompositeGroupValue>>> monthly = new HashMap<CompositeGroupKey, List<List<CompositeGroupValue>>>();
			List<CompositeGroupValue> destinationList ;
			List<CompositeGroupValue> originList ;
			byte[] content = value.getBytes();
			InputStream is = new ByteArrayInputStream(content);
			BufferedReader bufReader = new BufferedReader(new InputStreamReader(new GZIPInputStream(is)));
			bufReader.readLine();
			String line = bufReader.readLine();
			while (line != null) {
				String[] flightDetails = csv_parser.parseLine(line);
				if(flightDetails.length == 110) {

					try {
						AirlineDetailsPojo airline = new AirlineDetailsPojo(flightDetails);
						AirlineSanity.sanityCheck(airline);
						String carrier = airline.getCarrier();
						String year = airline.getYear().toString();
						String cancelled = airline.getCancelled();
						String flDate = airline.getfLDate();
						long actualArrival = Utils.getTimeFromString(flDate, String.valueOf(airline.getActualArrivalTime()));
						long scheduledArrival = Utils.getTimeFromString(flDate, String.valueOf(airline.getCrsArrivalTime()));
						long actualDeparture = Utils.getTimeFromString(flDate, String.valueOf(airline.getActualDepartureTime()));
						long scheduledDeparture = Utils.getTimeFromString(flDate, String.valueOf(airline.getCrsDepartureTime()));
						CompositeGroupKey cKey = new CompositeGroupKey(carrier, year);
						CompositeGroupValue dVal = new CompositeGroupValue(carrier, year, cancelled, 0L, 0L, actualArrival, scheduledArrival, "A", airline.getDestination());
						CompositeGroupValue oVal = new CompositeGroupValue(carrier, year, cancelled, 0L, 0L, actualDeparture, scheduledDeparture, "A", airline.getOrigin());
						if(Utils.isFirstDayAndInRange(actualArrival)) context.write(cKey, dVal);
						if(Utils.isLastDayAndInRange(actualDeparture)) context.write(cKey, oVal);
						List<List<CompositeGroupValue>> vals = monthly.get(cKey);
						if(null == vals || vals.isEmpty()) {
							vals = new ArrayList<List<CompositeGroupValue>>();
							destinationList = new ArrayList<CompositeGroupValue>();
							originList = new ArrayList<CompositeGroupValue>();
						} else{
							destinationList = vals.get(0);
							originList = vals.get(1);
						}
						destinationList.add(dVal);
						originList.add(oVal);
						vals.add(0, destinationList);
						vals.add(1, originList);
						monthly.put(cKey, vals);
					} catch (InvalidFormatException | InsaneInputException | ParseException e) {} 
				}
				line = bufReader.readLine();
			}
			computeConnections(context, monthly);
		}

		private void computeConnections(
				Mapper<NullWritable, BytesWritable, CompositeGroupKey, CompositeGroupValue>.Context context,
				Map<CompositeGroupKey, List<List<CompositeGroupValue>>> monthly) throws IOException, InterruptedException {
			for(Map.Entry<CompositeGroupKey, List<List<CompositeGroupValue>>> entry : monthly.entrySet()) {
				long connections = 0L, missed = 0L;
				List<CompositeGroupValue> destinationList = entry.getValue().get(0);
				List<CompositeGroupValue> originList = entry.getValue().get(1);
				for (CompositeGroupValue dair : destinationList) {
					for (CompositeGroupValue oair : originList) {
						if(!dair.airport.equals(oair.airport)) continue;
						if(oair.scheduled <= dair.scheduled + SIX_HOURS_IN_MILLIS
								&& oair.scheduled >= dair.scheduled + THIRTY_MINS_IN_MILLIS) {
							connections++;
							if(dair.cancelled.equals("1") || dair.actual > oair.actual - THIRTY_MINS_IN_MILLIS) 
								missed++;
						}
					}
				}
				CompositeGroupKey key = entry.getKey();
				CompositeGroupValue value = new CompositeGroupValue(key.carrier, key.year, "0", connections, missed, 0L, 0L, "N", "");
				context.write(key, value);
			}
			
		}

		
		
//		void getConnections(Context context,
//				List<CompositeGroupValue> destinationList,
//				List<CompositeGroupValue> originList) throws IOException,
//				InterruptedException {
//			for (CompositeGroupValue dair : destinationList) {
//				for (CompositeGroupValue oair : originList) {
//
//					if(!dair.carrier.equals(oair.carrier) || dair.airport.equals(oair.airport)) break;
//						long connections = 0L, missedConnections = 0L;
//						if(oair.scheduled <= dair.scheduled + SIX_HOURS_IN_MILLIS
//								&& oair.scheduled >= dair.scheduled + THIRTY_MINS_IN_MILLIS) {
//							connections++;
//							if(dair.cancelled.equals("1") || dair.actual > oair.actual - THIRTY_MINS_IN_MILLIS) 
//								missedConnections++;
//							CompositeGroupKey cKey = new CompositeGroupKey(dair.carrier, dair.year);
//							CompositeGroupValue cVal = new CompositeGroupValue();
//							cVal.connections = connections;
//							cVal.isArriving = "N";
//							cVal.missedConnections = missedConnections;
//							context.write(cKey, cVal);
//						}
//					}
//				}
//			}
		}

//		void computeConnectionsFor(CompositeGroupValue a, CompositeGroupValue d) {
//			if (!a.airport.equals(d.airport) || !a.carrier.equals(d.carrier)) return;
//			if(d.scheduled <= a.scheduled + SIX_HOURS_IN_MILLIS
//					&& d.scheduled >= a.scheduled + THIRTY_MINS_IN_MILLIS) {
//				connections++;
//				if (d.cancelled.equals("1") || d.actual > a.actual - THIRTY_MINS_IN_MILLIS) missed++;
//			}
//		}
//	}


	/**
	 * 
	 * @author Shakti
	 *	Reducer Class for the app
	 *	It accumulates all the connections for the flight and returns the sum 
	 *	
	 */
	public static class ReduceClass extends Reducer<CompositeGroupKey, CompositeGroupValue, CompositeGroupKey, Text> {

		List<CompositeGroupValue> residualArrivals = new ArrayList<CompositeGroupValue>();
		List<CompositeGroupValue> residualDepartures = new ArrayList<CompositeGroupValue>();
		long connections = 0, missedConnections = 0;

		@Override
		public void reduce(CompositeGroupKey key, Iterable<CompositeGroupValue> values, Context context)
				throws IOException, InterruptedException {
			for (CompositeGroupValue v : values) {
				if(v.isArriving.equals("N")) {
					connections += v.connections;
					missedConnections += v.missedConnections;
				}
				else{
					CompositeGroupValue cval = new CompositeGroupValue(v.carrier, v.year, v.cancelled, 
							v.connections, v.missedConnections, v.actual, v.scheduled, v.isArriving, v.airport);
					if(v.isArriving.equals("A")) residualArrivals.add(cval);
					else residualDepartures.add(cval);
				}
			}
			updateConnectionsFromResiduals(residualArrivals, residualDepartures);
			context.write(key, new Text(connections+ AirlineConstants.SEPARATOR +missedConnections));
		}

		private void updateConnectionsFromResiduals(List<CompositeGroupValue> arrivals, List<CompositeGroupValue> departures) {
			for (CompositeGroupValue dair : arrivals) {
				for (CompositeGroupValue oair : departures) {
					if(dair.carrier.equals(oair.carrier)
							&& dair.year.equals(oair.year)
							&& dair.airport.equals(oair.airport)) {
						if(oair.scheduled <= dair.scheduled + SIX_HOURS_IN_MILLIS
								&& oair.scheduled >= dair.scheduled + THIRTY_MINS_IN_MILLIS) {
							connections++;
							if(dair.cancelled.equals("1") || dair.actual > oair.actual - THIRTY_MINS_IN_MILLIS) 
								missedConnections++;
						}
					}
				}
			}
		}

	}
}
