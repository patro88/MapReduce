package com.cs6240.airline;

/*
 * @author : Shakti Patro
 * Class Name : ReadDir.java
 * Purpose : i.   Reads the folder with all csv.gz files
 * 			 ii.  Spawns threads for each file 
 * 			 iii. Gets the map from each thread and calculates average and median
 *           iv.  Prints the K, F and flight details in the console.// 
 */	
import java.io.File;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class MultiThreadedJob {

	private Map<String, String> finalAvgPriceMap= new TreeMap<String, String>(); 
	private Map<String, List<Double>>finalFlightPriceMap = new HashMap<String, List<Double>>();
	//private int finalValueForK = 0, finalValueForF = 0;
	/*
	 * This method performs following tasks :
	 * 1. Reads the folder 
	 * 2. For each file spawns a thread, using the callable class of A1Inhale
	 * 3. Gets the future objects and prepares the map with average and median values
	 * 4. Prints out the results
	 */
	public Map<String, String> readAllFilesInFolder(final File folder, String oper) throws InterruptedException, ExecutionException {
		ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
		Set<Callable<Map<String, List<Double>> >> callables = new HashSet<Callable<Map<String, List<Double>>>>();
		
		for (final File fileEntry : folder.listFiles()) {
			FileEvaluator a1 = new FileEvaluator(fileEntry); 
			callables.add(a1);
		}
			
			List<Future<Map<String, List<Double>>>> futures = executorService.invokeAll(callables);
			for(Future<Map<String, List<Double>>> future : futures){
				Map<String, List<Double>> retunedValues = future.get();
				for (Map.Entry<String, List<Double>> entry : retunedValues.entrySet()) {
					String key = entry.getKey();
					if(key == "K") {
						//finalValueForK += entry.getValue().get(0).intValue();
					} else if (key == "F") {
						//finalValueForF += entry.getValue().get(0).intValue();
					} else {
						if(finalFlightPriceMap.get(key) == null) {
							finalFlightPriceMap.put(key, entry.getValue());
						} else {
							List<Double> list = finalFlightPriceMap.get(key);
							list.addAll(entry.getValue());
							finalFlightPriceMap.put(key, list);
						}
					}
				}
			}			
			performCalculations(oper);
			//printResults();
			executorService.shutdown();
			return finalAvgPriceMap;
	}

	/*
	 * This method calculates mean,median or fast median from the map and populates the finalAvgPriceMap 
	 */
	private void performCalculations(String oper){
		for (Map.Entry<String, List<Double>> entry : finalFlightPriceMap.entrySet()) {
			String key = entry.getKey();
			List<Double> priceList = entry.getValue();
			Double calculatedPrice;
			if(oper.equals("mean")) {
				calculatedPrice = Calculations.calculateMean(priceList);
			} else {
				calculatedPrice = Calculations.calculateMedian(priceList);
			}
			
			DecimalFormat formatter = new DecimalFormat("###.##");
			if(FileEvaluator.getFlightsActiveIn2015().contains(key.split("\t")[1])) {
				finalAvgPriceMap.put(key, formatter.format(calculatedPrice));
			}
		}
	}

}
