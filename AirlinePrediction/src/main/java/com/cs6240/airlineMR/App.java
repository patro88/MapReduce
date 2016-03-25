package com.cs6240.airlineMR;
/**
 * @author : Shakti Patro and Kavyashree Ngendrakumar 
 * @collaborator : Pankaj and Kartik
 * Main class For application.
 * Based on input , Application specific method calls are done here  
 * 
 */
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

/*
 * This class is used to read the arguments and call the appropriate functions
 * It calls either single threaded , or multi threaded or psedo modes.
 * 
 */
public class App {
	static String R_PATH="";
	public static void main(String[] args) throws Exception {
		if(args.length != 4) {
			System.err.println("Please enter input in proper format.");
			System.err.println("Usage: App <input path> <output path>");
			System.exit(-1);
		}
		//code : To be used in local to delete the existing output folder 
		File file1 = new File(args[3]+"/train");
		File file2 = new File(args[3]+"/test");
		File file3 = new File(args[3]+"/validate");
		FileUtils.deleteDirectory(file1);
		FileUtils.deleteDirectory(file2);
		FileUtils.deleteDirectory(file3);
		appPrediction(args);
	}

	public static void appPrediction(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new MapperReducer(), args);
		if(res != 0) System.exit(res);
		final File testData = new File(args[3]+"/test");
		final File trainData =  new File(args[3]+"/train");
		Map<String, String> monthlyFiles = getMonthwiseFiles(trainData, args[3]);
		runRScritToPredict(monthlyFiles, testData, args[3]);
		
	}
	
	private static Map<String, String> getMonthwiseFiles(File trainData, String output) {
		Map<String, String> monthlyFiles = new HashMap<String, String>();
		for (final File fileEntry : trainData.listFiles()) {
			if(fileEntry.getName().contains("SUCCESS") || fileEntry.getName().startsWith(".")) continue;
			String month = fileEntry.getName().split("_")[1].split("-")[0];
			System.out.println("month= "+month);
			String file = monthlyFiles.get(month);
			if(file != null) {
				file = file + " " + output+ "/train/" +fileEntry.getName();
				monthlyFiles.put(month, file);
			} else{
				monthlyFiles.put(month, output+ "/train/" +fileEntry.getName());
			}
		}
		return monthlyFiles;
	}
	
	private static void runRScritToPredict(Map<String, String> monthlyFiles, File testData, String output) throws InterruptedException {
		//R_PATH = "/usr/local/bin/RScript ";
		R_PATH = "/usr/bin/RScript ";
		ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
		for (final File fileEntry : testData.listFiles()) {
			if(fileEntry.getName().contains("SUCCESS") || fileEntry.getName().startsWith(".")) continue;
			String month = fileEntry.getName().split("_")[1].split("-")[0];
			if(monthlyFiles.get(month) == null) continue;
			Runnable predictor = new Predictor(
					output+ "/test/" +fileEntry.getName(), 
					output+ "/validate/"+ fileEntry.getName(), 
					monthlyFiles.get(month));
			executorService.execute(predictor);
		}
		executorService.shutdown();
		while(!executorService.awaitTermination(300, TimeUnit.SECONDS)){
			System.out.println("Waiting for R threads to complete !!!!!");
		};
		System.out.println("All tasks are finished!");
		
		//call method to prepare confusion matrix 

	}

	
	private static class Predictor implements Runnable{
		private String arguments;
		public Predictor(String testArg, String validateArg, String trainArg) {
			this.arguments = testArg+ " " + validateArg+ " " +trainArg;
			System.out.println(this.arguments);
		}
		
		@Override
		public void run() {
			String rScriptFileName = " PredictRandomForest.R ";
			try {
				Process child = Runtime.getRuntime().exec(R_PATH + rScriptFileName + arguments);
				int code = child.waitFor();
				switch (code) {
				case 0:
					break;
				case 1:
					String message = IOUtils.toString(child.getErrorStream());
					System.err.println(message);
				}
				System.out.println("executed!!!!!!!");
			} catch (IOException |InterruptedException e) {}
		}
	}
	
	
}