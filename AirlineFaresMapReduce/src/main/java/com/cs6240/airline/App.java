package com.cs6240.airline;
/**
 * @author : Shakti Patro and Kavyashree Ngendrakumar 
 * Its the main class
 * Also calculates the time taken for the complete job.
 * 
 */
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

/*
 * This class is used to read the arguments and call the appropriate functions
 * It calls either single threaded , or multi threaded or psedo modes.
 * 
 */
public class App {
	
	public static void main(String[] args) throws Exception {
		if(args.length != 4) {
			System.err.println("Please enter input in proper format.");
			System.err.println("Usage: App <-s/-m/-h> <input path> <output path> <mean/median/fastMedian>");
			System.exit(-1);
		}
		String mode = args[0].toLowerCase();
		String inputDir = args[1];
		String outputDir = args[2];
		String query = args[3].toLowerCase();
		
		if (!mode.equals("-s") && !mode.equals("-m") && !mode.equals("-h")) {
			System.out.println("Valid 1st argument are -s for local sequential run, -m for local parallel run"
					+ " and -h psuedo distributed or emr hadoop mode");
			System.exit(-1);
		}
		if ((mode.equals("-s") || mode.equals("-m")) && (! new File(inputDir).isDirectory())) {
			System.out.println("Input directory or output directory is not valid directory");
			System.exit(-1);
		}
		if (!query.equals("mean") && !query.equals("median") && !query.equals("fastmedian")) {
			System.out.println("Valid 4th argument should be either mean/median/fastMedian");
			System.exit(-1);
		}

		switch(mode) {
		case "-s": 	appSingleThreaded(inputDir, outputDir, query); break;
		case "-m":	appMultiThreaded(inputDir, outputDir, query); break;
		case "-h":	appPseudo(new String[]{inputDir, outputDir, query}); break;
		default:
			System.out.println("Valid 1st argument are -s for local sequential run, -m for local parallel run"
					+ " and -h psuedo distributed or emr hadoop mode");
			System.exit(-1);
		}
	}

	/**
	 * Calls the tool runner's run method - For ruunning Mapreduce Program
	 * 
	 * @param args : the input, output & mode 
	 * @throws Exception
	 */
	public static void appPseudo(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new MapReduceJob(), args);
		System.exit(res);	
	}
	

	/**
	 * Calls the single threaded inplementation  
	 * 
	 * @param inputDir
	 * @param otputDir
	 * @param query
	 * @throws Exception
	 */
	public static void appSingleThreaded(String inputDir, String otputDir, String query) throws Exception {

		SingleThreadedJob read = new SingleThreadedJob();
		final File folder = new File(inputDir);
		Map<String, String> results = read.readAllFilesInFolder(folder, query);
		createOutputs(otputDir, query, "single-threaded", results);
	}

	/**
	 * Calls the multi threaded inplementation  
	 * 
	 * @param inputDir
	 * @param otputDir
	 * @param query
	 * @throws Exception
	 */
	public static void appMultiThreaded(String inputDir, String otputDir, String query) throws Exception {
		MultiThreadedJob read = new MultiThreadedJob();
		final File folder = new File(inputDir);
		Map<String, String> results = read.readAllFilesInFolder(folder, query);
		createOutputs(otputDir, query, "multi-threaded", results);
	}

	/**
	 * This program run for single and multithreaded env 
	 * and prints the results in the given output folder
	 * 
	 * @param outputFile
	 * @param query
	 * @param mode
	 * @param results
	 */
	private static void createOutputs(String outputFile, String query, String mode, Map<String, String> results) {

		try{
			File outputFolder = new File(outputFile);
			if (!outputFolder.exists()) {
				outputFolder.mkdir();
			}
			File outputTextFile =new File(outputFolder+"/"+ mode + "-" +query+".txt");
			if (!outputTextFile.exists()) {
				outputTextFile.createNewFile();
			}

			FileWriter fw = new FileWriter(outputTextFile.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);
			for (Map.Entry<String,String> entry: results.entrySet()) {
				bw.write(entry.getKey()+"\t"+entry.getValue() + "\n");
			}
			bw.close();
		}
		catch(IOException ie){}
	}

}
