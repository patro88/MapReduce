package com.cs6240.airline;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.lang.time.StopWatch;

/**
 * @author : Shakti Patro and Kavyashree Ngendrakumar
 * Its the main class for single threaded application
 * Also calculates the time taken for the complete job.
 *
 */

public class App {

	public static void main(String[] args) throws Exception {
		for (int i = 1; i <= 3; i++) {
			StopWatch watch = new StopWatch();
			if(args.length != 3) {
				System.err.println("Please enter input in proper format.");
				System.err.println("------USAGE-------");
				System.err.println("jar input output operation");
				System.err.println("EX:: Airline.jar /input/all output median");
			}
			watch.start();
			SingleThreadedJob read = new SingleThreadedJob();
			final File folder = new File(args[0]);
			Map<String, String> results = read.readAllFilesInFolder(folder, args[2]);
			watch.stop();
			long timeTaken = watch.getTime();
			
			createOutputs(args[1], args[2], timeTaken, results, i);
		}
		
	}

     //Calulate the time taken to run single threaded application
	private static void createOutputs(String output, String opr, long timeTaken, Map<String, String> results, int iter) {
		
		try{
            File outputFolder = new File(output);
            if (!outputFolder.exists()) {
                outputFolder.mkdir();
            }
            File outputTextFile =new File(outputFolder+"/"+"outputDataSingleThread"+ iter +opr+".txt");
            if (!outputTextFile.exists()) {
                outputTextFile.createNewFile();
            }
            File timeTakenFile =new File(outputFolder+"/"+"timeTakenSingleThread"+ opr+".txt");
            if (!timeTakenFile.exists()) {
                timeTakenFile.createNewFile();
            }
            FileWriter fw1 = new FileWriter(outputTextFile.getAbsoluteFile());
            BufferedWriter bw1 = new BufferedWriter(fw1);
            FileWriter fw2 = new FileWriter(timeTakenFile.getAbsoluteFile());
            BufferedWriter bw2 = new BufferedWriter(fw2);
            for (Map.Entry<String,String> entry: results.entrySet()) {
            	bw1.write(entry.getKey()+"\t"+entry.getValue() + "\n");
            }
            bw1.close();
            bw2.append(String.valueOf(timeTaken/1000));
            bw2.close();
        }
		catch(IOException ie){}
	}
}
