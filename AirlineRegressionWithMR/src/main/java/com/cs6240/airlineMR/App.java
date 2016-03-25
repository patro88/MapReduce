package com.cs6240.airlineMR;
/**
 * @author : Shakti Patro and Kavyashree Ngendrakumar 
 * Its the main class
 * Also calculates the time taken for the complete job.
 * 
 */
import java.io.FileNotFoundException;
import java.io.IOException;

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
			System.err.println("Usage: App <input path> <output path>");
			System.exit(-1);
		}
		
		String jobType = args[0].toLowerCase();
		String inputDir = args[1];
		String outputDir = args[2];
		String jobOneArgs[]={jobType,inputDir,outputDir};
		
		if(jobType.equals("regression")){
			appPseudoRegression(jobOneArgs);
		}else{
			appPseudoMedian(args);	
		}

	}

	public static void appPseudoRegression(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new MapperReducer(), args);
		System.exit(res);
	}
	
	
	public static void appPseudoMedian(String[] args) throws Exception {

		String carrier = args[3];
        //String fileName = args[3];
        //String line = null;
        String jobType = args[0].toLowerCase();
		String inputDir = args[1];
		String outputDir = args[2];
        try {
//            FileReader fileReader = new FileReader(fileName);
//            BufferedReader bufferedReader = new BufferedReader(fileReader);
//
//            while((line = bufferedReader.readLine()) != null) {
//            	carrier = line.replace("\"", "");
//            }
            String jobTwoArgs[]={jobType,inputDir,outputDir,carrier};
        	int res = ToolRunner.run(new MedianMapReducer(), jobTwoArgs);
        	//bufferedReader.close(); 
			System.exit(res);      
        }
        catch(FileNotFoundException ex) {
            //System.err.println("Unable to open file '" + fileName + "'");                
        }
        catch(IOException ex) {
            //System.err.println("Error reading file '"  + fileName + "'");                  
        }	
	}

}




