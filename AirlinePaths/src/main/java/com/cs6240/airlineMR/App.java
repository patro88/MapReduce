package com.cs6240.airlineMR;
/**
 * @author : Shakti Patro and Kavyashree Ngendrakumar 
 * 
 * Main class For application.
 * Based on input , Application specific method calls are done here  
 * 
 */
import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

/*
 * This class is used to read the arguments and call the appropriate functions
 * It calls either single threaded , or multi threaded or psedo modes.
 * 
 */
public class App {
	public static void main(String[] args) throws Exception {
		if(args.length != 2) {
			System.err.println("Please enter input in proper format.");
			System.err.println("Usage: App <input path> <output path>");
			System.exit(-1);
		}
		//code : To be used in local to delete the existing output folder 
		File file = new File(args[1]);
		if(file.exists()){
			FileUtils.forceDelete(file);
		}
		appPseudoRegression(new String[]{args[0], args[1]});
	}

	public static void appPseudoRegression(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new MapperReducer(), args);
		System.exit(res);
	}

}