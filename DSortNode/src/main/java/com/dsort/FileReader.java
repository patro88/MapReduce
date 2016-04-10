package com.dsort;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 * @author Pankaj Tripathi 
 * @author Kartik Mahaley 
 * @author Shakti Patro 
 * @author Chen Bai
 * 
 * Functionality:
 * This class reads all the data from the files. The files that are read include File containing the data 
 * and a file containing the IP addresses of all the active EC2 instances.
 */
public class FileReader {

	public static final int WBAN_NUMBER = 0;
	public static final int YEARMONTHDAY = 1;
	public static final int TIME = 2;
	public static final int DRY_BULB_TEMP = 8;

	/**
	 * @param folderPath
	 * @return List<FileData>
	 * @throws Exception 
	 */
	public static List<FileData> readFile(String folderPath) throws Exception{
		File dir = new File(folderPath);
		File[] files = dir.listFiles();
		String line;

		List<FileData> fileData = new ArrayList<FileData>();
		InputStream fs;
		GZIPInputStream gs;
		Reader reader;
		BufferedReader br = null;

		for(File f : files) {
			if(f.getName().endsWith("gz")){
				fs = new FileInputStream(f);
				gs = new GZIPInputStream(fs);
				reader = new InputStreamReader(gs, "UTF-8");
				br = new BufferedReader(reader);

				//Read the file line by line
				while ((line = br.readLine()) != null)   {
					String[] row = line.split(",");
					FileData temp = getFileData(row);
					fileData.add(temp);
				}
			}
		}
		br.close();
		return fileData;
	}

	/**
	 * @param row
	 * @return FileData
	 * This method reads the data from a row in file and constructs a FileData object 
	 * @throws Exception 
	 */
	public static FileData getFileData(String[] row) throws Exception {
		FileData record = new FileData();
		try{
			record.setWban(Integer.parseInt(row[WBAN_NUMBER]));
			record.setYearMonthDay(Integer.parseInt(row[YEARMONTHDAY]));
			record.setTime(Integer.parseInt(row[TIME]));
			record.setDryBulbTemp(Double.parseDouble(row[DRY_BULB_TEMP]));

		}catch(Exception e) { 
			throw new Exception("Exception");
		}
		
		return record;
	}

	/**
	 * @param ipFile
	 * @throws IOException
	 * @return ips
	 * This method reads the file containing the IPs of the active EC2 instances
	 * and returns the list containing their IPs
	 * */
	public static List<String> getAllIPs(String ipFile) throws IOException {
		FileInputStream fis = new FileInputStream(ipFile);
		BufferedReader br = new BufferedReader(new InputStreamReader(fis));
		List<String> ips = new ArrayList<String>();
		String line;

		while ((line = br.readLine()) != null)   {
			if(!line.trim().equals("null")){
				ips.add(line.trim());
			}		  
		}
		br.close();
		return ips;
	}
}
