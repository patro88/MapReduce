package com.dsort;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class App {

	static int nodeIdentity; 
	static Map<Integer, String> ipmap = new HashMap<Integer, String>();
	static String masterip = null;
	static List<String>[] filesList = null;
	static List<FileData> globalPivots = new ArrayList<FileData>();
	static boolean globalPivotsCompleteFlag = false;
	static AmazonS3 s3;
	static String bucket;
	static String key;
	static String pwd;
	
	public static void main(String[] args) throws IOException {

		bucket = "patrosp";
		key = "AKIAJZ4FWRH2QNRPVK6Q";
		pwd = "Bdfi6I4OGmmxUqyVgkifgXzfbGPhGPG4aAqJ9ic2";
		s3 = new AmazonS3Client(new BasicAWSCredentials(key, pwd));
		
		if(args[0].equals("master")) {
			// 1. Read IP file and create a map to each node
			s3ReadIpFile(bucket, "ipaddress.txt");
			filesList  = s3ReadBuckets(bucket, "a9/climate");
			System.out.println("array length" + filesList.length);
			new MasterServer().startServer();
			
		} else {
			nodeIdentity = Integer.parseInt(args[1]);
//			nodeIdentity = 3;
			new Server().start();
			new Client().start();
		}
		
	}
	
	@SuppressWarnings("unchecked")
	public static List<String>[] s3ReadBuckets(String bucket, String folder) throws IOException {
		ObjectListing objs = s3.listObjects(new ListObjectsRequest().withBucketName(bucket).withPrefix(folder));
		Integer fileCount = 0;
		int nodes = ipmap.size();
		System.out.println("nodes = "+ nodes);
		List<String>[] filesForNodes = new ArrayList[nodes]; 
		for (S3ObjectSummary obj : objs.getObjectSummaries()) {
			List<String> files = filesForNodes[fileCount%nodes];
			if(files == null) { 
				files = new ArrayList<String>();
			}
			files.add(obj.getKey());
			filesForNodes[fileCount%nodes] = files;
			fileCount++;
		}
		return filesForNodes;
	}
	
	
	public static void s3ReadIpFile(String bucket, String file) throws IOException {
		S3Object s3o =s3.getObject(new GetObjectRequest(bucket,file));
		BufferedReader reader=new BufferedReader(new InputStreamReader(s3o.getObjectContent()));
		while(true){
			String line=reader.readLine();
			if(line==null)break;
			String[] ips  = line.split(","); 
			int nodeNum = Integer.parseInt(ips[0]);
			if(nodeNum ==0) masterip = ips[2];
			else ipmap.put(nodeNum, line);
		}
	}
}
