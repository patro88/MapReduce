package com.dsort;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

/**
 * @author Pankaj Tripathi 
 * @author Kartik Mahaley 
 * @author Shakti Patro 
 * @author Chen Bai
 * 
 * Functionality:
 * Fetches the data and IP from the files and then calls for sorting the data between the nodes.
 */
public class Client extends Thread{

	static List<FileData> localData = new ArrayList<FileData>();
	static List<FileData> pivots = new ArrayList<FileData>();
	static int p ;

	/**
	 * @param args
	 *		  1. iplist
	 *		  2. s3 input path
	 *		  3. s3 output path
	 * 		  
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException 
	 */
	@SuppressWarnings("unchecked")
	public void run() {
		try {
			//step 1.1 : read ip files , put into map
			App.s3  = new AmazonS3Client(new BasicAWSCredentials(App.key, App.pwd));
			App.s3ReadIpFile(App.bucket, "ipaddress.txt");

			// step 2: Start Client , connect to master to get file list, close client conn
//			App.masterip = "localhost";
			System.out.println("master = "+ App.masterip);
			Socket client = new Socket(App.masterip, 7077);
			System.out.println("Just connected to " + client.getRemoteSocketAddress());

			List<Object> outputList = new ArrayList<Object>();
			outputList.add("getFiles");
			outputList.add(App.nodeIdentity);
			ObjectOutputStream objectOut = new ObjectOutputStream(client.getOutputStream());
			objectOut.writeObject(outputList);

			ObjectInputStream in = new ObjectInputStream(client.getInputStream());
			List<String> filesList = (ArrayList<String>) in.readObject();
			System.out.println(filesList);

//			in.close();
//			objectOut.close();
			client.close();

			// step 3: Read File, Sort and Create pivots
			for (String fileName : filesList) {
				readS3FileContents(App.bucket, fileName);
			}
			Collections.sort(localData);
			p = App.ipmap.size();
			int w = localData.size()/p;
			System.out.println("w= " + w);
			for (int j=0; j < p; j++) {
				pivots.add(localData.get((j*w)));
			}
			System.out.println("pivots = "+ pivots);
			// step 4: Start Client , connect to master to send pivots, close client conn
			Socket client2 = new Socket(App.masterip, 7077);
			outputList = new ArrayList<Object>(); 
			outputList.add("pivot"); 
			outputList.add(pivots);
			objectOut = new ObjectOutputStream(client2.getOutputStream());
			objectOut.writeObject(outputList);
			objectOut.close();
			client2.close();

			// step 5: global pivots, close client conn
			List<FileData> globalPivots = null;
			while(true) {
				Socket client3 = new Socket(App.masterip, 7077);
				outputList = new ArrayList<Object>(); 
				outputList.add("globalpivots"); 
				objectOut = new ObjectOutputStream(client3.getOutputStream());
				objectOut.writeObject(outputList);
				in = new ObjectInputStream(client3.getInputStream());
				Object outObj = in.readObject();
				if (!(outObj instanceof String)) {
					globalPivots = (List<FileData>) outObj;
					System.out.println("globalPivots = "+ globalPivots);
					System.out.println("first value globalPivots = "+ globalPivots.get(0).toString());
					in.close();
					objectOut.close();
					break;
				} else{
					Thread.sleep(1000);
				}
				client3.close();
			}

			// step 5: Split based on global pivots  
			Map<Integer, List<FileData>> nodalFiles = new HashMap<Integer, List<FileData>>();
			nodalFiles = getSections(globalPivots);
			Server.finalSortedResult.addAll(nodalFiles.get(App.nodeIdentity));
			
			// step 6: Start client connection to other nodes, share data based on pivot
			for (Map.Entry<Integer, List<FileData>> entry : nodalFiles.entrySet()) {
				Integer nodeNum = entry.getKey();
				if(!nodeNum.equals(App.nodeIdentity)) {
					String ipAddress = App.ipmap.get(nodeNum).split(",")[2];
					Socket nodeClient = new Socket(ipAddress, 6066+nodeNum);
//					Socket nodeClient = new Socket("localhost", 6066+nodeNum);
					ObjectOutputStream nodeOut = new ObjectOutputStream(nodeClient.getOutputStream());
					nodeOut.writeObject(entry.getValue());
					nodeOut.close();
					nodeClient.close();
				}
			}
			
			Socket client4 = new Socket(App.masterip, 7077);
			outputList = new ArrayList<Object>(); 
			outputList.add("finish");
			objectOut = new ObjectOutputStream(client4.getOutputStream());
			objectOut.writeObject(outputList);
			in.close();
			objectOut.close();
			client4.close();
			
		} catch (IOException | ClassNotFoundException |InterruptedException e) {
			e.printStackTrace();
		}

	}

	private void readS3FileContents(String bucket, String file) throws IOException {
		S3Object s3o = App.s3.getObject(new GetObjectRequest(bucket,file));
		BufferedReader reader=new BufferedReader(new InputStreamReader(new GZIPInputStream(s3o.getObjectContent())));
		reader.readLine();
		String line=null;
		while((line=reader.readLine()) != null){
			String[] row = line.split(",");
			try{
				FileData record = FileReader.getFileData(row);
				localData.add(record);
			}catch (Exception e) {}


		}
		reader.close();
	}


	private Map<Integer, List<FileData>> getSections(List<FileData> pivots) {		
		Map<Integer, List<FileData>> sections = new HashMap<Integer, List<FileData>>();
		int currentPivotsIndex = 0;
		int from = 0;
		int key = 1;
		FileData pivot = pivots.get(currentPivotsIndex);

		for (int i = 0; i < localData.size(); i++) {
			if (localData.get(i).compareTo(pivot) == 1) {
				sections.put(key, new ArrayList<FileData>(localData.subList(from, i)));
				from = i;
				currentPivotsIndex ++;
				key++;
				if (currentPivotsIndex >= pivots.size()) {
					break;
				}
				pivot = pivots.get(currentPivotsIndex);
			}
		}
		sections.put(key, new ArrayList<FileData>(localData.subList(from, localData.size())));
		return sections;
	}

}