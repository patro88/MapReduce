package com.dsort;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Server extends Thread{

	static List<FileData> finalSortedResult = new ArrayList<FileData>();
	static int totalClients = 0;
	@SuppressWarnings("unchecked")
	public void run(){

		ServerSocket serverSocket = null;

		boolean listeningSocket = true;
		try {
			int port = 6066 + App.nodeIdentity;
			serverSocket = new ServerSocket(port);
			System.out.println(InetAddress.getLocalHost().getHostAddress());
			System.out.println("Waiting for client on port " + serverSocket.getLocalPort() + "...");
			
			while(listeningSocket){
				Socket clientSocket = serverSocket.accept();
				totalClients ++;
				System.out.println("Just connected to " + clientSocket.getRemoteSocketAddress());
				ObjectInputStream in = new ObjectInputStream(clientSocket.getInputStream());
				List<FileData> clientData = (ArrayList<FileData>) in.readObject();
				finalSortedResult.addAll(clientData);
				if(totalClients == Client.p -1) {
					Collections.sort(finalSortedResult);
					File file = new File("output-000"+App.nodeIdentity);
					if (!file.exists()) {
						file.createNewFile();
					}
					FileWriter fw = new FileWriter(file.getAbsoluteFile());
					BufferedWriter bw = new BufferedWriter(fw);
					for (FileData fileData : finalSortedResult) {
						bw.write(fileData.toString()+"\n");
					}
					bw.close();
					listeningSocket = false;
				}
				in.close();
				clientSocket.close();
			}
			serverSocket.close();
		} catch (IOException e) {
			System.err.println("Could not listen on port: 6066");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}		       
	}

}