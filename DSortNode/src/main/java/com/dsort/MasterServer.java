package com.dsort;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MasterServer {

	static boolean listeningSocket = true;
	static List<FileData> pivots = new ArrayList<FileData>();
	static int nodeCount = 0;
	
	public void startServer() throws IOException {
		
		// 3. Start Server
		ServerSocket serverSocket = null;
		try {
			serverSocket = new ServerSocket(7077);
			System.out.println("Waiting for client on port " + serverSocket.getLocalPort() + "...");
			while(listeningSocket){
				Socket clientSocket = serverSocket.accept();
				new MasterMiniServer(clientSocket).start();
			}
			serverSocket.close();
		} catch (IOException e) {
			System.err.println("Could not listen on port: 7077");
			listeningSocket = false;
		}		
	}
}

class MasterMiniServer extends Thread {
	private Socket clientSocket;

	public MasterMiniServer(Socket socket) throws IOException {
		clientSocket = socket;
	}

	@SuppressWarnings("unchecked")
	public void run() {
		ObjectInputStream in = null;
		ObjectOutputStream out = null;
		try {
			//Socket clientSocket = clientSocket.accept();
			System.out.println("Just connected to " + clientSocket.getRemoteSocketAddress());
			
			//read from client
			in = new ObjectInputStream(clientSocket.getInputStream());
			out = new ObjectOutputStream(clientSocket.getOutputStream());
			List<Object> clientData = (List<Object>)in.readObject();
			String op = (String)clientData.get(0);
			
			
			switch (op) {
			case "getFiles":
				List<String> files = App.filesList[((Integer)clientData.get(1))-1];
				System.out.println("Master files = : " + files);
				out.writeObject(files);
				break;
			case "pivot":
				List<FileData> clientPivots = (ArrayList<FileData>)clientData.get(1);
				MasterServer.pivots.addAll(clientPivots);
				System.out.println("client pivots = "+ clientPivots);
				System.out.println("Master pivot = "+ MasterServer.pivots);
				MasterServer.nodeCount++;
				System.out.println("count = "+MasterServer.nodeCount);
				
				// Create global pivots
				int p = App.ipmap.size(), rho = p/2;
				if(MasterServer.nodeCount == p) {
					System.out.println("p = "+ p + " \n rho = " + rho);
					Collections.sort(MasterServer.pivots);
					for (int i = 0 ; i < (p-1); i++) {
						App.globalPivots.add(MasterServer.pivots.get(((i+1)*p)+rho -1));
					}
					System.out.println("global pivots (only once)= "+ App.globalPivots);
				}
				System.out.println("pivot request complete.....");
				break;
				
			case "globalpivots":
				System.out.println("global pivot request from client .....");
				if(MasterServer.nodeCount != App.ipmap.size()) {
					out.writeObject("-1");
				}else{
					Thread.sleep(1000);
					out.writeObject(App.globalPivots);
				}
				System.out.println("globalpivots request complete.....");
				break;
				
			case "finish":
				System.out.println("finish called");
				if(MasterServer.nodeCount != 2*App.ipmap.size()) MasterServer.nodeCount ++;
				else MasterServer.listeningSocket = false;
				break;
				
			default:
				break;
			}
			in.close();
			out.close();
			clientSocket.close();
		}
		catch(IOException | ClassNotFoundException | InterruptedException e){ 
			e.printStackTrace();
		} 		
	}
}