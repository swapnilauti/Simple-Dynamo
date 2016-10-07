package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {
	private String myPort = null;                                                  // eg 11108
	private String myNodeHash = null;                                                  // hash code of my id
	private String myNodeId = null;                                                    // eg 5554
	private LinkedHashMap<String,String> successors;                                          //hashcode - portNO of successor of this nodes
	private HashSet<Integer> remotePorts;                                            // stores all portno eg 11108..
	private TreeMap<String,String> nodeTree;                                          // hashcode - portNO
	private TreeMap<String,String> tempKeyValues = new TreeMap<String, String>();                                         //stores temporary key and values
	private Uri mUri = null;
	private static final String KEY_FIELD = "key";
	private static final String VALUE_FIELD = "value";
	private ServerSocket serverSocket = null;
	private String hmString = null;
	private boolean isSendingSuccess;

	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}
	@Override//========================================MAIN FUNCTIONS======================================================
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}


	//========================================HELPER FUNCTIONS======================================================
	private void initHashSet(){
		remotePorts = new HashSet<Integer>();
		remotePorts.add(11108);
		remotePorts.add(11112);
		remotePorts.add(11116);
		remotePorts.add(11120);
		remotePorts.add(11124);
	}

	private void initNodeTree(){
		nodeTree = new TreeMap<String, String>();
		for(Iterator<Integer> i = remotePorts.iterator();i.hasNext();) {
			String portNo = i.next().toString();
			Integer nodeId = Integer.parseInt(portNo)/2;
			try{
				nodeTree.put(genHash(nodeId.toString()),portNo);
			}catch (NoSuchAlgorithmException e){
			}
		}
	}

	private LinkedHashMap<String,String> generateSuccessors(String portNo){
		LinkedHashMap<String,String> temp = new LinkedHashMap<String, String>();
		Map.Entry<String,String> mapEntry = null;
		String hashPortNo = null;
		Integer nodeId = Integer.parseInt(portNo)/2;

		try{
			hashPortNo = genHash(nodeId.toString());
		}catch (NoSuchAlgorithmException e){

		}

		temp.put(hashPortNo, portNo);

		mapEntry = nodeTree.higherEntry(hashPortNo);
		if(mapEntry==null){
			mapEntry = nodeTree.firstEntry();
		}

		temp.put(mapEntry.getKey(), mapEntry.getValue());

		mapEntry = nodeTree.higherEntry(mapEntry.getKey());

		if(mapEntry==null){
			mapEntry = nodeTree.firstEntry();
		}

		temp.put(mapEntry.getKey(), mapEntry.getValue());

		return temp;
	}

	private String getHigherPortNo(String keyHash) {
		return nodeTree.higherKey(keyHash)==null?nodeTree.get(nodeTree.firstKey()):nodeTree.get(nodeTree.higherKey(keyHash));
	}

	private String getLowerPortNo(String keyHash) {
		return nodeTree.lowerKey(keyHash)==null?nodeTree.get(nodeTree.lastKey()):nodeTree.get(nodeTree.lowerKey(keyHash));
	}

	private MatrixCursor stringToCursor(MatrixCursor m, String hmString){
		if(hmString==null || hmString.equals("")) {
			return m;
		}
		else {
			hmString = hmString.substring(1, hmString.length() - 1);
			StringBuilder sb = new StringBuilder();
			String entries[] = hmString.split(",");
			for (int i = 0; i < entries.length; i++) {
				String temp[] = entries[i].split("=");
				String[] row = new String[2];
				row[0] = temp[0].trim();
				row[1] = temp[1].trim();
				m.addRow(row);
			}
		}
		return m;
	}

	private String genHash(String input) throws NoSuchAlgorithmException {
		if(input == null)
			return null;
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}

	private String cursorToString(MatrixCursor m){
		if(m == null||m.getCount()==0)
			return "";
		String key = null;
		String value = null;
		HashMap<String,String> hm = new HashMap<String, String>();
		m.moveToFirst();
		while(!m.isAfterLast()){
			key = m.getString(0);
			value = m.getString(1);
			hm.put(key,value);
			m.moveToNext();
		}
		return hm.toString();
	}

	private void printNodeTree() {
		for (Iterator<Map.Entry<String, String>> i = nodeTree.entrySet().iterator(); i.hasNext(); ) {
			Map.Entry<String, String> entry = i.next();
		}
	}

	private void dumpCursor(MatrixCursor m){
		int keyIndex = m.getColumnIndex(KEY_FIELD);
		int valueIndex = m.getColumnIndex(VALUE_FIELD);
		ContentValues cv = null;
		if(m.getCount()==0)
			return;
		m.moveToFirst();

		try {
			while (!m.isAfterLast()) {
				cv = new ContentValues();
				cv.put(KEY_FIELD, m.getString(keyIndex));
				cv.put(VALUE_FIELD, m.getString(valueIndex));
				tempKeyValues.put(m.getString(keyIndex),m.getString(valueIndex));
				insert(mUri, cv);
				m.moveToNext();
			}
		} catch (Exception e) {

		}
	}
	//========================================MAIN FUNCTIONS======================================================
	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		String key = selection;

		if(selection.equals("@")) {
			String fileNames[] = getContext().fileList();
			for(int i=0;i<fileNames.length;i++) {
				key = fileNames[i];
				try {
					getContext().deleteFile(key);
				} catch (Exception e) {
				}
			}
		}
		else if(selection.equals("*")){
			for(Iterator<Integer> i = remotePorts.iterator();i.hasNext();) {
				Integer portNo = i.next();
				ClientTask ct = new ClientTask();
				ct.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "3", "@",portNo.toString());
			}
		}
		else{
			String keyHash = null;
			try{
				keyHash = genHash(key);
			}catch (NoSuchAlgorithmException e){
			}
			if(selectionArgs!=null) {
				try {
					getContext().deleteFile(key);
				}catch (Exception e) {
				}
			}
			else{
				String node = getHigherPortNo(keyHash);

				LinkedHashMap<String, String> tempSuccessors = generateSuccessors(node);
				for(Iterator<Map.Entry<String,String>> i=tempSuccessors.entrySet().iterator();i.hasNext();){
					Map.Entry<String,String> entry = i.next();
					ClientTask ct = new ClientTask();
					ct.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "3", selection,entry.getValue());
				}
			}
		}
		return 0;
	}


	@Override
	public Uri insert(Uri uri, ContentValues values) {
		FileOutputStream f= null;
		String key = values.getAsString("key");
		String value = values.getAsString("value");
		String keyHash = null;
		try{
			keyHash = genHash(key);
		}catch (NoSuchAlgorithmException e){

		}
		String keyPortNo = getHigherPortNo(keyHash);
		if(tempKeyValues.containsKey(key)) {
			tempKeyValues.remove(key);
			try {
				f = getContext().openFileOutput(key, 0);
			} catch (FileNotFoundException e) {
			}
			try {
				f.write(value.getBytes());
			} catch (IOException e) {
			} finally {
				try {
					f.close();
				} catch (Exception e) {

				}
			}
		}
		else{
			ClientTask ct = null;
			LinkedHashMap<String, String> tempSuccessors = generateSuccessors(keyPortNo);
			for (Iterator<Map.Entry<String, String>> i = tempSuccessors.entrySet().iterator(); i.hasNext(); ) {
				Map.Entry<String, String> entry = i.next();
				ct = new ClientTask();
				ct.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "2", entry.getValue(), key, value);
			}


		}
		return uri;
	}

	@Override
	public boolean onCreate() {

		// TODO Auto-generated method stub
		initHashSet();
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		myNodeId = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		try {
			myNodeHash = genHash(myNodeId);
		}catch (NoSuchAlgorithmException e){
		}
		myPort = String.valueOf((Integer.parseInt(myNodeId) * 2));
		initNodeTree();
		successors = generateSuccessors(myPort);
		mUri = buildUri("content", "content://edu.buffalo.cse.cse486586.simpledynamo.provider");
		try {
			serverSocket = new ServerSocket(10000);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
		}
		printNodeTree();
		delete(mUri,"@",null);

		String columnNm[] = {"key","value"};
		MatrixCursor m = new MatrixCursor(columnNm);
		Integer successorPort= Integer.parseInt(getHigherPortNo(myNodeHash));
		try {
			successorPort/=2;
			successorPort = Integer.parseInt(getHigherPortNo(genHash(successorPort.toString())));
		}catch (NoSuchAlgorithmException e){
		}
		Integer predecessorPort = Integer.parseInt(getLowerPortNo(myNodeHash));
		ClientTask ct = null;
		for(int i=0;i<2;i++){
			ct = new ClientTask();
			ct.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "0",myPort,"0",successorPort.toString());
			try {
				ct.get();
			} catch (Exception e) {
			}
			m = stringToCursor(m, hmString);
			hmString = null;
			ct = new ClientTask();
			ct.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "0", predecessorPort.toString(),i==0?"1":"0",predecessorPort.toString());
			try {
				ct.get();
			} catch (Exception e) {
			}
			m = stringToCursor(m, hmString);
			hmString = null;
			try {
				successorPort/=2;
				successorPort = Integer.parseInt(getLowerPortNo(genHash(successorPort.toString())));
				predecessorPort/=2;
				predecessorPort = Integer.parseInt(getLowerPortNo(genHash(predecessorPort.toString())));
			}catch (NoSuchAlgorithmException e){
			}
		}
		dumpCursor(m);
		return true;
	}

	public Cursor initialQuery(Uri uri, String projection, String selection,
							   String[] selectionArgs,String sortOrder) {
		FileInputStream f= null;
		BufferedReader br = null;
		String key = null;
		String columnNm[] = {"key","value"};
		String columnVal[]= new String[2];
		MatrixCursor m = new MatrixCursor(columnNm);
		String fileNames[] = getContext().fileList();
		String myPredecessor = selection;
		Integer predecessorPort = Integer.parseInt(selection);
		try {
			predecessorPort/=2;
			predecessorPort = Integer.parseInt(getLowerPortNo(genHash(predecessorPort.toString())));
		}catch (NoSuchAlgorithmException e){
		}
		for(int i=0;i<fileNames.length;i++) {
			key = fileNames[i];
			String keyHash = null;
			try {
				keyHash = genHash(key);
			} catch (NoSuchAlgorithmException e) {
			}
			String keyPortNo = getHigherPortNo(keyHash);
			if(projection.equals("1")) {
				if (!keyPortNo.equals(selection) && !keyPortNo.equals(myPredecessor))
					continue;
			}
			else if (!keyPortNo.equals(selection))
				continue;


			try {
				f = getContext().openFileInput(key);
			} catch (FileNotFoundException e) {
			}
			try{
				br = new BufferedReader(new InputStreamReader(f));
				columnVal[0]=key;
				columnVal[1]=br.readLine();
				m.addRow(columnVal);
			}
			catch(IOException e) {
			}
			finally {
				try {
					f.close();
					br.close();
				}catch (IOException e){
				}
			}
		}
		return m;
	}


	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
						String[] selectionArgs,String sortOrder) {
		FileInputStream f= null;
		BufferedReader br = null;
		String key = selection;
		String columnNm[] = {"key","value"};
		String columnVal[]= new String[2];
		MatrixCursor m = new MatrixCursor(columnNm);
		if(selection.equals("@")) {
			String fileNames[] = getContext().fileList();
			for (int i = 0; i < fileNames.length; i++) {
				key = fileNames[i];
				try {
					f = getContext().openFileInput(key);
				} catch (FileNotFoundException e) {
				}
				try {
					br = new BufferedReader(new InputStreamReader(f));
					columnVal[0] = key;
					columnVal[1] = br.readLine();
					m.addRow(columnVal);
				} catch (IOException e) {
				} finally {
					try {
						f.close();
						br.close();
					} catch (IOException e) {
					}
				}
			}
		}
		else if(selection.equals("*") ) {
			for (Iterator<Integer> i = remotePorts.iterator(); i.hasNext(); ) {
				Integer portNo = i.next();
				ClientTask ct = new ClientTask();
				ct.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "1", "@", portNo.toString());
				try {
					ct.get();
				} catch (Exception e) {
				}
				m = stringToCursor(m, hmString);
				hmString = null;
			}
		}
		else{
			if(projection!=null) {
				try {
					f = getContext().openFileInput(selection);
				} catch (FileNotFoundException e) {

				}
				try {
					br = new BufferedReader(new InputStreamReader(f));
					columnVal[0] = key;
					columnVal[1] = br.readLine();
					m.addRow(columnVal);
				} catch (IOException e) {
				} finally {
					try {
						f.close();
						br.close();
					} catch (IOException e) {
					}
				}
			}
			else {
				String keyHash = null;
				try {
					keyHash = genHash(key);
				} catch (NoSuchAlgorithmException e) {
				}
				String keyPortNo = getHigherPortNo(keyHash);
				LinkedHashMap<String, String> tempSuccessors = generateSuccessors(keyPortNo);
				ArrayList<String> portNos = new ArrayList<String>();
				for(Iterator<Map.Entry<String,String>> i=tempSuccessors.entrySet().iterator();i.hasNext();){
					Map.Entry<String,String> entry = i.next();
					portNos.add(0,entry.getValue());
				}
				int i=0;
				for(boolean flag=true;flag;i=(i+1)%3) {

					ClientTask ct = new ClientTask();
					ct.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "1", selection,portNos.get(i));
					try {
						ct.get();
					} catch (Exception e) {
					}
					if(hmString!=null && !hmString.equals("")) {
						flag = false;
						m = stringToCursor(m, hmString);
						hmString = null;
					}
				}
			}
		}

		return m;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	//========================================CLIENT SERVER CLASSES======================================================

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {


		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			BufferedReader br = null;
			PrintWriter out = null;
			OutputStream os = null;
			InputStream is = null;
			Socket socket = null;
			MatrixCursor m =null;
			ContentValues cv = null;
			while (true) {
				try {
					socket = serverSocket.accept();
					is = socket.getInputStream();
					br = new BufferedReader(new InputStreamReader(is));
					String temp = br.readLine();
					if (temp != null && temp != "") {
						String arr[] = temp.split(" ");
						String socketHash = null;
						switch (Integer.parseInt(arr[0])) {
							case 0:
								m = (MatrixCursor) initialQuery(mUri, arr[2] , arr[1], null, null);
								break;
							case 1:

								m = (MatrixCursor) query(mUri, new String[0] , arr[1], null, null);
								break;
							case 2:
								tempKeyValues.put(arr[1],arr[2]);
								cv = new ContentValues();
								cv.put(KEY_FIELD, arr[1]);
								cv.put(VALUE_FIELD, arr[2]);
								insert(mUri, cv);
								break;
							case 3:
								delete(mUri, arr[1], new String[0]);
								break;
						}
						try {
							os = socket.getOutputStream();
							out = new PrintWriter(os, true);

							if (Integer.parseInt(arr[0]) == 1 || Integer.parseInt(arr[0]) == 0) {
								out.println(cursorToString(m));
							} else{
								out.println("success");
							}

							out.flush();

						}finally{
							out.close();
							os.close();
						}

					}

				} catch (Exception e) {
				} finally {
					try {
						br.close();
						is.close();
						socket.close();
					} catch (Exception e) {

					}

				}
			}
		}
	}

	private class ClientTask extends AsyncTask<String, Void, Void> {

		private boolean messageSendUtility(Integer portNo,String msg,String option){
			Socket socket = null;
			BufferedReader br = null;
			PrintWriter out = null;
			OutputStream os = null;
			OutputStreamWriter osw = null;
			BufferedWriter bw = null;
			Integer curPort = null;
			InputStream is = null;

			try {
				socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), portNo);
				os = socket.getOutputStream();
				osw = new OutputStreamWriter(os);
				out = new PrintWriter(osw, true);
				is = socket.getInputStream();
				br = new BufferedReader(new InputStreamReader(is));
				out.println(msg);
				out.flush();
				if(option.equals("1") || option.equals("0")){
					hmString = br.readLine();
				}
				else{
					br.readLine();
				}

			}catch(Exception e){
				return false;
			}finally {
				try {
					br.close();
					is.close();
					socket.close();
				} catch (Exception e) {
				}
				return true;
			}
		}
		@Override
		protected Void doInBackground(String... msgs) {
			if(msgs[0]==null || msgs[0]=="")
				return null;
			String msgToSend ="";
			switch (Integer.parseInt(msgs[0])){
				case 0:
					for(int i=0;i<=2;i++){
						msgToSend = msgToSend.concat(msgs[i]);
						msgToSend = msgToSend.concat(" ");
					}
					isSendingSuccess = messageSendUtility(Integer.parseInt(msgs[3]),msgToSend,msgs[0]);
					break;

				case 1:
					for(int i=0;i<2;i++){
						msgToSend = msgToSend.concat(msgs[i]);
						msgToSend = msgToSend.concat(" ");
					}
					isSendingSuccess = messageSendUtility(Integer.parseInt(msgs[2]),msgToSend,msgs[0]);
					break;
				case 2:
					for(int i=0;i<msgs.length;i++){
						if(i==1)
							continue;
						msgToSend = msgToSend.concat(msgs[i]);
						msgToSend = msgToSend.concat(" ");
					}
					isSendingSuccess = messageSendUtility(Integer.parseInt(msgs[1]),msgToSend,msgs[0]);
					break;
				case 3:
					for(int i=0;i<2;i++){
						msgToSend = msgToSend.concat(msgs[i]);
						msgToSend = msgToSend.concat(" ");
					}
					isSendingSuccess = messageSendUtility(Integer.parseInt(msgs[2]),msgToSend,msgs[0]);
					break;
			}

			return null;
		}
	}
}