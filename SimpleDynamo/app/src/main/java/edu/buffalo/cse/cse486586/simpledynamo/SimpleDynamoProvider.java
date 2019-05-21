package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OptionalDataException;
import java.io.OutputStream;
import java.io.StreamCorruptedException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.Date;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.text.TextUtils;
import android.util.Log;
import java.security.Timestamp;

public class SimpleDynamoProvider extends ContentProvider {

	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	static final String REMOTE_PORT[] = {"11108", "11112", "11116", "11120", "11124"};
	static List<nodeid> chordDHT = new ArrayList<nodeid>();
	static String myPort = null;
	static String activenodes = null;
	static int global_cnt =0;
	static int currindex=0;
	static int previndex =0;
	static int succindex=0;
    static int supersuccindex=0;
    static int superprevindex=0;
	static int DHTsize =0;
	static boolean recoveryflag=false;
	//static String activenodes[]={}
	static final int SERVER_PORT = 10000;
	int avdno = 0;
	String myporthash = null;
	private Uri buildUri2(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}
	Uri mUri = buildUri2("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
    ConcurrentHashMap<String, HashSet<String>> nodekeylist = new ConcurrentHashMap<String, HashSet<String>>();
    //HashMap<String, Long> nodetimestamp = new HashMap<String, Long>();
    //ConcurrentHashMap<String,Integer> keyversion = new ConcurrentHashMap<String, Integer>();

	/*
    This class is used for creating node object having the port number and hash code of the node
    Referenced from : https://www.geeksforgeeks.org/java-util-arrays-equals-java-examples/
     */
	public class nodeid {
		public String nodeport;
		public String nodehash;
		public nodeid(String nodeport, String nodehash) {
			this.nodeport = nodeport;
			this.nodehash = nodehash;
		}
		public String getport(){
			return nodeport;
		}
		public String getnodehash() {
			return nodehash;
		}
		public boolean equals(Object o){
			nodeid newobject =  (nodeid)(o);
			if((this.nodeport.equals(newobject.nodeport))&&(this.nodehash.equals(newobject.nodehash))){
				return  true;
			}
			return false;
		}
	}
	/*
    Comparator to sort the nodes based on their hash code
    Referenced from : https://www.geeksforgeeks.org/comparable-vs-comparator-in-java/
     */
	class sortbyhash implements Comparator<nodeid>
	{
		public int compare(nodeid a, nodeid b)
		{
			return a.getnodehash().compareTo(b.getnodehash());
		}
	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
        while (!recoveryflag){
            continue;
        }
        File directory = getContext().getFilesDir();

        Log.v("deleteDelete",selection);

        /*
        deleting in local
        Referenced from : https://developer.android.com/reference/android/content/ContentProvider.html#delete(android.net.Uri,%20java.lang.String,%20java.lang.String[])
         */
        if(selection.equals("@")){
            File[] fileNames = directory.listFiles();
            for(File files : fileNames){
                File file = new File(directory, files.getName());
                this.getContext().deleteFile(files.getName());
            }
            return 0;
        }
        else if(selection.equals("*")){
            File[] fileNames = directory.listFiles();
            for(File files : fileNames){
                File file = new File(directory, files.getName());
                this.getContext().deleteFile(files.getName());
            }
            String msg = "@" + "_"+ "globaldelete";
            try {
                String result = new ClientTask_delete().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort).get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
            //if(result.contains("0")){
            return 0;
            //}
        }
        else {
            try {
                String hashkeycode = genHash(selection);

                Log.v("delete",selection);
                this.getContext().deleteFile(selection);
                String msg =  selection+ "_"+ "singlekeydelete";

                if(hashkeycode.compareTo(chordDHT.get(DHTsize-1).getnodehash())>0 ||
                        hashkeycode.compareTo(chordDHT.get(0).getnodehash())<=0){
                    msg = msg + chordDHT.get(0).getport();
                }
                else {
                    for (int i = 0; i < DHTsize; i++) {
                        if (hashkeycode.compareTo(chordDHT.get(i).getnodehash()) <= 0) {
                            msg = msg + chordDHT.get(i).getport();
                            break;
                        }
                    }
                }
                String result = new ClientTask_delete().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort).get();
                return 0;

            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }

        }
        return 0;

	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
        //System.out.println("In Insert "+ values.getAsString("key"));
        String key = values.getAsString("key");
        String value = values.getAsString("value");
        String hashkeycode = null;


        FileOutputStream outputStream;
        try {
            hashkeycode = genHash(key);
            Log.v("In Insert ",key + " "+ value + " "+ hashkeycode);

            if((hashkeycode.compareTo(chordDHT.get(previndex).getnodehash())>0
                    && hashkeycode.compareTo(chordDHT.get(currindex).getnodehash())<=0 && !key.contains("replica")) ||
                    (currindex==0 && hashkeycode.compareTo(chordDHT.get(currindex).getnodehash())<=0 && !key.contains("replica")) ||
                    (currindex==0 && hashkeycode.compareTo(chordDHT.get(previndex).getnodehash())>0 && !key.contains("replica"))){

                System.out.println("Reached the correct node "+ key +" "+value);
                while (!recoveryflag){
                    continue;
                }
                outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
                outputStream.write(value.getBytes());
                outputStream.close();

                String msg = key+"_"+value + "_"+ "replica"+ "_"+myPort;
                new ClientTask_insert().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);

                HashSet<String>keys=nodekeylist.get(myPort);
                keys.add(key);
                nodekeylist.put(myPort,keys);
                Log.v("Hashmap insertion1", myPort+" "+key +" "+ value + " "+ hashkeycode);
                return uri;

            }
            else{
                if(key.contains("replica")){
                    key=key.split("\\_")[0];
                    while (!recoveryflag){
                        continue;
                    }
                    System.out.println("Reached here for inserting replica "+key +" "+ value);
                    outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
                    outputStream.write(value.getBytes());
                    outputStream.close();

                    return uri;
                }
                else {
                    //String msg = key+"_"+value + "_"+ "prevnode";
                    String msg = key+"_"+value + "_"+"replica"+ "_";//+myPort;
                    if(hashkeycode.compareTo(chordDHT.get(DHTsize-1).getnodehash())>0 ||
                                hashkeycode.compareTo(chordDHT.get(0).getnodehash())<=0){
                            msg = msg + chordDHT.get(0).getport();
                        }
                    else {
                        for (int i = 0; i < DHTsize; i++) {
                            if (hashkeycode.compareTo(chordDHT.get(i).getnodehash()) <= 0) {
                                msg = msg + chordDHT.get(i).getport();
                                break;
                            }
                        }
                    }
                    //String result= clientTash_msg(msg);
                    new ClientTask_insert().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);

                    return uri;
                }
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return uri;
	}

    public Uri insert_recovery(Uri uri, ContentValues values) {
        String key1 = values.getAsString("key");
        String key=key1.substring(0,key1.lastIndexOf("+"));
        String value = values.getAsString("value");
        /*
        Referenced from:
        https://developer.android.com/training/data-storage/files#java
         */
        FileOutputStream outputStream;

        try {
            outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
            outputStream.write(value.getBytes());
            outputStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        HashSet<String>keys=nodekeylist.get(key1.substring(key1.lastIndexOf("+")+1));
        keys.add(key);
        nodekeylist.put(key1.substring(key1.lastIndexOf("+")+1),keys);

        Log.v("Hashmapinsertion_reco", key1.substring(key1.lastIndexOf("+")+1)+" "+key +" "+ value);

        return uri;
    }

	@Override
	public boolean onCreate() {
	    Log.d(TAG, "onCreate provider");
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        System.out.println("My Port is "+ myPort);


        for(int i =11108;i<11128;i+=4){
            nodekeylist.put(String.valueOf(i),new HashSet<String>());
        }
        for (int i = 0; i < REMOTE_PORT.length; i++) {
            int avd = Integer.parseInt(REMOTE_PORT[i])/2;
            String emulator = Integer.toString(avd);
            System.out.println(REMOTE_PORT[i]);
            String nodehash = null;
            try {
                nodehash = genHash(emulator);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            System.out.println("The avds are " + avd + " " + emulator + " " + REMOTE_PORT[i] + " " + nodehash);
            chordDHT.add(new nodeid(REMOTE_PORT[i], nodehash));
        }
        Collections.sort(chordDHT, new sortbyhash());

        avdno = Integer.parseInt(myPort)/2;
        try {
            myporthash = genHash(Integer.toString(avdno));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

            DHTsize = chordDHT.size();
            nodeid obj = new nodeid (myPort, myporthash);
            currindex = chordDHT.indexOf(obj);
            Log.i("Current index of ", myPort + "  "+currindex);
            if(currindex == 0){
                previndex =DHTsize-1;
            }
            else{
                previndex =currindex-1;
            }
            if(currindex == DHTsize-1){
                succindex =0;
            }
            else{
                succindex =currindex+1;
            }
            if(succindex==DHTsize-1){
                supersuccindex = 0;
            }
            else{
                supersuccindex=succindex+1;
            }
            if(previndex==0){
                superprevindex = DHTsize-1;
            }
            else{
                superprevindex=previndex-1;
            }
            System.out.println("Reached here to set index " + superprevindex + " "+ previndex + " " + currindex  + " " + succindex +" "+ supersuccindex);

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
        }

        for(String files : this.getContext().fileList()){
            Log.v("OnCreatedelete", files);
            this.getContext().deleteFile(files);
        }

        String msg = myPort+"+"+"recovered";
        new ClientTask_recover().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);


        return true;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
	    Log.v("In Query", selection);
        String line = null;
        MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
        File directory = getContext().getFilesDir();
        String hashkeycode = null;

        if(selection.length()>1 && !selection.contains("replicaquery")){
            try {
                hashkeycode = genHash(selection);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }

            if((hashkeycode.compareTo(chordDHT.get(previndex).getnodehash())>0
                    && hashkeycode.compareTo(chordDHT.get(currindex).getnodehash())<=0) ||
                    (currindex==0 && hashkeycode.compareTo(chordDHT.get(currindex).getnodehash())<=0) ||
                    (currindex==0 && hashkeycode.compareTo(chordDHT.get(previndex).getnodehash())>0 )) {
                try {
                    while (!recoveryflag){
                        continue;
                    }
                File file = new File(directory, selection);
                BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
                line = bufferedReader.readLine();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                cursor.addRow(new String[]{selection, line});
                Log.v("query", selection);
                Log.v("value", line);
                return  cursor;
            }
            else{

                String msg = selection + "_"+ "querynode";
                String result = null;
                if(hashkeycode.compareTo(chordDHT.get(DHTsize-1).getnodehash())>0 ||
                        hashkeycode.compareTo(chordDHT.get(0).getnodehash())<=0){
                    msg = msg + chordDHT.get(0).getport();
                }
                else{
                    for (int i=0;i<DHTsize;i++){
                        if(hashkeycode.compareTo(chordDHT.get(i).getnodehash())<=0){
                            msg = msg + chordDHT.get(i).getport();
                            break;
                        }
                    }
                }
                result = clientTash_msg(msg);
                System.out.println("Client return :" +result);

                if(result!=null){
                    String key_value [] = result.split("\\#");
                    for(int i=0;i<key_value.length;i++){
                        String key_str = key_value[i].substring(0,key_value[i].lastIndexOf("_"));
                        String value_str = key_value[i].substring(key_value[i].lastIndexOf("_")+1);
                        cursor.addRow(new String[]{key_str, value_str});
                    }
                    return cursor;
                }

            }
        }
        else if(selection.equals("@")){
            while (!recoveryflag){
                continue;
            }

            File[] fileNames = directory.listFiles();
            try {
            for(File files : fileNames){
                File file = new File(directory, files.getName());
                BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
                line = bufferedReader.readLine();
                cursor.addRow(new String[]{files.getName(), line});
                Log.v("Query@Return", files.getName() +" "+ line);
            }
            } catch (IOException e) {
                e.printStackTrace();
            }
            return cursor;
        }
        else if(selection.equals("*")) {
            while (!recoveryflag){
                continue;
            }
            String msg = "@" + "_" + "globaldump";
            File[] fileNames = directory.listFiles();
            try {

                for (File files : fileNames) {
                    File file = new File(directory, files.getName());
                    BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
                    line = bufferedReader.readLine();
                    cursor.addRow(new String[]{files.getName(), line});
                    Log.v("Query*Return", files.getName() +" "+ line);
                }
            }catch (IOException e) {
                e.printStackTrace();
            }

            String result = clientTash_msg(msg);
            System.out.println("Client return :" + result);
            if (result != null) {
                String key_value[] = result.split("\\#");
                for (int i = 0; i < key_value.length; i++) {
                    String[] arr = key_value[i].split("\\_");
                    if (arr.length > 1) {
                        Log.v("PrintResult", key_value[i] + " " + arr[0]);
                        cursor.addRow(new String[]{arr[0], arr[1]});
                    }
                    Log.v("Query*Return",  arr[0]+" "+ arr[1]);
                }
                return cursor;
            }
        }
        else if(selection.contains("replicaquery")){

            selection= selection.substring(0,selection.lastIndexOf("+"));
            while (!recoveryflag){
                continue;
            }

            try {
                File file = new File(directory, selection);
                BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
                line = bufferedReader.readLine();
            } catch (IOException e) {
                e.printStackTrace();
            }
            cursor.addRow(new String[]{selection, line});
            Log.v("query", selection);
            Log.v("value", line);
            return  cursor;

        }

        return null;
	}

    public String query_recover(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {
        String line = "";
        File directory = getContext().getFilesDir();

        try{
            HashSet<String> recovery_port_keys = nodekeylist.get(selection.substring(0,selection.lastIndexOf("+")));

            Log.v("query_recover",selection);
            if(recoveryflag){
                String result = "";
                String res_key_value = null;
                for (String e : recovery_port_keys) {
                    try {
                        Log.v("query_recover","Port is "+ selection.substring(0,selection.lastIndexOf("+")) +"recovery port list "+e);
                        File file = new File(directory, e);
                        BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
                        line = bufferedReader.readLine();
                        res_key_value = e+"_"+ line;
                        result+=res_key_value+"+";
                    } catch (IOException exception) {
                        exception.printStackTrace();
                    }

                }
                Log.v("query_recover","returning: "+result);
                return result;
            }
        } catch (Exception e){
            e.printStackTrace();
        }
        return  "";

    }

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}
    public int getcontextdelete(String selection) {
	    Log.v("Deleting in method",selection);
        this.getContext().deleteFile(selection);
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {

            ServerSocket serverSocket = sockets[0];
            Socket socket = null;

            try {
                while(true) {

                    /*
                     * Receiving msg from Client and passing them to onProgressUpdate
                     * Referenced from https://docs.oracle.com/javase/7/docs/api/java/io/ObjectInputStream.html
                     */
                    socket = serverSocket.accept();
                    socket.setSoTimeout(500);
                    InputStream inputStream = socket.getInputStream();
                    ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
                    String msg = (String) objectInputStream.readObject();


                    if(msg.contains("replica")){
                        String str[]=msg.split("\\_");
                        System.out.println("Server side replica string "+ str[0]+ " "+ str[1]+ " " +str[2]+ " "+ str[3]);
                        String key = str[0];
                        //String value = str[1];

                        ContentValues cv = new ContentValues();
                        cv.put("key", key+"_"+"replica");
                        cv.put("value", str[1]);
                        insert(mUri, cv);
                        HashSet<String>keys=nodekeylist.get(str[3]);
                        keys.add(key);
                        nodekeylist.put(str[3],keys);
                        Log.v("Hashmapinsert_rep", str[3]+" "+key +" "+ str[1]);

                        OutputStream outputStream= socket.getOutputStream();
                        ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
                        String ack_msg= "ACK";
                        objectOutputStream.writeObject(ack_msg);
                        objectOutputStream.flush();
                    }

                    else if(msg.contains("querynode") || msg.contains("globaldump")){
                        String key;
                        //System.out.println("Received query request " + nodekeylist.get(myPort));
                        //System.out.println("Received query request " + nodetimestamp.get(myPort));
                        if(msg.contains("querynode")){
                            key = msg.substring(0,msg.lastIndexOf("_"))+"+"+"replicaquery";
                        }
                        else {
                            key ="@";
                        }

                        Cursor resultCursor = query(mUri, null,
                                key, null, null);
                        /*
                        Referenced from :
                        https://stackoverflow.com/questions/4920528/iterate-through-rows-from-sqlite-query/4920643
                         */
                        String array[] = new String[resultCursor.getCount()];
                        int i = 0;
                        resultCursor.moveToFirst();
                        while (!resultCursor.isAfterLast()) {

                            String res_key = resultCursor.getString(resultCursor.getColumnIndex("key"));
                            String res_value = resultCursor.getString(resultCursor.getColumnIndex("value"));
                            String res_key_value = res_key + "_"+ res_value;
                            array[i] = res_key_value;
                            i++;
                            resultCursor.moveToNext();
                        }
                        /*
                        https://stackoverflow.com/questions/33802971/alternative-for-string-join-in-android
                         */
                        String result = TextUtils.join("#", array);
                        Log.v("Array",result);
                        OutputStream outputStream= socket.getOutputStream();
                        ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
                        objectOutputStream.writeObject(result);
                        objectOutputStream.flush();
                    }
                    else if(msg.contains("globaldelete")){
                        String selection = msg.substring(0,msg.lastIndexOf("_"));
                        int res  = delete(mUri, selection, null);

                        OutputStream outputStream= socket.getOutputStream();
                        ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
                        objectOutputStream.writeObject(Integer.toString(res));
                        objectOutputStream.flush();
                    }
                    else if(msg.contains("recovered")) {
                        String resultCursor = query_recover(mUri, null, msg, null, null);
                        Log.v("Recovery Array"+msg,resultCursor);
                        OutputStream outputStream= socket.getOutputStream();
                        ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
                        objectOutputStream.writeObject(resultCursor);
                        objectOutputStream.flush();
                    }
                    else if(msg.contains("singlekeydelete")){
                        String selection = msg.substring(0,msg.lastIndexOf("_"));
                        getcontextdelete(selection);



                        OutputStream outputStream= socket.getOutputStream();
                        ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
                        objectOutputStream.writeObject(Integer.toString(0));
                        objectOutputStream.flush();
                    }

                }
            }catch (SocketTimeoutException e){
                e.printStackTrace();
            }
            catch (Exception e) {
                e.printStackTrace();
            }
            finally {
                try {
                    socket.close();
                }
                catch (IOException e)
                {
                    System.out.println(e);
                }
            }
            return null;
        }

    }

    public String clientTash_msg(String msg){
        String msgToSend = msg;
        String msg_ack = "";
        String msg_ack1 = "";
        int res =0;

         if(msgToSend.contains("querynode")) {
             Socket socket = null;
             String controllerport = msg.substring(msg.length() - 5);
             int index = 0;
             int index1 = 0;
             int index2 = 0;
             for (nodeid temp : chordDHT) {
                 if (controllerport.equals(temp.getport())) {
                     index = chordDHT.indexOf(temp);
                     break;
                 }
             }
             if (index == chordDHT.size() - 1) {
                 index1 = 0;
                 index2 = 1;
             } else if (index == chordDHT.size() - 2) {
                 index1 = index + 1;
                 index2 = 0;
             } else {
                 index1 = index + 1;
                 index2 = index + 2;
             }
             int i = 0;
             while (true) {
                     if (i == 0) {
                         i++;
                         try{
                             /*
                              * Sending message to server
                              * Referenced from https://docs.oracle.com/javase/7/docs/api/java/io/ObjectOutputStream.html
                              */
                             socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgToSend.substring(msgToSend.length() - 5)));
                             socket.setSoTimeout(1000);

                             OutputStream outputStream = socket.getOutputStream();
                             ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
                             objectOutputStream.writeObject(msgToSend);
                             objectOutputStream.flush();

                             /*
                              * Checking if acknowledgement message is received from  server
                              * so that socket can be closed
                              * Referenced from https://stackoverflow.com/questions/47903053/how-can-i-get-acknowledgment-at-client-side-without-closing-server-socket
                              */
                             InputStream inputStream = socket.getInputStream();
                             ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
                             msg_ack = (String) objectInputStream.readObject();

                             if (msg_ack != null || !msg_ack.equals("") || !msg_ack.equals("null")) {
                                 break;
                             }
                             else{
                                 Log.i("Client msg query", msg_ack+" "+msgToSend);
                             }
                         }catch (SocketTimeoutException exception) {
                             exception.printStackTrace();
                         }catch (UnknownHostException exception){
                             exception.printStackTrace();
                         }catch (StreamCorruptedException exception){
                             exception.printStackTrace();
                         }catch (SocketException exception){
                             exception.printStackTrace();
                         }catch (IOException exception){
                             exception.printStackTrace();
                         }catch (NullPointerException exception){
                             exception.printStackTrace();
                         }catch (Exception e) {
                             System.out.print(e);
                         }

                     } else if (i == 1) {
                         i++;
                         try{
                             Log.i("Client msg query", msgToSend);
                             //System.out.println("Messages queried to successor" +chordDHT.get(index1).getport());
                             socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(chordDHT.get(index1).getport()));
                             socket.setSoTimeout(1000);

                             OutputStream outputStream = socket.getOutputStream();
                             ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
                             objectOutputStream.writeObject(msgToSend);
                             objectOutputStream.flush();

                             InputStream inputStream = socket.getInputStream();
                             ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
                             msg_ack = (String) objectInputStream.readObject();
                             if (msg_ack != null || !msg_ack.equals("") || !msg_ack.equals("null")) {
                                 break;
                             }
                         }catch (SocketTimeoutException exception) {
                             exception.printStackTrace();
                         }catch (UnknownHostException exception){
                             exception.printStackTrace();
                         }catch (StreamCorruptedException exception){
                             exception.printStackTrace();
                         }catch (SocketException exception){
                             exception.printStackTrace();
                         }catch (IOException exception){
                             exception.printStackTrace();
                         }catch (NullPointerException exception){
                             exception.printStackTrace();
                         }catch (Exception e) {
                             System.out.print(e);
                         }

                     } else if (i == 2) {
                         i = 0;
                         try{
                             Log.i("Client msg query", msgToSend);
                             //System.out.println("Messages queried to successor" +chordDHT.get(index2).getport());
                             socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(chordDHT.get(index2).getport()));
                             socket.setSoTimeout(1000);

                             OutputStream outputStream = socket.getOutputStream();
                             ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
                             objectOutputStream.writeObject(msgToSend);
                             objectOutputStream.flush();

                             InputStream inputStream = socket.getInputStream();
                             ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
                             msg_ack = (String) objectInputStream.readObject();
                             if (msg_ack != null || !msg_ack.equals("") || !msg_ack.equals("null")) {
                                 break;
                             }
                         }catch (SocketTimeoutException exception) {
                             exception.printStackTrace();
                         }catch (UnknownHostException exception){
                             exception.printStackTrace();
                         }catch (StreamCorruptedException exception){
                             exception.printStackTrace();
                         }catch (SocketException exception){
                             exception.printStackTrace();
                         }catch (IOException exception){
                             exception.printStackTrace();
                         }catch (NullPointerException exception){
                             exception.printStackTrace();
                         }catch (Exception e) {
                             System.out.print(e);
                         }

                     }
             }
             return msg_ack;
         }

        else if(msgToSend.contains("globaldump")){
            for(int i=0;i<chordDHT.size();i++){
                try{
                    if(!chordDHT.get(i).getport().equals(myPort)){
                        System.out.println("Sending msg "+ msgToSend + " " + chordDHT.size() +" " +chordDHT.get(i).getport());
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(chordDHT.get(i).getport()));
                        socket.setSoTimeout(1000);
                        OutputStream outputStream = socket.getOutputStream();
                        ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
                        objectOutputStream.writeObject(msgToSend);
                        objectOutputStream.flush();

                        InputStream inputStream = socket.getInputStream();
                        ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
                        msg_ack1 += (String) objectInputStream.readObject();
                        msg_ack1+="#";
                        socket.close();

                    }
                }catch (SocketTimeoutException exception) {
                    exception.printStackTrace();
                }catch (UnknownHostException exception){
                    exception.printStackTrace();
                }catch (StreamCorruptedException exception){
                    exception.printStackTrace();
                }catch (SocketException exception){
                    exception.printStackTrace();
                }catch (IOException exception){
                    exception.printStackTrace();
                }catch (NullPointerException exception){
                    exception.printStackTrace();
                }catch (Exception e) {
                    System.out.print(e);
                }
            }
            return msg_ack1;
        }
        else if(msgToSend.contains("globaldelete")){
            for(int i=0;i<chordDHT.size();i++){
                try{
                if(!chordDHT.get(i).getport().equals(myPort)){

                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(chordDHT.get(i).getport()));
                    socket.setSoTimeout(1000);
                    OutputStream outputStream = socket.getOutputStream();
                    ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
                    objectOutputStream.writeObject(msgToSend);
                    objectOutputStream.flush();

                    InputStream inputStream = socket.getInputStream();
                    ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
                    msg_ack = (String) objectInputStream.readObject();
                    res = res + Integer.parseInt(msg_ack);
                    socket.close();
                }
                }catch (SocketTimeoutException e){
                    e.printStackTrace();
                }
                catch (Exception e){
                    e.printStackTrace();
                }
            }
            //if(res==0) {
                return "0";
            //}
        }
    return null;
    }


    private class ClientTask_insert extends AsyncTask<String, Void, String> {

        @Override
        protected synchronized String doInBackground(String... msgs) {
            String msgToSend = msgs[0];

            String msg_ack = null;
            String msg_ack1 = "";
            int res = 0;

            if (msgToSend.contains("replica")) {
                if (msgToSend.substring(msgToSend.length() - 5).equals(myPort)) {
                    int i = 0;
                    Socket socket = null;
                    try{
                        System.out.println("Messages forwareded to successor 1" + chordDHT.get(succindex).getport()+" "+msgToSend);
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(chordDHT.get(succindex).getport()));
                        socket.setSoTimeout(1000);

                        OutputStream outputStream = socket.getOutputStream();
                        ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
                        objectOutputStream.writeObject(msgToSend);
                        objectOutputStream.flush();

                        InputStream inputStream = socket.getInputStream();
                        ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
                        msg_ack = (String) objectInputStream.readObject();
                        socket.close();
                    }
                    catch (SocketTimeoutException exception) {
                        exception.printStackTrace();
                    }catch (UnknownHostException exception){
                        exception.printStackTrace();
                    }catch (StreamCorruptedException exception){
                        exception.printStackTrace();
                    }catch (SocketException exception){
                        exception.printStackTrace();
                    }catch (IOException exception){
                        exception.printStackTrace();
                    }catch (NullPointerException exception){
                        exception.printStackTrace();
                    }catch (Exception e) {
                        System.out.print(e);
                    }
                    try{System.out.println("Messages forwareded to successor 2" + chordDHT.get(supersuccindex).getport()+" "+msgToSend);
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(chordDHT.get(supersuccindex).getport()));
                        socket.setSoTimeout(1000);

                        OutputStream outputStream = socket.getOutputStream();
                        ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
                        objectOutputStream.writeObject(msgToSend);
                        objectOutputStream.flush();

                        InputStream inputStream = socket.getInputStream();
                        ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
                        msg_ack = (String) objectInputStream.readObject();
                        socket.close();

                    }catch (SocketTimeoutException exception) {
                        exception.printStackTrace();
                    }catch (UnknownHostException exception){
                        exception.printStackTrace();
                    }catch (StreamCorruptedException exception){
                        exception.printStackTrace();
                    }catch (SocketException exception){
                        exception.printStackTrace();
                    }catch (IOException exception){
                        exception.printStackTrace();
                    }catch (NullPointerException exception){
                        exception.printStackTrace();
                    }catch (Exception e) {
                        System.out.print(e);
                    }
                } else {
                    String controllerport = msgToSend.substring(msgToSend.length() - 5);
                    int index = 0;
                    int index1 = 0;
                    int index2 = 0;
                    for (nodeid temp : chordDHT) {
                        if (controllerport.equals(temp.getport())) {
                            index = chordDHT.indexOf(temp);
                            break;
                        }
                    }
                    if (index == chordDHT.size() - 1) {
                        index1 = 0;
                        index2 = 1;
                    } else if (index == chordDHT.size() - 2) {
                        index1 = index + 1;
                        index2 = 0;
                    } else {
                        index1 = index + 1;
                        index2 = index + 2;
                    }
                    int i = 0;
                    Socket socket = null;
                    try{
                        System.out.println("Messages forwareded to successor 1" + chordDHT.get(index).getport()+" "+msgToSend);
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(chordDHT.get(index).getport()));
                        socket.setSoTimeout(1000);

                        OutputStream outputStream = socket.getOutputStream();
                        ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
                        objectOutputStream.writeObject(msgToSend);
                        objectOutputStream.flush();

                        InputStream inputStream = socket.getInputStream();
                        ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
                        msg_ack = (String) objectInputStream.readObject();
                        socket.close();
                    }catch (SocketTimeoutException exception) {
                        exception.printStackTrace();
                    }catch (UnknownHostException exception){
                        exception.printStackTrace();
                    }catch (StreamCorruptedException exception){
                        exception.printStackTrace();
                    }catch (SocketException exception){
                        exception.printStackTrace();
                    }catch (IOException exception){
                        exception.printStackTrace();
                    }catch (NullPointerException exception){
                        exception.printStackTrace();
                    }catch (Exception e) {
                        System.out.print(e);
                    }
                    try{
                        System.out.println("Messages forwareded to successor 2" + chordDHT.get(index1).getport() +" "+msgToSend);
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(chordDHT.get(index1).getport()));
                        socket.setSoTimeout(1000);

                        OutputStream outputStream = socket.getOutputStream();
                        ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
                        objectOutputStream.writeObject(msgToSend);
                        objectOutputStream.flush();

                        InputStream inputStream = socket.getInputStream();
                        ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
                        msg_ack = (String) objectInputStream.readObject();
                        socket.close();

                    }catch (SocketTimeoutException exception) {
                        exception.printStackTrace();
                    }catch (UnknownHostException exception){
                        exception.printStackTrace();
                    }catch (StreamCorruptedException exception){
                        exception.printStackTrace();
                    }catch (SocketException exception){
                        exception.printStackTrace();
                    }catch (IOException exception){
                        exception.printStackTrace();
                    }catch (NullPointerException exception){
                        exception.printStackTrace();
                    }catch (Exception e) {
                        System.out.print(e);
                    }
                    try{
                        System.out.println("Messages forwareded to successor 3" + chordDHT.get(index2).getport()+" "+msgToSend);
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(chordDHT.get(index2).getport()));
                        socket.setSoTimeout(1000);

                        OutputStream outputStream = socket.getOutputStream();
                        ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
                        objectOutputStream.writeObject(msgToSend);
                        objectOutputStream.flush();

                        InputStream inputStream = socket.getInputStream();
                        ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
                        msg_ack = (String) objectInputStream.readObject();
                        socket.close();

                    }catch (SocketTimeoutException exception) {
                        exception.printStackTrace();
                    }catch (UnknownHostException exception){
                        exception.printStackTrace();
                    }catch (StreamCorruptedException exception){
                        exception.printStackTrace();
                    }catch (SocketException exception){
                        exception.printStackTrace();
                    }catch (IOException exception){
                        exception.printStackTrace();
                    }catch (NullPointerException exception){
                        exception.printStackTrace();
                    }catch (Exception e) {
                        System.out.print(e);
                    }
                }
            }
            return null;
        }
    }

    ////

    private class ClientTask_delete extends AsyncTask<String, Void, String> {

        @Override
        protected synchronized String doInBackground(String... msgs) {
            String msgToSend = msgs[0];

            String msg_ack = null;
            //String msg_ack1 = "";
            int res = 0;

            if(msgToSend.contains("globaldelete")){
                for(int i=0;i<chordDHT.size();i++){
                    try{
                        if(!chordDHT.get(i).getport().equals(myPort)){

                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(chordDHT.get(i).getport()));
                            socket.setSoTimeout(1000);
                            OutputStream outputStream = socket.getOutputStream();
                            ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
                            objectOutputStream.writeObject(msgToSend);
                            objectOutputStream.flush();

                            InputStream inputStream = socket.getInputStream();
                            ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
                            msg_ack = (String) objectInputStream.readObject();
                            res = res + Integer.parseInt(msg_ack);
                            socket.close();
                        }
                    }catch (SocketTimeoutException exception) {
                        exception.printStackTrace();
                    }catch (UnknownHostException exception){
                        exception.printStackTrace();
                    }catch (StreamCorruptedException exception){
                        exception.printStackTrace();
                    }catch (SocketException exception){
                        exception.printStackTrace();
                    }catch (IOException exception){
                        exception.printStackTrace();
                    }catch (NullPointerException exception){
                        exception.printStackTrace();
                    }catch (Exception e) {
                        System.out.print(e);
                    }
                }
                //if(res==0) {
                return "0";
                //}
            }
            else if(msgToSend.contains("singlekeydelete")){
                String controllerport = msgToSend.substring(msgToSend.length() - 5);
                int index = 0;
                int index1 = 0;
                int index2 = 0;
                for (nodeid temp : chordDHT) {
                    if (controllerport.equals(temp.getport())) {
                        index = chordDHT.indexOf(temp);
                        break;
                    }
                }
                if (index == chordDHT.size() - 1) {
                    index1 = 0;
                    index2 = 1;
                } else if (index == chordDHT.size() - 2) {
                    index1 = index + 1;
                    index2 = 0;
                } else {
                    index1 = index + 1;
                    index2 = index + 2;
                }

                if(!chordDHT.get(index).getport().equals(myPort)){
                    try{
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(chordDHT.get(index).getport()));
                        socket.setSoTimeout(1000);
                        OutputStream outputStream = socket.getOutputStream();
                        ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
                        objectOutputStream.writeObject(msgToSend);
                        objectOutputStream.flush();

                        InputStream inputStream = socket.getInputStream();
                        ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
                        msg_ack = (String) objectInputStream.readObject();
                        res = res + Integer.parseInt(msg_ack);
                        socket.close();
                    }
                    catch (SocketTimeoutException exception) {
                        exception.printStackTrace();
                    }catch (UnknownHostException exception){
                        exception.printStackTrace();
                    }catch (StreamCorruptedException exception){
                        exception.printStackTrace();
                    }catch (SocketException exception){
                        exception.printStackTrace();
                    }catch (IOException exception){
                        exception.printStackTrace();
                    }catch (NullPointerException exception){
                        exception.printStackTrace();
                    }catch (Exception e) {
                        System.out.print(e);
                    }
                }
                if(!chordDHT.get(index1).getport().equals(myPort)){
                    try{
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(chordDHT.get(index1).getport()));
                        socket.setSoTimeout(1000);
                        OutputStream outputStream = socket.getOutputStream();
                        ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
                        objectOutputStream.writeObject(msgToSend);
                        objectOutputStream.flush();

                        InputStream inputStream = socket.getInputStream();
                        ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
                        msg_ack = (String) objectInputStream.readObject();
                        res = res + Integer.parseInt(msg_ack);
                        socket.close();
                    }
                    catch (SocketTimeoutException exception) {
                        exception.printStackTrace();
                    }catch (UnknownHostException exception){
                        exception.printStackTrace();
                    }catch (StreamCorruptedException exception){
                        exception.printStackTrace();
                    }catch (SocketException exception){
                        exception.printStackTrace();
                    }catch (IOException exception){
                        exception.printStackTrace();
                    }catch (NullPointerException exception){
                        exception.printStackTrace();
                    }catch (Exception e) {
                        System.out.print(e);
                    }
                }
                if(!chordDHT.get(index2).getport().equals(myPort)){
                    try{
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(chordDHT.get(index2).getport()));
                        socket.setSoTimeout(1000);
                        OutputStream outputStream = socket.getOutputStream();
                        ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
                        objectOutputStream.writeObject(msgToSend);
                        objectOutputStream.flush();

                        InputStream inputStream = socket.getInputStream();
                        ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
                        msg_ack = (String) objectInputStream.readObject();
                        res = res + Integer.parseInt(msg_ack);
                        socket.close();
                    }
                    catch (SocketTimeoutException exception) {
                        exception.printStackTrace();
                    }catch (UnknownHostException exception){
                        exception.printStackTrace();
                    }catch (StreamCorruptedException exception){
                        exception.printStackTrace();
                    }catch (SocketException exception){
                        exception.printStackTrace();
                    }catch (IOException exception){
                        exception.printStackTrace();
                    }catch (NullPointerException exception){
                        exception.printStackTrace();
                    }catch (Exception e) {
                        System.out.print(e);
                    }
                }
                //if(res==0) {
                return "0";
                //}
            }
            return null;
        }
    }

    ////

    private class ClientTask_recover extends AsyncTask<String, Void, Void> {

        @Override
        protected synchronized Void doInBackground(String... msgs) {
            String msgToSend = msgs[0];
            String msg_ack = "";

            int counter =0;
            while(true){
                if(counter==0){
                    counter=1;
                    try{
                        msgToSend = myPort + "+" + "recovered";
                        Log.i("succ  ", myPort + " "+chordDHT.get(succindex).getport() + "Msgbeingsent" +msgToSend);
                        Socket socket3 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(chordDHT.get(succindex).getport()));
                        socket3.setSoTimeout(1000);

                        OutputStream outputStream3 = socket3.getOutputStream();
                        ObjectOutputStream objectOutputStream3 = new ObjectOutputStream(outputStream3);
                        objectOutputStream3.writeObject(msgToSend);
                        objectOutputStream3.flush();

                        InputStream inputStream3 = socket3.getInputStream();
                        ObjectInputStream objectInputStream3 = new ObjectInputStream(inputStream3);
                        msg_ack = (String) objectInputStream3.readObject();
                        socket3.close();
                        Log.v("ClientTask_recover",msg_ack);

                        Log.v("msg ack received"+counter,msg_ack);
                        if(msg_ack != null || !msg_ack.equals("") || !msg_ack.equals("null")){
                            Log.v("msg ack received",msg_ack);
                            String[] key_value = msg_ack.split("\\+");
                            Log.v("key_vale length",String.valueOf(key_value.length));
                            if(key_value.length>0){
                                for(int i=0;i<key_value.length;i++){
                                    if(key_value[i].contains("_")){
                                        String key = key_value[i].substring(0,key_value[i].lastIndexOf("_"));
                                        String value = key_value[i].substring(key_value[i].lastIndexOf("_")+1);
                                        ContentValues cv = new ContentValues();
                                        cv.put("key", key+"+"+myPort);
                                        cv.put("value", value);
                                        insert_recovery(mUri, cv);
                                        Log.i("Sent insertionown", key+"+"+myPort +" "+value);
                                    }
                                }
                            }
                        }
                        break;
                    }catch (SocketTimeoutException exception) {
                        exception.printStackTrace();
                    }catch (UnknownHostException exception){
                        exception.printStackTrace();
                    }catch (StreamCorruptedException exception){
                        exception.printStackTrace();
                    }catch (SocketException exception){
                        exception.printStackTrace();
                    }catch (IOException exception){
                        exception.printStackTrace();
                    }catch (NullPointerException exception){
                        exception.printStackTrace();
                    }catch (Exception e) {
                        System.out.print(e);
                    }
                }
                else if(counter ==1){
                    counter=0;

                    try{
                        msgToSend = myPort + "+" + "recovered";
                        Log.i("succ  ", myPort + " "+chordDHT.get(supersuccindex).getport() + "Msgbeingsent" +msgToSend);
                        Socket socket3 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(chordDHT.get(supersuccindex).getport()));
                        socket3.setSoTimeout(1000);

                        OutputStream outputStream3 = socket3.getOutputStream();
                        ObjectOutputStream objectOutputStream3 = new ObjectOutputStream(outputStream3);
                        objectOutputStream3.writeObject(msgToSend);
                        objectOutputStream3.flush();

                        InputStream inputStream3 = socket3.getInputStream();
                        ObjectInputStream objectInputStream3 = new ObjectInputStream(inputStream3);
                        msg_ack = (String) objectInputStream3.readObject();
                        socket3.close();
                        Log.v("ClientTask_recover",msg_ack);

                        if(msg_ack != null || !msg_ack.equals("") || !msg_ack.equals("null")) {
                            Log.v("msg ack received", msg_ack);
                            String[] key_value = msg_ack.split("\\+");
                            Log.v(" key_vale length", String.valueOf(key_value.length));
                            if (key_value.length > 0) {
                                for (int i = 0; i < key_value.length; i++) {
                                    if(key_value[i].contains("_")){
                                        String key = key_value[i].substring(0, key_value[i].lastIndexOf("_"));
                                        String value = key_value[i].substring(key_value[i].lastIndexOf("_") + 1);
                                        ContentValues cv = new ContentValues();
                                        cv.put("key", key + "+" + myPort);
                                        cv.put("value", value);
                                        insert_recovery(mUri, cv);
                                        Log.i("Sent insertionown2", key + "+" + myPort + " " + value);
                                    }
                                }
                            }
                        }
                        break;
                    }catch (SocketTimeoutException exception) {
                        exception.printStackTrace();
                    }catch (UnknownHostException exception){
                        exception.printStackTrace();
                    }catch (StreamCorruptedException exception){
                        exception.printStackTrace();
                    }catch (SocketException exception){
                        exception.printStackTrace();
                    }catch (IOException exception){
                        exception.printStackTrace();
                    }catch (NullPointerException exception){
                        exception.printStackTrace();
                    }catch (Exception e) {
                        System.out.print(e);
                    }
                }
            }

            for(int i=0;i<2;i++) {
                try {
                    String msg = null;
                    if (i == 0) {
                        Log.v("ClientTask_recover",chordDHT.get(previndex).getport());
                        String port1=chordDHT.get(previndex).getport();
                        msg = chordDHT.get(previndex).getport() + "+" + "recovered";
                        Log.i("Prev Node replica ", myPort + " " +chordDHT.get(previndex).getport() + "Msgbeingsent" +msg);
                        Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(chordDHT.get(previndex).getport()));
                        socket1.setSoTimeout(1000);

                        OutputStream outputStream1 = socket1.getOutputStream();
                        ObjectOutputStream objectOutputStream1 = new ObjectOutputStream(outputStream1);
                        objectOutputStream1.writeObject(msg);
                        objectOutputStream1.flush();

                        InputStream inputStream1 = socket1.getInputStream();
                        ObjectInputStream objectInputStream1 = new ObjectInputStream(inputStream1);
                        msg_ack = (String) objectInputStream1.readObject();
                        socket1.close();

                        Log.v("msg ack received"+i,msg_ack);
                        if(msg_ack != null || !msg_ack.equals("") || !msg_ack.equals("null")){
                            Log.v("msg ack received"+i,msg_ack);
                            String[] key_value1 = msg_ack.split("\\+");

                            if(key_value1.length>0){
                                for(int j=0;j<key_value1.length;j++){
                                    if(key_value1[j].contains("_")){
                                        String key = key_value1[j].substring(0,key_value1[j].lastIndexOf("_"));
                                        String value = key_value1[j].substring(key_value1[j].lastIndexOf("_")+1);
                                        ContentValues cv = new ContentValues();
                                        cv.put("key", key+"+"+port1);
                                        cv.put("value", value);
                                        insert_recovery(mUri, cv);
                                        Log.i("Sent insertion", key+"+"+port1 +" "+value);
                                    }
                                }
                            }
                        }
                    }
                    else if(i==1){
                        String port2=chordDHT.get(superprevindex).getport();
                        msg = chordDHT.get(superprevindex).getport() + "+" + "recovered";
                        Log.i("Super Prev Node eplica", myPort + " " +chordDHT.get(superprevindex).getport() + "Msgbeingsent" +msg);
                        Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(chordDHT.get(superprevindex).getport()));
                        socket2.setSoTimeout(1000);

                        OutputStream outputStream2 = socket2.getOutputStream();
                        ObjectOutputStream objectOutputStream2 = new ObjectOutputStream(outputStream2);
                        objectOutputStream2.writeObject(msg);
                        objectOutputStream2.flush();

                        InputStream inputStream2 = socket2.getInputStream();
                        ObjectInputStream objectInputStream2 = new ObjectInputStream(inputStream2);
                        msg_ack = (String) objectInputStream2.readObject();
                        socket2.close();

                        Log.v("msg ack received"+i,msg_ack);
                        if(msg_ack != null|| !msg_ack.equals("") || !msg_ack.equals("null")){
                            Log.v("msg ack received"+i,msg_ack);
                            String[] key_value1 = msg_ack.split("\\+");

                            if(key_value1.length>0){
                                for(int j=0;j<key_value1.length;j++){
                                    if(key_value1[j].contains("_")){
                                        String key = key_value1[j].substring(0,key_value1[j].lastIndexOf("_"));
                                        String value = key_value1[j].substring(key_value1[j].lastIndexOf("_")+1);
                                        ContentValues cv = new ContentValues();
                                        cv.put("key", key+"+"+port2);
                                        cv.put("value", value);
                                        insert_recovery(mUri, cv);
                                        Log.i("Sent insertion", key+"+"+port2 +" "+value);
                                    }
                                }
                            }
                        }
                    }

                } catch (SocketTimeoutException exception) {
                    Log.v("ClientTask_recover","SocketTimeoutException");
                    exception.printStackTrace();
                } catch (OptionalDataException e) {
                    e.printStackTrace();
                } catch (SocketException e) {
                    e.printStackTrace();
                } catch (UnknownHostException e) {
                    e.printStackTrace();
                } catch (StreamCorruptedException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                } catch (Exception e){
                    e.printStackTrace();
                }
            }

            recoveryflag=true;
            return null;
        }
        }
    }


