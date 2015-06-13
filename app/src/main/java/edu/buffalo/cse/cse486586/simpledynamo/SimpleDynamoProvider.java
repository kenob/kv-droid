package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.Executor;

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
    static final String TAG = "DYProvider";
    public static String myPort;
    private Router router;
    public static final int READ_QUORUM = 2;
    public static final int WRITE_QUORUM = 2;
    static final Executor SER = AsyncTask.SERIAL_EXECUTOR;
    public static final String[] allNodes = new String[]{"5554","5556","5558","5560","5562"};
    public static final String WILDCARD = "\\^";
    private static final String[] KV_FIELDS = new String[]{"key","value"};
    private static HashMap<String, Integer> vectorClock = new HashMap<>(5);
    private static final String DELETED_FLAG = "D";
    private int messageId;


    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        if (!checkUri(uri)){
            return 0;
        }
        if (selection.equals("\"*\"")){
            //TODO: delete all globally
            int count = deleteAllLocal();
            //Tell all known nodes to delete all keys
            for (String port : allNodes) {
                if(!port.equals(myPort)) {
                    Message msg = new Message(MType.DELETEALL, myPort,"",getMessageId());
                    new ClientTask(msg, port, true).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
                    Message reply = msg.getReply();
                    if (reply.isValid()) {
                        count += Integer.parseInt(reply.getMessage());
                    }
                }
            }
            Log.d(TAG, count + " entries deleted! ");
            return 0;
        }
        if (selection.equals("\"@\"")){
            //delete all locally
            deleteAllLocal();
            return 0;
        }
        //delete locally or pass request on, depending on the result of hashing
        String coord = router.getCoordinator(selection,false);
        if (coord.equals(myPort)){
            deleteMaster(selection);
            return 0;
        }

        Message msg = new Message(MType.DELETE, myPort, selection, getMessageId());
        Message reply = sendAndDetect(msg, coord);

        if (reply.getmType().equals(MType.LOCAL)){
            deleteMaster(selection);
            return 0;
        }
        Log.d(TAG, reply.getMessage() + " deleted at remote location " + reply.getSender());
        return 0;
    }

    public boolean deleteNodeLocal(String selection) {
        return getContext().deleteFile(selection);
    }

    public int deleteAllLocal(){
        String[] files = getContext().fileList();
        for (String file : files){
            deleteNodeLocal(file);
        }
        return files.length;
    }

    public void deleteMaster(String key){
        //treat deletions as writes
        String value = insertMaster(key,"blah",true);
        for (String node : router.getSuccessors(myPort)){
            Message msg = new Message(MType.INSERT_REPLICA,myPort,getKVStr(key,value),getMessageId());
            new ClientTask(msg, node, false).executeOnExecutor(SER);
        }
    }

    private Message sendAndDetect(Message msgTemplate, String coord){
        Message reply;
        Queue<Message> retryQueue = new LinkedList<>();
        ArrayList<String> successors = router.getSuccessors(coord);
        int count = 0;
        String currentTarget;
        // If there is a failure, resend the request to the successors
        while (count < successors.size()) {
            currentTarget = coord;
            if (count > 0){
                currentTarget = successors.get(count - 1);
            }
            if (currentTarget.equals(myPort)){
                Log.d(TAG, "THIS ONE IS MINE");
            }
            Message msg = msgTemplate.makeCopy(getMessageId());
            new ClientTask(msg, currentTarget, true).executeOnExecutor(SER);
            reply = msg.getReply();
            if (!reply.getmType().equals(MType.VOID)){
                Log.d(TAG, "Returning " + reply + " to message " + msg);
                return reply;
            }
            retryQueue.add(msg);
            count += 1;
            Log.d(TAG, "THAT ONE GOT AWAY, RETRYING ...");
        }

        /*Check if some reply has arrived for one of the old message. (Un)surprisingly, this actually,
        this actually happens
         */
        while (!retryQueue.isEmpty()){
            Message msg = retryQueue.poll();
            reply = msg.getReply();
            if (!reply.getmType().equals(MType.VOID)){
                return reply;
            }
        }

        Log.d(TAG, "ALL HOPE IS LOST - MAYDAY");
        return null;
    }


	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO insert locally or pass request on, depending on the result of hashing
        if (!checkUri(uri)){
            return null;
        }
        String key = values.getAsString("key");
        String value = values.getAsString("value");
        String timestampedValue = null;
        String coord = router.getCoordinator(key,false);
        if (coord.equals(myPort)){
            try {
                timestampedValue = insertKeyLocal(key, value);
            } catch (QuorumException e) {
                e.printStackTrace();
            }
            return uri;
        }
        Log.d(TAG, "Passing key " + key + " on to " + coord);
        Message msg = new Message(MType.INSERT, myPort, key +":"+value, getMessageId());
        Message reply = sendAndDetect(msg, coord);
        if (reply.getmType().equals(MType.LOCAL)){
            try {
                timestampedValue = insertKeyLocal(key, value);
                msg.setMessage(getKVStr(key, timestampedValue));
            } catch (QuorumException e) {
                e.printStackTrace();
            }
        }
        else {
            msg.setMessage(reply.getMessage());
        }
        //Finally, resend a best-effort message to everyone, in case anyone missed
        ArrayList<String> successors = router.getGroup(key);
        msg.setmType(MType.BACKUP_INSERT);
        for (String node : successors){
            new ClientTask(msg, node, false).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
        }
        Log.d(TAG, reply.getMessage() + " inserted at remote location " + reply.getSender());
        return uri;
    }

    public String insertKeyLocal(String key, String value) throws QuorumException {
        //Only master node timestamps
        String timeStampedValue = insertMaster(key, value, false);
        int quorumCount = 1;
        Log.d(TAG, "successors are " + router.getSuccessors(myPort));
        Log.d(TAG, "Inserting keys...");
        Deque<Message> tempQueue = new LinkedList<>();
        ArrayList<String> successors = Router.getInstance().getSuccessors(SimpleDynamoProvider.myPort);

        for (String node : successors) {
            Message msg = new Message(MType.INSERT_REPLICA,myPort,getKVStr(key, timeStampedValue),getMessageId());
            tempQueue.add(msg);
            new ClientTask(msg, node, true).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
        }
        Deque<Message> backupQueue = new LinkedList<>();
        int waitRounds = 0;

        while (quorumCount < WRITE_QUORUM) {
            Log.d(TAG, "Current queue is " + tempQueue);
            if (tempQueue.isEmpty() && waitRounds <=1){
                Log.d(TAG, "Swapping queues");
                tempQueue = backupQueue;
                backupQueue = new LinkedList<>();
                waitRounds += 1;
            }
            Message msg = tempQueue.poll();
            if(msg != null) {
                Message reply = msg.getReply();
                if (reply.getmType().equals(MType.VOID)) {
                    //Just in case this isn't a failure
                    backupQueue.add(msg);
                }
                else if (reply.getmType().equals(MType.REJECT)) {
                    //do nothing
                }
                else{
                    quorumCount += 1;
                    Log.d(TAG, "One replica replied, quorum count now " + quorumCount);
                    if (quorumCount >= WRITE_QUORUM) {
                        return timeStampedValue;
                    }
                }
            }
            else {
                Log.d(TAG, "ALL HOPE IS LOST. MAYDAY :'(");
                break;
            }
        }
        return timeStampedValue;
    }

    public String insertMaster(String key, String value, boolean delete) {
        /* For keys without timestamps
           Return value format: <coordinator>##<timestamp>##<value>##e:
           Ensures that write is entirely completed for it to be
           reported as complete
        */
        String deletedFlag = "E";
        if (delete){
            deletedFlag = DELETED_FLAG;
        }

        DataOutputStream outputStream;
        String coord = router.getCoordinator(key, false);
        value = coord + "##" + getVectorClock(coord,0, true) + "##" + value + "##" + deletedFlag;
        try {
            outputStream = new DataOutputStream(getContext().openFileOutput(key, Context.MODE_PRIVATE));
            outputStream.writeUTF(value);
            outputStream.close();
            return value;
        }
        catch (IOException e){
            Log.e(TAG, "File write failed");
        }
        return value;
    }

    public boolean insertSlave(String key, String tStampedValue) {
        /* For keys with timestamps*/
        DataOutputStream outputStream;
        String coord = router.getCoordinator(key, false);
        try {
            String[] valTS = tStampedValue.split("##");
            int timestamp = Integer.parseInt(valTS[1]);
            String currentValue = getFileContents(key, true, true);
            if (currentValue != null) {
                // Don't overwrite a more recent or already executed insertion
                String[] currentValArray = currentValue.split("##");
                int currentInsertTime = Integer.parseInt(currentValArray[1]);
                if (timestamp <= currentInsertTime) {
                    return false;
                }
            }
            getVectorClock(coord, timestamp, false);
            outputStream = new DataOutputStream(getContext().openFileOutput(key, Context.MODE_PRIVATE));
            outputStream.writeUTF(tStampedValue);
            outputStream.close();
//            Log.d(TAG, "inserted slave "+ key + ":" + tStampedValue);
            return true;
        }
        catch (IOException e){
            Log.e(TAG, "File write failed");
        }
        return true;
    }

    @Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
        Log.d(TAG, "Provider instance created!");
        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        myPort = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);

        router = Router.getInstance();
        router.initMyPort(myPort);


        router.initNodes(allNodes);
        HashSet<String> neighborSet = router.getNeighbors(myPort);
        Log.d(TAG, " My neigbor set - " + neighborSet);
        initVectorClock();
        messageId = 0;
        //start server
        try{
            ServerSocket serverSocket = new ServerSocket(10000);
            new ServerTask(this, myPort).executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            e.printStackTrace();
        }

        //Send wake messages and collect missed keys
        //Send phase
        Queue<Message> tempQueue = new LinkedList<>();
        for (String node : neighborSet){
            Message msg = new Message(MType.WAKE, myPort,myPort,getMessageId());
            new ClientTask(msg, node, true).executeOnExecutor(SER);
            tempQueue.add(msg);
        }
        //receive phase
        int count = 0;
        while (!tempQueue.isEmpty()){
            Message sent = tempQueue.remove();
            Message msg = sent.getReply();
            if (msg.isValid()){
                String[] contents = msg.getMessage().split(",");
                if (contents.length > 0) {
                    for (String kv : contents) {
                        if (!kv.trim().equals("")) {
                            String[] kvp = kv.split(":");
                            if (kvp.length > 1) {
                                insertSlave(kvp[0], kvp[1]);
                                count += 1;
                            }
                        }
                    }
                }
            }
        }

        Log.d(TAG, count + " messages recovered");
        Log.d(TAG, "All initialized!");
		return true;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
        if (!checkUri(uri)){
            return null;
        }
        Log.d(TAG, "Local query for " + selection);
        if (selection.contains("^")){
            Log.d(TAG, "This is a wildcard query");
            return wildCardQueryGlobal(selection);
        }
        if (selection.equals("\"*\"")){
            //TODO: query all globally
            MatrixCursor mat = queryAllLocal();
            TreeSet<String> results = new TreeSet<>();

            for (String node : allNodes){
                if (!node.equals(myPort)) {
                    Message msg = new Message(MType.QUERYALL, myPort, "", getMessageId());
                    Message reply = null;
                    new ClientTask(msg, node, true).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
                    reply = msg.getReply();
                    if (reply.isValid()) {
                        String[] rs = reply.getMessage().split(",");
                        for (String kv : rs) {
                            if (!kv.equals("")) {
                                results.add(kv.trim());
                            }
                        }
                    }
                }
            }
            return getCursor(mat, results);
        }
        if (selection.equals("\"@\"")){
            //TODO: query all locally
            return this.queryAllLocal();
        }

        //TODO: query locally or pass request on, depending on the result of hashing

        String coord = router.getCoordinator(selection,false);
        if (coord.equals(myPort)){
            try {
                return queryMaster(selection);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }


        Message msg = new Message(MType.QUERY, myPort, selection, getMessageId());
        Message result = sendAndDetect(msg, coord);

        if (result.getmType().equals(MType.LOCAL)){
            try {
                return queryMaster(selection);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        String[] kvResult = result.getMessage().split(":");
        if (kvResult.length < 2){
            Log.d(TAG, "Result arrived, but no messages found for query " + kvResult[0]);
            return null;
        }
        Log.d(TAG, "Returned " +kvResult[1]+" for " + selection);
        return getCursor(selection, kvResult[1]);
	}

    public MatrixCursor queryMaster(String selection) throws QuorumException {
        Log.d(TAG, "Querying local... ");
        MatrixCursor mat = queryNodeLocal(selection);
        String[] result = getContentsAsArray(selection);
        int quorumCount = 1;
        int currentTS = 0;
        if (result != null){
            currentTS = Integer.parseInt(result[1]);
        }


        Deque<Message> tempQueue = new LinkedList<>();
        Deque<Message> backupQueue = new LinkedList<>();
        int waitRounds = 0;

        ArrayList<String> successors = Router.getInstance().getSuccessors(SimpleDynamoProvider.myPort);

        for (String node : successors) {
            Message msg = new Message(MType.QUERY_REPLICA,myPort,selection,getMessageId());
            tempQueue.add(msg);
            new ClientTask(msg, node, true).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
        }

        while (quorumCount < READ_QUORUM) {
            if (tempQueue.isEmpty() && waitRounds <=1){
                Log.d(TAG, "Swapping queues");
                tempQueue = backupQueue;
                backupQueue = new LinkedList<>();
                waitRounds += 1;
            }
            Log.d(TAG, "Current queue is " + tempQueue);
            Message msg = tempQueue.poll();
            if (msg != null) {
                Message reply = msg.getReply();
                if (reply.getmType().equals(MType.VOID)) {
                    //Just in case this isn't a failure
                    //TODO: Add a max number of re-adds
                    backupQueue.add(msg);
                }
                else if (reply.getmType().equals(MType.REJECT)) {
                    //do nothing
                }
                else {
                    //Conflict resolution
                    String[] kvp = reply.getMessage().split(":");
                    if (kvp.length > 1) {
                        String[] valArray = kvp[1].split("##");
                        quorumCount += 1;
                        Log.d(TAG, "One replica replied, quorum count now " + quorumCount);
                        int thisTS = Integer.parseInt(valArray[1]);
                        if (thisTS > currentTS || result == null) {
                            result = valArray;
                            currentTS = thisTS;
                            Log.d(TAG, "Updating value to the latest");
                        }
                    }
                    if (quorumCount >= READ_QUORUM) {
                        Log.d(TAG, "Closing query session ... ");
                        mat = getCursor(selection, result[2]);
                        return mat;
                    }
                }
            }

            else {
                Log.d(TAG, "ALL HOPE IS LOST. MAYDAY :'(");
                break;
            }
        }
        return mat;
    }


    public MatrixCursor queryNodeLocal(String selection) {
        if (selection.contains("^")){
            Log.d(TAG, "This is a local wildcard query");
            return wildCardQueryLocal(selection);
        }
        try {
            String str = getFileContents(selection, false, false);
            if (str == null){
                return null;
            }
            MatrixCursor mat = new MatrixCursor(KV_FIELDS);
            mat.addRow(new String[]{selection, str});
            return mat;
        }
        catch(Exception e){
            Log.e("DHProvider", "File read failed");
        }

        Log.v("query", selection);
        return null;
    }

    private MatrixCursor queryAllLocal() {
        MatrixCursor mat = new MatrixCursor(KV_FIELDS);
        for (String selection : getContext().fileList()) {
            try {
                String str = getFileContents(selection, false, false);
                if (str != null) {
                    mat.addRow(new String[]{selection, str});
                }
            } catch (Exception e) {
                Log.e("DHProvider", "File read failed");
            }
        }
        return mat;
    }

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    public synchronized String getMessageId(){
        return myPort + "i" + messageId++;
    }

    public String getFileContents(String key, boolean withTimeStamp, boolean includeDeleted) {
        FileInputStream inputStream;
        try {
            inputStream = getContext().openFileInput(key);
            DataInputStream dataInputStream = new DataInputStream(inputStream);
            String fullString = dataInputStream.readUTF();
            String[] val = fullString.split("##");
            if (val.length > 3) {
                if (val[3].equals(DELETED_FLAG) && !includeDeleted){
                    //If key has not been marked as deleted
                    return null;
                }
                if (!withTimeStamp) {
                    return val[2];
                } else {
                    return fullString;
                }
            }
        }  catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public String[] getContentsAsArray(String key) {
        FileInputStream inputStream;
        try {
            inputStream = getContext().openFileInput(key);
            BufferedReader bu = new BufferedReader(new InputStreamReader(inputStream));
            StringBuilder str = new StringBuilder();
            String line;
            while ((line = bu.readLine()) != null) {
                str.append(line);
            }
            inputStream.close();
            String fullString = str.toString();
            String[] val = fullString.split("##");
            if (val.length > 3) {
                return val;
            }
        }  catch (IOException e) {
            e.printStackTrace();
        }
        return new String[]{};
    }


    private MatrixCursor getCursor(String key, String value) {
        MatrixCursor mat = new MatrixCursor(KV_FIELDS);
        mat.addRow(new String[]{key, value});
        return mat;
    }

    private Cursor getCursor(MatrixCursor mat, TreeSet<String> kvs) {
        for (String kv : kvs) {
            if (!kv.equals("")) {
                String[] kvp = kv.split(":");
                if (kvp.length > 1) {
                    mat.addRow(kvp);
                }
            }
        }
        return mat;
    }

    private Cursor getCursor(MatrixCursor mat, HashSet<String> kvs, String wildCard) {
        String[] cards = wildCard.split(WILDCARD);
        for (String kv : kvs) {
            if (!kv.equals("")) {
                String[] kvp = kv.split(":");
                if (kvp.length > 1 && kvp[0].startsWith(cards[0])) {
                    mat.addRow(kvp);
                }
            }
        }
        return mat;
    }

    public String transferToNode(String node)  {
        StringBuilder dataToTransfer = new StringBuilder();
        String[] files = getContext().fileList();
        int count = 0;
        for (String key : files){
            String coord = router.getCoordinator(key,false);
            if (coord.equals(node) || router.getSuccessors(coord).contains(node)){
                String value = getFileContents(key,true,true);
                dataToTransfer.append(key);
                dataToTransfer.append(":");
                dataToTransfer.append(value);
                dataToTransfer.append(",");
                count += 1;
            }
        }

        if (count > 0) {
            return dataToTransfer.toString();
        }
        else {
            return null;
        }
    }

    private boolean checkUri(Uri uri){
        boolean uriFound = uri.toString().contains("edu.buffalo.cse.cse486586.simpledynamo.provider");
        if (!uriFound){
            Log.e(TAG, "Uri " + uri.toString() + " not found!");
        }
        return uriFound;
    }

    public HashSet<String> listKVLocal() {
        HashSet<String> localList = new HashSet<>();
        for (String selection : getContext().fileList()) {
            try {
                String str = getFileContents(selection, false, false);
                if (str!=null) {
                    localList.add(selection + ":" + str);
                }

            } catch (Exception e) {
                Log.e(TAG, "File read failed");
            }
        }
        return localList;
    }

    public MatrixCursor wildCardQueryLocal(String key){
        MatrixCursor mat = new MatrixCursor(KV_FIELDS);
        String[] cards = key.split(WILDCARD);
        for (String selection : getContext().fileList()) {
            try {
                if (selection.startsWith(cards[0])) {
                    String str = getFileContents(selection, false, false);
                    if (str != null) {
                        mat.addRow(new String[]{selection, str});
                    }
                }
            } catch (Exception e) {
                Log.e("DHProvider", "File read failed");
            }
        }
        return mat;
    }

    public Cursor wildCardQueryGlobal(String key){
        MatrixCursor mat = wildCardQueryLocal(key);
        HashSet<String> results = new HashSet<>();

        for (String node : allNodes){
            if (!node.equals(myPort)) {
                Message msg = new Message(MType.QUERYALL, myPort, "", getMessageId());
                Message reply = null;
                new ClientTask(msg, node, true).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
                reply = msg.getReply();
                if (reply.isValid()) {
                    String[] rs = reply.getMessage().split(",");
                    for (String kv : rs) {
                        if (!kv.equals("")) {
                            results.add(kv.trim());
                        }
                    }
                }
            }
        }
        return getCursor(mat, results, key);
    }

    public synchronized int getVectorClock(String node, int value, boolean increment){
        /* Increments and gets a timestamp for a coordinator, or just updates the value
        *  received.
         */
        int currTS = vectorClock.get(node);
        if (increment) {
            currTS = currTS + 1;
        }
        currTS = Math.max(value, currTS);
        vectorClock.put(node, currTS);
        Log.d(TAG, "Vector clock now " + vectorClock);
        return currTS;
    }

    private void initVectorClock(){
        Log.d(TAG, "Initializing Vector clock...");
        for (String node : allNodes){
            vectorClock.put(node, 0);
        }
        //Get all vector timestamps from your repository
        for (String selection : getContext().fileList()) {
            try {
                String str = getFileContents(selection, true, true);
                if (str != null) {
                    String[] valArray = str.split("##");
                    int ts = vectorClock.get(valArray[0]);
                    int currTS = Integer.parseInt(valArray[1]);
                    currTS = Math.max(ts, currTS);
                    vectorClock.put(valArray[0], currTS);
                }
            } catch (Exception e) {
                Log.e(TAG, "File read failed");
            }
        }
        Log.d(TAG, "Vector clock now " + vectorClock);
    }
    public String getKVStr(String key, String value){
        return key+":"+value;
    }
}
