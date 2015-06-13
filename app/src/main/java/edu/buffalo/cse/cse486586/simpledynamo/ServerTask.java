package edu.buffalo.cse.cse486586.simpledynamo;

import android.database.Cursor;
import android.os.AsyncTask;
import android.util.Log;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.Executor;

/**
 * Created by keno on 4/21/15.
 */
public class ServerTask extends AsyncTask<ServerSocket, Cursor, Void>{
    private SimpleDynamoProvider dhtp;
    String myPort;
    static final String TAG = "DYServer";
    private static ServerTask currentServerTask;
    private HashMap<String, Message> messageBank;
    static final Executor SER = AsyncTask.SERIAL_EXECUTOR;
    private HashSet<Integer> forbiddenPorts;
    Router router;
    SimpleDynamoActivity home;

    public synchronized HashMap<String, Message> getMessageBank() {
        return messageBank;
    }

    public ServerTask(SimpleDynamoProvider simpleDhtProvider, String myPort) {
        Log.d(TAG, "Initializing!");
        this.dhtp = simpleDhtProvider;
        this.myPort = myPort;
        currentServerTask = this;
        messageBank = new HashMap<>();
        Log.d(TAG, "All initialized!");
        router = Router.getInstance();
        forbiddenPorts = new HashSet<>();
    }

    public static ServerTask getServerTask(){
        return currentServerTask;
    }
    public SimpleDynamoProvider getDhtp() {
        return dhtp;
    }

    @Override
    protected void onProgressUpdate(Cursor... values) {
        SimpleDynamoActivity.printToScreen(values[0]);
    }

    @Override
    protected Void doInBackground(ServerSocket... sockets) {
        ServerSocket serverSocket = sockets[0];
        DataInputStream inputStream;
        while (true) {
            try {
                Socket clientSocket = serverSocket.accept();
                inputStream = new DataInputStream(clientSocket.getInputStream());
                String msgString = inputStream.readUTF();
                Log.d(TAG, "Received message " + msgString);
                Message message = null;
                if(!msgString.equals("")){
                    //Empty messages happen during crashes, believe me
                    message = new Message(msgString);
                }

                if (message == null){
                   Log.d(TAG, "Null message received");
                }

                else if (getMessageBank().containsKey(message.sendId) &&
                        !(message.getSender().equals(myPort) && message.inSDClass())){
                    Log.d(TAG, "Message exists in bank");
                    getMessageBank().get(message.sendId).setReply(message);
                }

                else if (message.getmType().equals(MType.ACK)) {

                }
                else if (message.getmType().equals(MType.PRINTLOCAL)) {
                    Cursor cur = dhtp.queryNodeLocal(message.getMessage());
                    publishProgress(cur);
                }
                else if (message.getmType().equals(MType.WAKE)) {
                    new CRUDHandler(dhtp,message,MType.WAKE,"").start();
                    Log.d(TAG, "Spawned new crud task for WAKE to " +message.getSender());
                }
                else if (message.getmType().equals(MType.TRANSFER)) {
                    new CRUDHandler(dhtp,message,MType.TRANSFER,"").start();
                    Log.d(TAG, "Spawned new crud task for TRANSFER to " + message.getSender());
                }
                else if (message.getmType().equals(MType.DELETE)) {
                    dhtp.deleteMaster(message.getMessage());
                    String sender = message.getSender();
                    message.setSender(SimpleDynamoProvider.myPort);
                    message.setmType(MType.DELETED);
                    new ClientTask(message,sender,false).executeOnExecutor(SER);
                }
                else if (message.getmType().equals(MType.DELETE_REPLICA)) {
                    dhtp.deleteNodeLocal(message.getMessage());
                    //return local
                }
                else if (message.getmType().equals(MType.DELETEALL)) {
                    int deleted = dhtp.deleteAllLocal();
                    message.setMessage(Integer.toString(deleted));
                    String sender = message.getSender();
                    message.setSender(SimpleDynamoProvider.myPort);
                    new ClientTask(message,sender,false).executeOnExecutor(SER);
                }
                else if (message.getmType().equals(MType.INSERT)) {
                    String[] kvp = message.getMessage().split(":");
                    new CRUDHandler(dhtp,message,MType.INSERT,kvp[0], kvp[1]).start();
                    Log.d(TAG, "Spawned new crud task for " + message.toString());
                }
                else if (message.getmType().equals(MType.INSERT_REPLICA)) {
                    String[] kvp = message.getMessage().split(":");

                    //prevent nodes from saving keys not belonging to them
                    if (router.getKeySuccessors(kvp[0]).contains(myPort) && kvp.length > 1) {
                        dhtp.insertSlave(kvp[0], kvp[1]);
                        message.setmType(MType.ACK);
                    }
                    else {
                        message.setmType(MType.REJECT);
                    }
                    //Return message
                    String sender = message.getSender();
                    message.setSender(SimpleDynamoProvider.myPort);
                    new ClientTask(message,sender,false).executeOnExecutor(SER);
                }
                else if (message.getmType().equals(MType.BACKUP_INSERT)) {
                    // Last resort insertion
                    String[] kvp = message.getMessage().split(":");

                    //prevent nodes from saving keys not belonging to them
                    if (router.getGroup(kvp[0]).contains(myPort) && kvp.length > 1) {
                        dhtp.insertSlave(kvp[0], kvp[1]);
                    }
                }
                else if (message.getmType().equals(MType.QUERY)){
                    new CRUDHandler(dhtp,message,MType.QUERY, message.getMessage()).start();
                    Log.d(TAG, "Spawned new crud task " + message.toString());
                }
                else if (message.getmType().equals(MType.QUERY_REPLICA)) {
                    String sender = message.getSender();
                    message.setSender(SimpleDynamoProvider.myPort);
                    String key = message.getMessage();
                    //Don't bother to search for keys that cannot exist
                    Log.d(TAG, "Successors for this key are " + router.getKeySuccessors(key));
                    if (!router.getKeySuccessors(key).contains(myPort)){
                        message.setmType(MType.REJECT);
                        new ClientTask(message,sender,false).executeOnExecutor(SER);
                    }
                    else {
                        String value = dhtp.getFileContents(key,true,false);
                        /*if the key is found in this node, attach the value to the message,
                        else, return just the key
                         */
                        if (value != null) {
                            message.setMessage(dhtp.getKVStr(key,value));
                        }
                        message.setmType(MType.REPLICA_RESULT);
                        new ClientTask(message, sender, false).executeOnExecutor(SER);
                    }
                } else if (message.getmType().equals(MType.QUERYALL)) {
                    StringBuilder newMessage = new StringBuilder();
                    HashSet<String> myKVs = dhtp.listKVLocal();
                    for (String kv : myKVs) {
                        newMessage.append(kv);
                        newMessage.append(",");
                    }
                    message.setMessage(newMessage.toString());
                    String sender = message.getSender();
                    message.setSender(SimpleDynamoProvider.myPort);
                    new ClientTask(message,sender,false).executeOnExecutor(SER);
                }
            } catch (IOException e) {
                Log.d(TAG, "There was an error while receiving a message");
            }
        }
    }

    public String cursorToString(Cursor cur){
        int valIndex = cur.getColumnIndex("value");
        int keyIndex = cur.getColumnIndex("key");

        cur.moveToFirst();
        return cur.getString(keyIndex)+":"+cur.getString(valIndex);
    }

    public synchronized void saveMessage(Message msg) {
        this.getMessageBank().put(msg.sendId, msg);
    }

    public void deleteMessage(String msgId) {
        this.getMessageBank().remove(msgId);
    }

    public void flushMessageBank(){
        for(Message mess : getMessageBank().values()){
            mess.poisonReplyQueue();
        }
    }
    private synchronized HashSet<Integer> getForbiddenPorts(){
        return forbiddenPorts;
    }
//
//    public void registerSession(Integer port){
//        this.getForbiddenPorts().remove(port);
//    }
//
//    public synchronized void killSession(Integer port){
//        this.getForbiddenPorts().add(port);
//    }
}
