package edu.buffalo.cse.cse486586.simpledynamo;

import android.database.Cursor;
import android.os.AsyncTask;
import android.util.Log;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.Executor;

/**
 * Created by keno on 4/21/15.
 */
public class OldServerTask extends AsyncTask<ServerSocket, String, Void>{
    private SimpleDynamoProvider dhtp;
    String myPort;
    static final String TAG = "DYServer";
    private static OldServerTask currentOldServerTask;
    private HashMap<String, Message> messageBank;
    static final Executor SER = AsyncTask.SERIAL_EXECUTOR;

    public HashMap<String, Message> getMessageBank() {
        return messageBank;
    }

    public OldServerTask(SimpleDynamoProvider simpleDhtProvider, String myPort) {
        Log.d(TAG, "Initializing!");
        this.dhtp = simpleDhtProvider;
        this.myPort = myPort;
        currentOldServerTask = this;
        messageBank = new HashMap<>();
        Log.d(TAG, "All initialized!");
    }

    public static OldServerTask getServerTask(){
        return currentOldServerTask;
    }
    public SimpleDynamoProvider getDhtp() {
        return dhtp;
    }

    @Override
    protected Void doInBackground(ServerSocket... sockets) {
        ServerSocket serverSocket = sockets[0];
        BufferedReader inpBuffer;
        while (true) {
            try {
                inpBuffer = new BufferedReader(new InputStreamReader(serverSocket.accept().getInputStream()));
                StringBuilder msg = new StringBuilder();
                String line;
                while ((line = inpBuffer.readLine()) != null) {
                    msg.append(line);
                }
                Log.d(TAG, "Received message " + msg.toString());
                Message message = new Message(msg.toString());
                Message saved = getMessageBank().get(message.sendId);


                if (saved != null){
                    //message was most likely a reply
                    Log.d(TAG, "removing message " + msg.toString());
                    saved.setReply(message);
                }

                else if (message.getmType().equals(MType.ACK)){

                }

                else if (message.getmType().equals(MType.WAKE)) {

                }

                else if (message.getmType().equals(MType.TRANSFER)) {

                }

                else if (message.getmType().equals(MType.DELETE)) {
                    dhtp.deleteMaster(message.getMessage());
                    message.setmType(MType.ACK);
                    new ClientTask(message, message.getSender(),false).executeOnExecutor(SER);
                }

                else if (message.getmType().equals(MType.DELETE_REPLICA)) {
                    dhtp.deleteNodeLocal(message.getMessage());
                    //return local
                }

                else if (message.getmType().equals(MType.DELETEALL)) {
                    int deleted = dhtp.deleteAllLocal();
                    message.setMessage(Integer.toString(deleted));
                    message.setmType(MType.ACK);
                    new ClientTask(message,message.getSender(),false).executeOnExecutor(SER);
                }

                else if (message.getmType().equals(MType.INSERT)) {
                    String[] kvp = message.getMessage().split(":");
                    dhtp.insertKeyLocal(kvp[0], kvp[1]);
                    message.setmType(MType.ACK);
                    new ClientTask(message, myPort, false).executeOnExecutor(SER);
                }

                else if (message.getmType().equals(MType.INSERT_REPLICA)) {
                    String[] kvp = message.getMessage().split(":");
                    dhtp.insertMaster(kvp[0], kvp[1], true);
                    new ClientTask(message,message.getSender(),false).executeOnExecutor(SER);
                }
                else if (message.getmType().equals(MType.QUERY)) {
                    Cursor cur = dhtp.queryMaster(message.getMessage());
                    if (cur != null) {
                        message.setMessage(cursorToString(cur));
                        new ClientTask(message,
                                message.getSender(),false).executeOnExecutor(SER);
                    }
                    else{
                        //report failure
                    }
                }

                else if (message.getmType().equals(MType.QUERY_REPLICA)) {
                    Cursor cur = dhtp.queryNodeLocal(message.getMessage());
                    if (cur != null) {
                        message.setMessage(cursorToString(cur));
                        new ClientTask(message,
                                message.getSender(),false).executeOnExecutor(SER);
                    }
                    else{
                        //report failure
                    }
                }

                else if (message.getmType().equals(MType.QUERYALL)) {
                    StringBuilder newMessage = new StringBuilder();
                    HashSet<String> myKVs = dhtp.listKVLocal();
                    for (String kv : myKVs){
                        newMessage.append(kv);
                        newMessage.append(",");
                    }
                    message.setMessage(newMessage.toString());
                    new ClientTask(message,
                            message.getSender(),false).executeOnExecutor(SER);
                }


            } catch (IOException e) {
                Log.d(TAG, "There was an error while receiving a message");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public String cursorToString(Cursor cur){
        int valIndex = cur.getColumnIndex("value");
        cur.moveToFirst();
        String value = cur.getString(valIndex);
        return value;
    }

    public void saveMessage(Message msg) {
        this.getMessageBank().put(msg.sendId, msg);
    }
}
