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
    /*
        Asynchronous task that handles incoming messages from other nodes
    */
    private SimpleDynamoProvider dhtp; // The provider object
    String myPort; 
    static final String TAG = "DYServer";
    private static ServerTask currentServerTask;
    private HashMap<String, Message> messageBank; // Stores locally sent messages
    static final Executor SER = AsyncTask.SERIAL_EXECUTOR;
    // private HashSet<Integer> forbiddenPorts;
    Router router;  // Router instance
    SimpleDynamoActivity home;  // User facing activity

    public synchronized HashMap<String, Message> getMessageBank() {
        /*
            Getter for the local message bank
        */
        return messageBank;
    }

    public ServerTask(SimpleDynamoProvider simpleDhtProvider, String myPort) {
        /*
            Constructor
        */
        Log.d(TAG, "Initializing!");
        this.dhtp = simpleDhtProvider;
        this.myPort = myPort;
        currentServerTask = this;
        messageBank = new HashMap<>();
        Log.d(TAG, "All initialized!");
        router = Router.getInstance();
        // forbiddenPorts = new HashSet<>();
    }

    public static ServerTask getServerTask(){
        /*
            Singleton getter for server task
        */
        return currentServerTask;
    }
    public SimpleDynamoProvider getDhtp() {
        /*
            Getter for the provider object
        */
        return dhtp;
    }

    @Override
    protected void onProgressUpdate(Cursor... values) {
        /*
            Relays results to the user facing activity to be 
            displayed on the screen
        */
        SimpleDynamoActivity.printToScreen(values[0]);
    }

    @Override
    protected Void doInBackground(ServerSocket... sockets) {
        /*
            Listens for connections, and processes received messages
            according to their message types
        */
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
                    // If this is a reply to a sent message (stored in the message bank), set the reply
                    Log.d(TAG, "Message exists in bank");
                    getMessageBank().get(message.sendId).setReply(message);
                }

                else if (message.getmType().equals(MType.ACK)) {
                    // Handle Acks

                }
                else if (message.getmType().equals(MType.PRINTLOCAL)) {
                    // Handle request to print out all the keys and values stored on this node
                    Cursor cur = dhtp.queryNodeLocal(message.getMessage());
                    publishProgress(cur);
                }
                else if (message.getmType().equals(MType.WAKE)) {
                    // Handle wake messages from another node.
                    // Sends the node all messages it may have missed since it was
                    // last seen
                    // Spawns a new task for this to avoid blocking the server's activities
                    new CRUDHandler(dhtp,message,MType.WAKE,"").start();
                    Log.d(TAG, "Spawned new crud task for WAKE to " +message.getSender());
                }
                else if (message.getmType().equals(MType.TRANSFER)) {
                    // Handles a truckload of missed messages sent to this node from
                    // another node. 
                    // Spawns a new task for this to avoid blocking the server's activities
                    new CRUDHandler(dhtp,message,MType.TRANSFER,"").start();
                    Log.d(TAG, "Spawned new crud task for TRANSFER to " + message.getSender());
                }
                else if (message.getmType().equals(MType.DELETE)) {
                    // Handles a request to delete a key for which this this node acts as a coordinator 
                    // and sends back an ack
                    dhtp.deleteMaster(message.getMessage());
                    String sender = message.getSender();
                    message.setSender(SimpleDynamoProvider.myPort);
                    message.setmType(MType.DELETED);
                    new ClientTask(message, sender, false).executeOnExecutor(SER);
                }
                else if (message.getmType().equals(MType.DELETE_REPLICA)) {
                    // Handles a request to delete a key for which this node is a replica
                    // No need for an ack, as we assume eventual consistency to reduce blocking time
                    dhtp.deleteNodeLocal(message.getMessage());
                }
                else if (message.getmType().equals(MType.DELETEALL)) {
                    // Handles a request to delete all keys from this node, and sends back an ack
                    int deleted = dhtp.deleteAllLocal();
                    message.setMessage(Integer.toString(deleted));
                    String sender = message.getSender();
                    message.setSender(SimpleDynamoProvider.myPort);
                    new ClientTask(message, sender, false).executeOnExecutor(SER);
                }
                else if (message.getmType().equals(MType.INSERT)) {
                    // Handles a request to insert a key for which this node acts as a coordinator
                    String[] kvp = message.getMessage().split(":");
                    new CRUDHandler(dhtp,message,MType.INSERT,kvp[0], kvp[1]).start();
                    Log.d(TAG, "Spawned new crud task for " + message.toString());
                }
                else if (message.getmType().equals(MType.INSERT_REPLICA)) {
                    // Handles a request to insert a key for which this node acts as a replica server
                    String[] kvp = message.getMessage().split(":");

                    //prevent this node from saving a key that shouldn't reside here
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

                    //prevent this node from saving a key that shouldn't reside here
                    if (router.getGroup(kvp[0]).contains(myPort) && kvp.length > 1) {
                        dhtp.insertSlave(kvp[0], kvp[1]);
                    }
                }
                else if (message.getmType().equals(MType.QUERY)){
                    // Handles a query for a key for which this node acts as a coordinator.
                    // Spawns a new task, as this requires waiting for other nodes to respond
                    // and that could block the server's core operations
                    new CRUDHandler(dhtp,message,MType.QUERY, message.getMessage()).start();
                    Log.d(TAG, "Spawned new crud task " + message.toString());
                }
                else if (message.getmType().equals(MType.QUERY_REPLICA)) {
                    // Handles a query for which this node acts as a replica server
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
                    // Handles a query for all keys residing on this node
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
        /*
            Returns a string representation of a key-value cursor
        */
        int valIndex = cur.getColumnIndex("value");
        int keyIndex = cur.getColumnIndex("key");

        cur.moveToFirst();
        return cur.getString(keyIndex)+":"+cur.getString(valIndex);
    }

    public synchronized void saveMessage(Message msg) {
        /*
            Saves a message in the sent message bank. We 
            need this to be synchronized so the ids aren't
            screwed up by concurrent calls
        */
        this.getMessageBank().put(msg.sendId, msg);
    }

    public void deleteMessage(String msgId) {
        /*
            Removes a message from the message bank
        */
        this.getMessageBank().remove(msgId);
    }

    public void flushMessageBank(){
        /*
            Poisons all reply queues to release all activities
            waiting for a reply
        */
        for(Message mess : getMessageBank().values()){
            mess.poisonReplyQueue();
        }
    }
//     private synchronized HashSet<Integer> getForbiddenPorts(){
//         return forbiddenPorts;
//     }
//
//    public void registerSession(Integer port){
//        this.getForbiddenPorts().remove(port);
//    }
//
//    public synchronized void killSession(Integer port){
//        this.getForbiddenPorts().add(port);
//    }
}
