package edu.buffalo.cse.cse486586.simpledynamo;

import android.os.AsyncTask;
import android.util.Log;

import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;

/**
 * Created by kenob on 4/22/15.
 */
public class CRUDHandler extends Thread {
    /* 
        Performs all activities requiring potentially blocking communication with other nodes. 
        Activity type is inferred from the calleed constructor.
        Constructor format:
        CRUD(dynamoprovider_instance, callback_object, params[])
     */
    private final SimpleDynamoProvider dhtp;
    private Message callbackMsg;
    private static final String TAG = "DYServer";
    private String[] params;
    MType action;


    public CRUDHandler(SimpleDynamoProvider dhtp, Message callbackMsg, MType action, String...params){
        this.dhtp = dhtp;
        this.callbackMsg = callbackMsg;
        this.params = params;
        this.action = action;
    }

    public void run(){
        if (action.equals(MType.INSERT)){
            try {
                insert();
            } catch (QuorumException e) {
                e.printStackTrace();
            }
        }
        else if (action.equals(MType.QUERY)){
            try {
                query();
            } catch (QuorumException e) {
                e.printStackTrace();
            }
        }
        else if (action.equals(MType.WAKE)){
            transfer();
        }
        else if (action.equals(MType.TRANSFER)){
            recover();
        }
    }

    private void insert() throws QuorumException {
        /*
            Inserts a key for which this node acts as a coordinator.
            Performs the insert somewhat asynchronously in the replica servers,
            and only sends a confirmation to the requesting node when 
            a number of nodes equal to the WRITE_QUORUM have successfully saved the key
        */
        String key = params[0];
        String value = params[1];
        String timeStampedValue = dhtp.insertMaster(key, value, false);
        int quorumCount = 1;

        Log.d(TAG, "successors are " +
                Router.getInstance().getSuccessors(SimpleDynamoProvider.myPort));
        Log.d(TAG, "Inserting keys...");

        boolean replied = false;
        ArrayList<String> successors =
                Router.getInstance().getSuccessors(SimpleDynamoProvider.myPort);

        //Send phase
        Deque<Message> tempQueue = new LinkedList<>();


        for (String node : successors) {
            Message msg = new Message(MType.INSERT_REPLICA, SimpleDynamoProvider.myPort,
                    dhtp.getKVStr(key,timeStampedValue),
                    dhtp.getMessageId());
            tempQueue.add(msg);
            new ClientTask(msg, node, true).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
        }

        Deque<Message> backupQueue = new LinkedList<>();
        int waitRounds = 0;

        //Receive phase
        while (quorumCount < SimpleDynamoProvider.WRITE_QUORUM) {
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
                }
                else if (reply.getmType().equals(MType.REJECT)) {
                    //do nothing
                    backupQueue.add(msg);
                }
                else {
                    quorumCount += 1;
                    Log.d(TAG, "One replica replied, quorum count now " + quorumCount);
                    if (quorumCount >= SimpleDynamoProvider.WRITE_QUORUM) {
                        String dest = callbackMsg.getSender();
                        callbackMsg.setmType(MType.INSERTED);
                        callbackMsg.setMessage(dhtp.getKVStr(key, timeStampedValue));
                        callbackMsg.setSender(SimpleDynamoProvider.myPort);
                        new ClientTask(callbackMsg, dest,
                                false).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
                        break;
                    }
                }
            }
            else {
                Log.d(TAG, "ALL HOPE IS LOST. MAYDAY :'(");
                break;
            }
        }
        //finally, resend a best effort message to everyone, in case some node just started
        successors = Router.getInstance().getGroup(key);
        Message msg = new Message(MType.BACKUP_INSERT, SimpleDynamoProvider.myPort,
                dhtp.getKVStr(key,timeStampedValue),
                dhtp.getMessageId());
        for (String node :successors){
            new ClientTask(msg, node, false).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
        }
    }

    private void query() throws QuorumException {
        /*
            Queries a key for which this node acts as a coordinator.
            Queries the replica servers somewhat asynchronously,
            and sends the result with the latest timestamp to the requesting node after 
            a number of nodes equal to the READ_QUORUM have replied
        */
        boolean replied = false;
        Message reply;
        Deque<Message> tempQueue = new LinkedList<>();
        ArrayList<String> successors = Router.getInstance().getSuccessors(SimpleDynamoProvider.myPort);
        String key = callbackMsg.getMessage();
        String dest = callbackMsg.getSender();
        callbackMsg.setmType(MType.QUERY_RESULT);
        String[] result = dhtp.getContentsAsArray(key);
        int quorumCount = 1;
        int currentTS = 0;
        if (result != null){
            currentTS = Integer.parseInt(result[1]);
        }

        for (String node :successors) {
            Message msg = new Message(MType.QUERY_REPLICA, SimpleDynamoProvider.myPort, params[0],
                    dhtp.getMessageId());
            tempQueue.add(msg);
            new ClientTask(msg, node, true).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
        }

        Deque<Message> backupQueue = new LinkedList<>();
        int waitRounds = 0;

        while (quorumCount < SimpleDynamoProvider.READ_QUORUM) {
            Log.d(TAG, "Current queue is " + tempQueue);
            if (tempQueue.isEmpty() && waitRounds <=3){
                Log.d(TAG, "Swapping queues");
                tempQueue = backupQueue;
                backupQueue = new LinkedList<>();
                waitRounds += 1;
            }
            Log.d(TAG, "Current queue is " + tempQueue);

            Message msg = tempQueue.poll();

            if (msg != null) {
                reply = msg.getReply();
                if (reply.getmType().equals(MType.VOID)) {
                    //Just in case this isn't a failure
                    backupQueue.add(msg);
                } else if (reply.getmType().equals(MType.REJECT)) {
                    //do nothing
                } else {
                    quorumCount += 1;
                    String[] kvp = reply.getMessage().split(":");
                    if (kvp.length > 1) {
                        String[] valArray = kvp[1].split("##");
                        Log.d(TAG, "One replica replied, quorum count now " + quorumCount);
                        int thisTS = Integer.parseInt(valArray[1]);
                        if (thisTS > currentTS) {
                            result = valArray;
                            currentTS = thisTS;
                            Log.d(TAG, "Updating value to the latest");
                        }
                    }

                    if (quorumCount >= SimpleDynamoProvider.WRITE_QUORUM) {
                            //Send a reply once R replicas have replied
                            if (result != null) {
                                callbackMsg.setMessage(dhtp.getKVStr(key, result[2]));
                            }
                            new ClientTask(callbackMsg, dest,
                                    false).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
                            break;
                    }
                }

            }
            else {
                Log.d(TAG, "ALL HOPE IS LOST. MAYDAY :'(");
                break;
            }
        }

    }

    private void recover(){
        /*
            Saves all keys sent to this node (typically after a wake message 
            was sent out)
        */
        String[] contents = callbackMsg.getMessage().split(",");
        if (contents.length > 0) {
            for (String kv : contents) {
                if (!kv.equals("")) {
                    String[] kvp = kv.split(":");
                    dhtp.insertSlave(kvp[0], kvp[1]);
                }
            }
        }
    }

    private void transfer(){
        /*
            Transfers all missed keys which should reside in the requesting node to such a node
        */
        String message = ServerTask.getServerTask().getDhtp().transferToNode(callbackMsg.getSender());
        if (message != null) {
            callbackMsg.setMessage(message);
            callbackMsg.setmType(MType.TRANSFER);
            String sender = callbackMsg.getSender();
            callbackMsg.setSender(SimpleDynamoProvider.myPort);
            new ClientTask(callbackMsg, sender, false).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
        }
    }


}
