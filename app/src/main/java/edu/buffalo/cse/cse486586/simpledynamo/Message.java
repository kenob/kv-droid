package edu.buffalo.cse.cse486586.simpledynamo;

import android.util.Log;

import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by keno on 4/21/15.
 */
public class Message {
    MType mType;
    String message;
    String sender;
    String sendId;
    private BlockingQueue<Message> replies;
    private Timer timer;
    private static final int BASE_TIMEOUT = 1600;
    private static final int INTERNAL_TIMEOUT = 500;
    static final String TAG = "DYMessage";
    String destination;
    int timesQueried = 0;
    Timer internalTimer;


    public Message(MType mt, String sender, String msg, String id){
        this.mType = mt;
        this.message = msg;
        this.sender = sender;
        replies = new LinkedBlockingQueue<>(10);
        this.sendId = id;
    }

    public Message getReply(){
        try {
            if (timesQueried > 1) {
                //Reduce timeout if this message has been queried before
                internalTimer = new Timer();
                internalTimer.schedule(new Poisoner(this), INTERNAL_TIMEOUT);
            }
            timesQueried += 1;
            return replies.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void startTimer(String destination){
        timer = new Timer();
        this.destination = destination;
        int timeout = BASE_TIMEOUT;
        if (this.mType.equals(MType.INSERT) ||
            this.mType.equals(MType.DELETE) ||
            this.mType.equals(MType.QUERY) ||
            this.mType.equals(MType.DELETEALL))
        {

            //group activities might double response time
            timeout = 3200;
        }
        timer.schedule(new Poisoner(this), timeout);
    }

    public Message makeCopy(String id){
        return new Message(this.mType, this.sender, this.message, id);
    }

    public void setReply(Message m){
        timer.cancel();
        try {
            replies.put(m);
//            ServerTask.getServerTask().deleteMessage(m.sendId);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Log.d(TAG, "Filled reply for " + toString() + " to " +destination);
    }

    public void poisonReplyQueue(){
        try {
            Message reply = new Message(MType.VOID, "","","");
            replies.put(reply);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Log.d(TAG, "Timeout exceeded, poisoned queue for message " + toString()+ " to " +destination);
    }

    public boolean isValid(){
        return !this.getmType().equals(MType.VOID);
    }

    public Message(MType mt, String sender, List<String> msgs, String id){
        this.mType = mt;
        StringBuilder sb = new StringBuilder("");
        for (String st : msgs){
            sb.append(st);
            sb.append(",");
        }
        this.message = sb.toString();
        this.sender = sender;
        this.sendId = id;
    }

    public Message(String fullMessage){
        String[] fM = fullMessage.split("\\|");
        this.sendId = fM[0];
        this.sender = fM[1];
        this.mType = MType.valueOf(fM[2]);
        this.message = "";
        if (fM.length > 3){
            this.message = fM[3];
        }
    }

    public boolean inSDClass(){
       return  mType.equals(MType.QUERY) || mType.equals(MType.INSERT) || mType.equals(MType.DELETE);
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public MType getmType() {
        return mType;
    }

    public void setmType(MType mType) {
        this.mType = mType;
    }
    public String getSender() {
        return sender;
    }

    public String toString(){
        return this.sendId + " - " +this.sender+ ": " + this.mType  + "  " + this.message;
    }
    public String sendFormat(){
        return this.sendId + "|" + this.sender +"|"+ this.mType  +"|"+ this.message;
    }

    public void setSender(String sender) {
        this.sender = sender;
    }

    private class Poisoner extends TimerTask {
        Message message;
        public Poisoner(Message m){
            this.message = m;
        }

        @Override
        public void run() {
           message.poisonReplyQueue();
        }
    }

}
