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
    /*
        Wrapper for messages to handle formatting for 
        transmission, parsing, and the reply mechanism
    */
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
        /*
            Constructs a message from a message string
        */
        this.mType = mt;
        this.message = msg;
        this.sender = sender;
        replies = new LinkedBlockingQueue<>(10);
        this.sendId = id;
    }

    public Message(MType mt, String sender, List<String> msgs, String id){
        /*
            Constructs a message from a list of strings
        */
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
        /*
            Constructs a message from a string representation of the 
            full message. Used to parse messages received from other nodes
        */
        String[] fM = fullMessage.split("\\|");
        this.sendId = fM[0];
        this.sender = fM[1];
        this.mType = MType.valueOf(fM[2]);
        this.message = "";
        if (fM.length > 3){
            this.message = fM[3];
        }
    }

    public String getMessage() {
        /*
            getter
        */
        return message;
    }

    public void setMessage(String message) {
        /*
            setter
        */
        this.message = message;
    }

    public MType getmType() {
        /*
            getter
        */
        return mType;
    }

    public void setmType(MType mType) {
        /*
            setter
        */
        this.mType = mType;
    }
    public String getSender() {
        /*
            getter
        */
        return sender;
    }

    public void setSender(String sender) {
        /*
            setter
        */
        this.sender = sender;
    }


    public Message getReply(){
        /*
            Returns the reply for this message. Returns
            a standard failure reply if a reply is not 
            received from the destination within the predefined
            time bounds 
        */
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
        /*
            Starts the timed task which poisons the reply queue (with 
            the standard failure reply) after the specified duration
        */
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
        /*
            Returns a copy of this message
        */
        return new Message(this.mType, this.sender, this.message, id);
    }

    public void setReply(Message m){
        /*
            Pushes the supplied message into the reply queue for this message
            and cancels the timer, so the reply queue is not poisoned
        */
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
        /*
            Pushes the standard failure message to the reply queue
        */
        try {
            Message reply = new Message(MType.VOID, "","","");
            replies.put(reply);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Log.d(TAG, "Timeout exceeded, poisoned queue for message " + toString()+ " to " +destination);
    }

    public boolean isValid(){
        /*
            Returns a flag that indicates whether or not this
            message should be considered as valid
        */
        return !this.getmType().equals(MType.VOID);
    }


    public boolean inSDClass(){
        /*
            Returns a flag that indicates whether a message is in the class of messages which can
            be replies (and hence can be set as the reply for another message). This is necessary
            to avoid non-replies being set as the reply for a waiting message
        */
       return  mType.equals(MType.QUERY) || mType.equals(MType.INSERT) || mType.equals(MType.DELETE);
    }


    public String toString(){
        /*
            Log friendly representation of this message
        */
        return this.sendId + " - " +this.sender+ ": " + this.mType  + "  " + this.message;
    }
    public String sendFormat(){
        /*
            Format for sending messages out
        */
        return this.sendId + "|" + this.sender +"|"+ this.mType  +"|"+ this.message;
    }

    private class Poisoner extends TimerTask {
        /*
            Task to poison the reply queue after some predefined timeout
        */
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
