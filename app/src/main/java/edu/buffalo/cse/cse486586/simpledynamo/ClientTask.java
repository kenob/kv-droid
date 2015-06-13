package edu.buffalo.cse.cse486586.simpledynamo;

/**
 * Created by keno on 4/21/15.
 */

import android.os.AsyncTask;
import android.util.Log;

import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;

/**
 * Created by keno on 4/1/15.
 */
public class ClientTask extends AsyncTask<String, Void, Message>{
    private static final String TAG = "DYClient";
    Message msg;
    String target;
    boolean expectReply;

    public ClientTask(Message msg, String target, boolean expectReply){
        this.msg = msg;
        this.target = target;
        this.expectReply = expectReply;

        //To accomodate both 5554 and 11108
        if (target.startsWith("5")){
            this.target = Integer.toString(Integer.parseInt(target) * 2);
        }
        Log.d(TAG, "message " + msg + " to " +this.target+" arrived at sender facility, expectReply? " +expectReply);
    }

    @Override
    protected Message doInBackground(String... strings) {
        Log.d(TAG, "sending to " + target);

        Socket socket = null;
        try {
            DataOutputStream outWriter;
            PrintWriter out;
            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(target));
            outWriter = new DataOutputStream(socket.getOutputStream());
            if (expectReply){
                ServerTask.getServerTask().saveMessage(msg);
                Log.d(TAG, msg.getmType() + " " + msg.getMessage() + " sent to " +target + "; awaiting" +
                        " response");
                msg.startTimer(target);
            }
            outWriter.writeUTF(msg.sendFormat());
            outWriter.close();
            socket.close();
            return null;
        }
        catch (SocketTimeoutException so){
            so.printStackTrace();
            Log.e(TAG, "SocketTimeout Exception");
            return null;
        }
        catch (IOException e) {
            e.printStackTrace();
            Log.e(TAG, "socket IOException");
            return null;
        }
    }

}
