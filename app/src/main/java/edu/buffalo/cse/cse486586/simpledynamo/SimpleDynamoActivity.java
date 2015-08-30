package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.app.Activity;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.EditText;
import android.widget.TextView;

public class SimpleDynamoActivity extends Activity {
    Uri mUri;
    public static final String TAG = "DYActivity";
    public static SimpleDynamoActivity currentActivity;
    public static TextView textV;

	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.activity_simple_dynamo);
    
		final TextView tv = (TextView) findViewById(R.id.textView1);
//        tv.setMovementMethod(new ScrollingMovementMethod());

        textV = tv;
        mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");

        //LDUMP
        findViewById(R.id.button1).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                printQueryResults("\"@\"", tv);
            }
        });

        //GDUMP
        findViewById(R.id.button2).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                printQueryResults("\"*\"", tv);
            }
        });

        //LDEL TEST
        findViewById(R.id.button7).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                getContentResolver().delete(mUri, "\"@\"", null);
                tv.setText("");
            }
        });

        //GDEL TEST
        findViewById(R.id.button5).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                getContentResolver().delete(mUri, "\"*\"", null);
                tv.setText("");
            }
        });

        final EditText ed = (EditText) findViewById(R.id.editText1);


        //QUERY LOCAL TEST
        findViewById(R.id.button3).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                String text = ed.getText().toString();
                if (!text.equals("")){
                    Message msg = new Message(MType.PRINTLOCAL,SimpleDynamoProvider.myPort,text,"000");
                    for (String node : SimpleDynamoProvider.allNodes){
                        new ClientTask(msg,node,false).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
                    }
                    ed.setText("");
                }
            }
        });

        //QUERY GLOBAL TEST
        findViewById(R.id.button4).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                String text = ed.getText().toString();
                if (!text.equals("")){
                    printQueryResults(text, tv);
                    ed.setText("");
                }
            }
        });

        //SHOW HASH
        findViewById(R.id.button6).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                tv.setText("");
                tv.append(Router.getInstance().myPort);
            }
        });

        //DELETE TEST
        findViewById(R.id.button9).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                String text = ed.getText().toString();
                if (!text.equals("")){
                    getContentResolver().delete(mUri, text, null);
                    ed.setText("");
                }
            }
        });

        //INSERT TEST
        findViewById(R.id.button8).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                String text = ed.getText().toString();
                if (!text.equals("")){
                    String[] kvp = text.split(":");
                    if (kvp.length > 1) {
                        ContentValues cv = new ContentValues();
                        cv.put("key", kvp[0]);
                        cv.put("value", kvp[1]);
                        Log.d(TAG, "Inserting " + kvp[0] + " and " + kvp[1]);
                        getContentResolver().insert(mUri, cv);
                        tv.setText(text + " inserted! ");
                    }
                    ed.setText("");
                }
            }
        });
        currentActivity = this;
    }

    private void printQueryResults(String query, TextView tv){
        /*
            Runs a query and prints the result to the screen
        */
        Cursor rCursor = getContentResolver().query(mUri, null,query, null, null);
        printToScreen(rCursor);
    }

    public static void printToScreen(Cursor rCursor){
        /*
            Prints the contents of a result cursor to the screen
        */
        TextView tv = textV;
        tv.setText("");
        if (rCursor == null) {
            Log.e(TAG, "Result null");
            tv.append("No results found!" + "\n");
            return;
        }

        int keyIndex = rCursor.getColumnIndex("key");
        int valueIndex = rCursor.getColumnIndex("value");

        rCursor.moveToFirst();
        tv.append(rCursor.getCount() + " results found " + "\n");
        if (rCursor.getCount() > 0) {
            while (!rCursor.isLast()) {
                String key = rCursor.getString(keyIndex);
                String value = rCursor.getString(valueIndex);
                tv.append(key + " - " + value + "\n");
                rCursor.moveToNext();
            }
            String key = rCursor.getString(keyIndex);
            String value = rCursor.getString(valueIndex);
            tv.append(key + " - " + value + "\n");
        }
    }

    private Uri buildUri(String scheme, String authority) {
        /*
            Builds a URI
        */
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

	@Override
	public boolean onCreateOptionsMenu(Menu menu) {
		// Inflate the menu; this adds items to the action bar if it is present.
		getMenuInflater().inflate(R.menu.simple_dynamo, menu);
		return true;
	}
	
	public void onStop() {
        super.onStop();
	    Log.v("Test", "onStop()");

	}

}
