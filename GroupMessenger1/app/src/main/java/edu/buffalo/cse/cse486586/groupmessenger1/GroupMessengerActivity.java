package edu.buffalo.cse.cse486586.groupmessenger1;

import android.app.Activity;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.KeyEvent;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 * 
 * @author stevko
 *
 */
public class GroupMessengerActivity extends Activity {

    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    static final int SERVER_PORT = 10000;
    static int sequenceNumber=-1;
    private static final String KEY="key";
    private static final String VALUE="value";

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            try {

                Socket socket=null;

                // For every message, send the message to all the AVDs i.e Servers in this context

                // If client is AVD0

                if (msgs[1].equals(REMOTE_PORT0))
                {
                    for(int i=0;i<5;i++)
                    {
                        if(i==0)
                        {
                            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT1));
                        }
                        if(i==1)
                        {
                            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT2));
                        }
                        if(i==2)
                        {
                            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT3));
                        }
                        if(i==3)
                        {
                            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT4));
                        }
                        if(i==4)
                        {
                            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT0));
                        }

                        String msgToSend = msgs[0];

                        OutputStreamWriter ostream = new OutputStreamWriter(socket.getOutputStream());
                        BufferedWriter bw=new BufferedWriter(ostream);
                        bw.write(msgToSend);
                        bw.flush();

                        InputStreamReader istream=new InputStreamReader(socket.getInputStream());
                        BufferedReader brClient=new BufferedReader(istream);

                        if(brClient.readLine()=="PA2 OK")
                        {
                            socket.close();
                        }
                    }
                }

                //If client is AVD1

                if (msgs[1].equals(REMOTE_PORT1))
                {
                    for(int i=0;i<5;i++)
                    {
                        if(i==0)
                        {
                            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT0));
                        }
                        if(i==1)
                        {
                            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT2));
                        }
                        if(i==2)
                        {
                            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT3));
                        }
                        if(i==3)
                        {
                            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT4));
                        }
                        if(i==4)
                        {
                            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT1));
                        }
                        String msgToSend = msgs[0];

                        OutputStreamWriter ostream = new OutputStreamWriter(socket.getOutputStream());
                        BufferedWriter bw=new BufferedWriter(ostream);
                        bw.write(msgToSend);
                        bw.flush();

                        InputStreamReader istream=new InputStreamReader(socket.getInputStream());
                        BufferedReader brClient=new BufferedReader(istream);

                        if(brClient.readLine()=="PA2 OK")
                        {
                            socket.close();
                        }
                    }
                }

                //If client is AVD2

                if (msgs[1].equals(REMOTE_PORT2))
                {
                    for(int i=0;i<5;i++)
                    {
                        if(i==0)
                        {
                            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT0));
                        }
                        if(i==1)
                        {
                            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT1));
                        }
                        if(i==2)
                        {
                            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT3));
                        }
                        if(i==3)
                        {
                            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT4));
                        }
                        if(i==4)
                        {
                            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT2));
                        }
                        String msgToSend = msgs[0];

                        OutputStreamWriter ostream = new OutputStreamWriter(socket.getOutputStream());
                        BufferedWriter bw=new BufferedWriter(ostream);
                        bw.write(msgToSend);
                        bw.flush();

                        InputStreamReader istream=new InputStreamReader(socket.getInputStream());
                        BufferedReader brClient=new BufferedReader(istream);

                        if(brClient.readLine()=="PA2 OK")
                        {
                            socket.close();
                        }
                    }
                }

                //If client is AVD3

                if (msgs[1].equals(REMOTE_PORT3))
                {
                    for(int i=0;i<5;i++)
                    {
                        if(i==0)
                        {
                            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT0));
                        }
                        if(i==1)
                        {
                            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT1));
                        }
                        if(i==2)
                        {
                            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT2));
                        }
                        if(i==3)
                        {
                            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT4));
                        }
                        if(i==4)
                        {
                            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT3));
                        }

                        String msgToSend = msgs[0];

                        OutputStreamWriter ostream = new OutputStreamWriter(socket.getOutputStream());
                        BufferedWriter bw=new BufferedWriter(ostream);
                        bw.write(msgToSend);
                        bw.flush();

                        InputStreamReader istream=new InputStreamReader(socket.getInputStream());
                        BufferedReader brClient=new BufferedReader(istream);

                        if(brClient.readLine()=="PA2 OK")
                        {
                            socket.close();
                        }
                    }
                }

                //If client is AVD4

                if (msgs[1].equals(REMOTE_PORT4))
                {
                    for(int i=0;i<5;i++)
                    {
                        if(i==0)
                        {
                            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT0));
                        }
                        if(i==1)
                        {
                            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT1));
                        }
                        if(i==2)
                        {
                            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT2));
                        }
                        if(i==3)
                        {
                            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT3));
                        }
                        if(i==4)
                        {
                            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT4));
                        }
                        String msgToSend = msgs[0];

                        OutputStreamWriter ostream = new OutputStreamWriter(socket.getOutputStream());
                        BufferedWriter bw=new BufferedWriter(ostream);
                        bw.write(msgToSend);
                        bw.flush();

                        InputStreamReader istream=new InputStreamReader(socket.getInputStream());
                        BufferedReader brClient=new BufferedReader(istream);

                        if(brClient.readLine()=="PA2 OK")
                        {
                            socket.close();
                        }
                    }
                }
                //      remotePort = REMOTE_PORT1; //Can remoteports 1,2,3,4 be added here and sockets 1,2,3,4 be created


            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }


            return null;
        }
    }

    Uri contentUri=buildUri("content","edu.buffalo.cse.cse486586.groupmessenger1.provider");

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);


        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        try {

            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            Log.i(TAG,"Server task created");
        } catch (IOException e) {

            Log.e(TAG, "Can't create a ServerSocket");
            return;
        }


        /*
         * TODO: Use the TextView to display your messages. Though there is no grading component
         * on how you display the messages, if you implement it, it'll make your debugging easier.
         */
        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

        /*
         * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
         * OnPTestClickListener demonstrates how to access a ContentProvider.
         */
        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));

        /*
         * TODO: You need to register and implement an OnClickListener for the "Send" button.
         * In your implementation you need to get the message from the input box (EditText)
         * and send it to other AVDs.
         */

        // ---------- "Send" button OnClickListener implementation --------------------

        final EditText editText = (EditText) findViewById(R.id.editText1);

        Button sendButton=(Button)findViewById(R.id.button4);

        sendButton.setOnClickListener(
                new Button.OnClickListener()  //Interface to be implemented
                {
                    public void onClick(View v) // Method to be overridden
                    {
                        String msg=editText.getText().toString()+"\n";
                        editText.setText("");

                        //Client task will start running after the "Send" button is clicked

                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);
                    }
                }
        );

    }


    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            /*
             * TODO: Fill in your server code that receives messages and passes them
             * to onProgressUpdate().
             */
            String str = "";
            Socket s;

            try{
                while(true)
                {
                    s=serverSocket.accept();
                    InputStreamReader istream=new InputStreamReader(s.getInputStream());
                    BufferedReader br=new BufferedReader(istream);

                    OutputStreamWriter osw = new OutputStreamWriter(s.getOutputStream());
                    BufferedWriter bwServer=new BufferedWriter(osw);
                    bwServer.write("PA2 OK");
                    bwServer.flush();

                    str=br.readLine();
                    publishProgress(str);

                    //Sequence Number. Satrts with -1 and Increases by one each time

                    sequenceNumber++;

                    //Our content provider cannot be directly accessed. Can only be accessed using a content resolver.

                    //-----------Content resolver to store sequence number and message in DB--------

                    //Every time a client sends a message, it will be received and stored by all the servers i.e AVDs in database

                    ContentResolver cr=getContentResolver();
                    ContentValues cValue=new ContentValues(); // used to insert value into DB
                    cValue.put(KEY,Integer.toString(sequenceNumber));
                    cValue.put(VALUE,str);
                    cr.insert(contentUri,cValue);

                    s.close();


                }
            }
            catch(Exception e)
            {
                Log.e(TAG,"IOException");
            }

            return null;

        }
        protected void onProgressUpdate(String...strings) {
            /*
             * The following code displays what is received in doInBackground().
             */
            String strReceived = strings[0].trim();
            TextView remoteTextView = (TextView) findViewById(R.id.textView1);
            remoteTextView.append(strReceived + "\t\n");

            return;
        }

    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }
}

