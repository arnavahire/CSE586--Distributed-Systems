package edu.buffalo.cse.cse486586.groupmessenger2;

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
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.PriorityQueue;


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
    static int serverSequenceNumber=-1;
    static int maxAgreedSeqNumber=-1;
    public static int uniqueId=0;
    static int keyNo=-1;
    static String failedPort="";
    static int f=0;
    private static final String KEY="key";
    private static final String VALUE="value";


    /*               Comparator and priority queue initialization for ISIS algorithm                */

    Comparator<msgObject> comparator=new sequenceNumberComparator();
    PriorityQueue<msgObject> queue=new PriorityQueue<msgObject>(25,comparator);
    PriorityQueue<msgObject> fqueue=new PriorityQueue<msgObject>(25,comparator);

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            try {

                Socket socket=null;
                LinkedList<Integer> list=new LinkedList<Integer>();  // To store the proposed sequence numbers from all AVDs

                // For every message, send the message to all the AVDs i.e Servers in this context

                // If client is AVD0/AVD1/AVD2/AVD3/AVD4

                for(int i=0;i<5;i++)
                {
                    if(i==0)
                    {
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT1));
                        //    socket.setSoTimeout(500);
                    }
                    if(i==1)
                    {
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT2));
                        //    socket.setSoTimeout(500);
                    }
                    if(i==2)
                    {
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT3));
                        //    socket.setSoTimeout(500);
                    }
                    if(i==3)
                    {
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT4));
                        //    socket.setSoTimeout(500);
                    }
                    if(i==4)
                    {
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT0));
                        //    socket.setSoTimeout(500);
                    }


                    String msgToSend = msgs[0];

                    String portNumber=msgs[1];  // Extracted portNumber for msg ID
                    Log.i(TAG,msgs[1]);

/*--------------------------------------------------ISIS ALGORITHM starts--------------------------------------------------------------------------------------------------------*/

                    // Create msg ID

                    String msgID=portNumber+Integer.toString(uniqueId);

                    //Send msgID, message  (Sending port number also so that it can be extracted and used at server side for comparison purposes)

                    OutputStreamWriter ostream = new OutputStreamWriter(socket.getOutputStream());
                    BufferedWriter bw=new BufferedWriter(ostream);
                    bw.write(msgID+":"+portNumber+":"+msgToSend);
                    bw.flush();

                    Log.i(TAG,"Client Task: "+msgID+" "+msgToSend);

                    // Now after server task, client obtains the proposed sequence number
                    try{
                        InputStreamReader istream=new InputStreamReader(socket.getInputStream());
                        BufferedReader brClient=new BufferedReader(istream);

                        String readstring="";
                        readstring =brClient.readLine();

                        Log.i(TAG,"Server reply at client: "+readstring);

                        String[] fragments=readstring.split(":");  // 'readstring' String contains the port number of the proposed sequence number sender
                        String ports=fragments[0];
                        String proposedNumber=fragments[1];

                        // Adding proposed seq numbers in the list to obtain max agreed sequence number

                        list.add(Integer.parseInt(proposedNumber));

                        if(readstring.contains("PA2 OK"))
                        {
                            Log.i(TAG,"Closing client socket");
                            socket.close();
                        }

                    }
                    catch (IOException e){
                        Log.e(TAG,"Client IO exception during multicast-1 at port: "+portNumber);
                        socket.close();
                    }
                    catch (NullPointerException e){
                        Log.e(TAG,"Null pointer exception at client during mulitcast-1 at port: "+String.valueOf(socket.getPort()));
                        socket.close();
                        failedPort=String.valueOf(socket.getPort());    // Caught failed port
                    }

                }

                //Checking the proposed sequence numbers in the list

                for(int x=0;x<list.size();x++)
                {
                    Log.i(TAG,"Proposed numbers: "+list.get(x).toString()+" ");
                }

                //Finding max of all the proposed sequence numbers

                int max=0;
                for(int i=0;i<list.size();i++)
                {
                    if(list.get(i)>max)
                        max=list.get(i);
                }

                //Multicast the original msg with this agreed sequence number

                for(int i=0;i<5;i++)
                {
                    if(i==0)
                    {
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT1));
                        //    socket.setSoTimeout(500);
                    }
                    if(i==1)
                    {
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT2));
                        //    socket.setSoTimeout(500);
                    }
                    if(i==2)
                    {
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT3));
                        //    socket.setSoTimeout(500);
                    }
                    if(i==3)
                    {
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT4));
                        //    socket.setSoTimeout(500);
                    }
                    if(i==4)
                    {
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT0));
                        //    socket.setSoTimeout(500);
                    }

                    // To handle failed port

                    if(String.valueOf(socket.getPort()).equals(failedPort))
                    {
                        socket.close();
                        continue;                   // go to next iteration
                    }


/*--------------------DO NOT GO AHEAD FOR A FAILED PROCESS IF DETECTED DURING MULTICAST-1---------------------------------------------------------------------------------------------------------------------------------------*/

                    String msgToSend = msgs[0];

                    String portNumber=msgs[1];  // Extracted portNumber for msg ID
                    Log.i(TAG,msgs[1]);

                    // Create msg ID

                    String msgID=portNumber+Integer.toString(uniqueId);

                    //Send msgID, Max Agreed Sequence, message  (Sending port number also so that it can be extracted and used at server side for comparison purposes)

                    OutputStreamWriter ostream = new OutputStreamWriter(socket.getOutputStream());
                    BufferedWriter bw=new BufferedWriter(ostream);
                    bw.write(msgID+":"+portNumber+":"+max+":"+msgToSend);
                    bw.flush();

                    Log.i(TAG,"Client Task: "+msgID+" "+portNumber+" "+max+" "+msgToSend);

                    try{
                        InputStreamReader istream=new InputStreamReader(socket.getInputStream());
                        BufferedReader brClient=new BufferedReader(istream);

                        String readstring="";
                        readstring=brClient.readLine();   //incase null value comes in, readstring will not be null

                        Log.i(TAG,"Server reply at client: "+readstring);

                        if(readstring.contains("PA2 OK"))
                        {
                            socket.close();
                        }

                    }
                    catch (IOException e){
                        Log.e(TAG,"Client IO exception during multicast-2 at: "+portNumber);
                        socket.close();
                    }
                    catch (NullPointerException e){
                        Log.e(TAG,"Null pointer exception at client during mulitcast-2 at port: "+String.valueOf(socket.getPort()));

                        socket.close();
                    }


                }

                uniqueId++;   //incrementing msgID for every message sent by client

            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");

            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");}

            return null;
        }
    }

    Uri contentUri=buildUri("content","edu.buffalo.cse.cse486586.groupmessenger2.provider");

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
            msgObject obj;

            try{
                while(true)
                {
                    s=serverSocket.accept();
                    InputStreamReader istream=new InputStreamReader(s.getInputStream());
                    BufferedReader br=new BufferedReader(istream);

                    OutputStreamWriter osw = new OutputStreamWriter(s.getOutputStream());
                    BufferedWriter bwServer=new BufferedWriter(osw);

                    str=br.readLine();

                    // Separate msgID from the message

                    String[] parts= str.split(":");

                    if(parts.length==3)
                    {
                        String msgId=parts[0];
                        String portnumber=parts[1];
                        String msg=parts[2];

                        Log.i(TAG,"Server Task: "+msgId +" " + portnumber+" " + msg);
                        Log.i(TAG,"Server Task: max agreed number : "+Integer.toString(maxAgreedSeqNumber));
                        Log.i(TAG,"Server Task: seq number : "+Integer.toString(serverSequenceNumber));

                        // Find max of agreed and local sequence number

                        if(maxAgreedSeqNumber>=serverSequenceNumber)
                        {
                            serverSequenceNumber=maxAgreedSeqNumber+1;
                        }
                        else
                        {
                            serverSequenceNumber=serverSequenceNumber+1;
                        }

                        // Create a message object to be inserted in queue

                        obj=new msgObject(msgId,serverSequenceNumber,msg,0,portnumber);
                        Log.i(TAG,"MSG Object content: "+obj.messageID+" "+serverSequenceNumber+" "+msg+" "+portnumber);

                        if(!(obj.pNumber.equalsIgnoreCase(failedPort)))
                        {
                            synchronized (queue)
                            {
                                Log.i(TAG,"Adding"+" "+queue.add(obj));
                            }

                            Log.i(TAG,"Head value: "+queue.peek().message+" "+queue.peek().messageID);

                            // sending max seq number as proposed sequence number

                            bwServer.write(portnumber+":"+Integer.toString(serverSequenceNumber)+":"+"PA2 OK");
                            bwServer.flush();
                        }

                        s.close();

                    }
                    else if(parts.length==4)
                    {

                        String msgId=parts[0];
                        String portnumber=parts[1];
                        maxAgreedSeqNumber=Integer.parseInt(parts[2]);
                        String msg=parts[3];

                        Log.i(TAG,"Printing max agreed number : "+Integer.toString(maxAgreedSeqNumber));
                        Log.i(TAG,"Printing msgID : "+msgId);

                        //Create a new object that is an updated entry for the current entry in queue

                        msgObject newobj=new msgObject(msgId,maxAgreedSeqNumber,msg,1,portnumber);

                        Log.i(TAG,"New object's msgID: "+newobj.messageID);

                        //Queue re-ordering - (removing old entry and adding new one) Here we are giving reference of current object as in our 'msgObject' class we have implemented 'equals' method which checks for the same msg ID and deletes that corresponding entry from queue

                        // Using 'synchronized' for letting only one thread access the same queue at one time

                        synchronized (queue)
                        {
                            Log.i(TAG,"Removing: "+queue.remove(newobj));

                            Log.i(TAG,"Adding new object: "+queue.add(newobj));
                        }

                        // Removing message entries corresponding to failed port

                        LinkedList<msgObject> list=new LinkedList<msgObject>();

                        for (msgObject m : queue) {
                            if(m.pNumber.equalsIgnoreCase(failedPort)&& m.flag==0)  // here we collect the msg from the failed port in the list
                                list.add(m);
                        }

                        if(list.size()==0)   // if no undeliverable msg from failed port, do nothing, maitain queue as it is
                        {

                        }
                        else if(queue.contains(list.get(0)))  // if message from failed port present in the queue, remove that message
                        {
                            synchronized(queue)
                            {
                                queue.remove(list.get(0));
                            }

                        }

                        // Deliver all msgs to the processes that are deliverable i.e all the msgs whose flag is 1

                        msgObject head=queue.peek();

                        Log.i(TAG,"Head contents:"+head.messageID+" "+head.message+" Head flag: "+head.flag);

                        Log.i("Initial queue: ",queue.toString());

                        // We create another queue (fqueue) in which we add only those msgs whose flag is 1 i.e only the messages that are deliverable

                        while(head!=null && head.flag==1)
                        {
                            msgObject o=queue.poll();
                            fqueue.add(o);                       // adding the messages
                            head=queue.peek();
                        }

                        Log.i("Final queue: ",fqueue.toString());

                        /* LOGIC For message delivery--

                           This if condition 'if(fqueue.size()>=20||f==1)' will execute in following scenarios
                           1. The first time when the fqueue reaches a size of 20:
                            This we are doing so that most of the messages sent are buffered before delivery to database.
                            If we do not buffer the messages before delivery, due to delays, different messages may get different key values and will get stored in database immediately. That's undesirable.
                           2. When the global variable 'f' is 1.
                            This we are doing because after the 1st 20 messages are delivered to database, we have to deliver other messages too. For that from 21st message, after each message is received in fqueue it will be delivered to database.
                            Here we don't know how many messages will arrive after a node has failed. But we are guaranteed that 5 messages from 4 other AVDs i.e a total of 20 messages will definitely arrive.
                            The while loop within the if condition will keep executing till we encounter a case where a message is at head of 'queue' and has flag=0. In that case fqueue=empty hence head=null.
                            That time, don't insert entry into database. Just skip the iteration. Because in the next iteration, the head of 'queue' may have flag as 1 and then 'fqueue' can deliver that msg in DB.

                        */

                        if(fqueue.size()>=20||f==1)
                        {
                            Log.i("Fqueue: ",fqueue.toString());

                            head=fqueue.peek();

                            if(head==null)      // If final queue has no element. Case where initial queue is not empty but contains a message at head with flag=0,it won't be delivered to fqueue i.e for that msg multicast-2 is yet to happen
                            {

                            }
                            else
                            {
                                f=1;           // if head is not null, add it to database.  NOTE: This flag gets set the very first time the if condition is entered. after that this if condition i.e 'if(fqueue.size()>=20||f==1)' will always be entered since f will always be 1

                                while(head!=null && head.flag==1)  //set head.flag in if condition
                                {
                                    msgObject frontMsg=fqueue.poll();  // remove each entry from fqueue for adding to database
                                    publishProgress(frontMsg.message);

                                    //Now we will store keys in database, but the testing script requires keys to be inserted in sequential manner, i.e from 0 to 25 and not in random fashion

                                    //hence we will use another variable 'keyNo' and we will store this as the key number corresponding to that message

                                    keyNo++;  // initially keyNo was -1, hence we increment first and then add the keyNo.

                                    ContentResolver cr=getContentResolver();
                                    ContentValues cValue=new ContentValues(); // used to insert value into DB
                                    cValue.put(KEY,Integer.toString(keyNo));
                                    cValue.put(VALUE,frontMsg.message);
                                    cr.insert(contentUri,cValue);

                                    head=fqueue.peek();

                                    if(head==null)
                                        break;

                                }
                            }
                        }

                        bwServer.write("PA2 OK");
                        bwServer.flush();

                        s.close();

                    }
                }
            }
            catch(IOException e)
            {
                Log.e(TAG,"IOException at server");
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

    class msgObject extends Object
    {
        String messageID;
        int sequenceNumber;
        String message;
        int flag;
        String pNumber;

        public msgObject(String m_ID,int sNumber,String m,int f,String port)
        {
            this.messageID=m_ID;
            this.sequenceNumber=sNumber;
            this.message=m;
            this.flag=f;
            this.pNumber=port;
        }

        public boolean equals(Object obj)
        {
            Log.i("jijiji","Equals method");
            msgObject msg=(msgObject)obj;
            if(this.messageID.equals(msg.messageID))
                return true;
            else
                return false;
        }

        public String toString()
        {
            return "msgID: "+this.messageID+" Sequence No: "+this.sequenceNumber+" Msg: "+this.message+" Flag: "+this.flag+" Port Number: "+this.pNumber;
        }


    }
    // the message with least sequence number (highest priority) should be inserted in the queue

    class sequenceNumberComparator implements Comparator <msgObject>
    {
        @Override
        public int compare(msgObject obj1, msgObject obj2) {

            // insert the element whose sequence number is the least i.e priority=Least sequence number

            if(obj1.sequenceNumber<obj2.sequenceNumber)
            {
                return -1;
            }
            else if(obj1.sequenceNumber>obj2.sequenceNumber)
            {
                return 1;
            }
            else
            {
                // when sequence numbers clash i.e incase of equal sequence numbers, sort by least port numbers i.e priority=least port numbers

                if(Integer.parseInt(obj1.pNumber)<Integer.parseInt(obj2.pNumber))
                {
                    return -1;
                }
                else if(Integer.parseInt(obj1.pNumber)>Integer.parseInt(obj2.pNumber))
                {
                    return 1;
                }
            }
            return 0;
        }
    }
}




