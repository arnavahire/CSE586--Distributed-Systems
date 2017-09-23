package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.LinkedList;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.net.http.SslCertificate;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
import android.widget.TextView;

public class SimpleDhtProvider extends ContentProvider {

    static final String TAG = SimpleDhtActivity.class.getSimpleName();
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    static final int SERVER_PORT = 10000;
    static String myPort;
    static Node node=new Node();
    static LinkedList<Node> list=new LinkedList<Node>();
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";

    Uri contentUri=buildUri("content","edu.buffalo.cse.cse486586.simpledht.provider");

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }
    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {

        SqliteDB db=new SqliteDB(this.getContext()); // Android standard for SQLiteOpenHelper class
        SQLiteDatabase sqlDb=db.getWritableDatabase(); // To get a writeable database we create an object of type SQLDatabase
        String[] selection1={selection}; // in delete method of SQLiteDatabase object the selectionArgs field always contains an array for selection i.e array of values where the value lies in 0th index of array

        if(selection.equalsIgnoreCase("*"))
        {
            sqlDb.delete("database",null,null);
            Log.i(TAG,"Deleted");
        }
        else if(selection.equalsIgnoreCase("@"))
        {
            sqlDb.delete("database",null,null);
            Log.i(TAG,"Deleted");
        }
        else
        {
            sqlDb.delete("database","key=?",selection1);
            Log.i(TAG,"Deleted");
        }

        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values){

        if(node.getNode_id()==null||myPort.equalsIgnoreCase(node.getSuccessor_pno()))  // For other AVDs, if all inserts are being performed on a single avd then their node id must be null, since they can only get a node id when 5554 becomes active.
        // Also If only AVD 5554 is alive then its successor's port and it's port is the same
        {

            // Using SqLite database to store the (key,value) pairs

            SqliteDB db=new SqliteDB(this.getContext()); // Android standard for SQLiteOpenHelper class

            SQLiteDatabase sqlDb=db.getWritableDatabase(); // To get a writeable database we create an object of type SQLDatabase

            //  sqlDb.insert("database",null,values);

            //------- insertWithOnConflict(table_name,null field,ContentValues,Conflict_Algorithm)----------

            sqlDb.insertWithOnConflict("database",null,values,SQLiteDatabase.CONFLICT_REPLACE);  // To perform insertion and on Conflict i.e incase of duplicate records the old records are replaced by new ones.

            sqlDb.close();   // Close the DB on insertion

            Log.v("Inserting (Usual)", values.toString());
        }
        else
        {
            String key=(String)values.get(KEY_FIELD);
            String key_value=(String)values.get(VALUE_FIELD);
            String genKey="";
            Log.i("In Insert: ",key);
            try{
                genKey=genHash(key);
                Log.i("Key hash: ",genKey);
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }

            // Find out if its a boundary node

            int value=node.getNode_id().compareTo(node.getPredecesssor());   // predecessor is greater than current nodeid i.e its a boundary node
            if(value<0)
            {
                int val=genKey.compareTo(node.getNode_id());        // compare the key with current node_id
                int val2=genKey.compareTo(node.getPredecesssor());  // compare the key with the current node's predecessor
                if(val<=0 || val2>0)                              // if the value is greater than predecessor or if it is less than or equal to the current node_id
                {
                    // Using SqLite database to store the (key,value) pairs

                    SqliteDB db=new SqliteDB(this.getContext()); // Android standard for SQLiteOpenHelper class

                    SQLiteDatabase sqlDb=db.getWritableDatabase(); // To get a writeable database we create an object of type SQLDatabase

                    //  sqlDb.insert("database",null,values);

                    //------- insertWithOnConflict(table_name,null field,ContentValues,Conflict_Algorithm)----------

                    sqlDb.insertWithOnConflict("database",null,values,SQLiteDatabase.CONFLICT_REPLACE);  // To perform insertion and on Conflict i.e incase of duplicate records the old records are replaced by new ones.

                    sqlDb.close();   // Close the DB on insertion

                    Log.v("Inserting M ", values.toString());
                }
                else if(val>0||(val<0 && val2<0))
                {
                    Socket s;
                    try
                    {
                        s= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(node.getSuccessor_pno()));
                        OutputStreamWriter os = new OutputStreamWriter(s.getOutputStream());
                        BufferedWriter bwriter = new BufferedWriter(os);
                        String msg="Insert";

                        bwriter.write(msg+":"+node.getpNumber()+":"+key+":"+key_value);  // send insert, port number of current node and contentvalue.
                        bwriter.newLine();                             // We add a new line so that while writing bufferedwriter can write the entire line and then flush it
                        bwriter.flush();
                        Log.i("Insert Fwd","Msg sent to Successor");

                        InputStreamReader isstream = new InputStreamReader(s.getInputStream());
                        BufferedReader brC = new BufferedReader(isstream);

                        String readLine="";
                        readLine=brC.readLine();

                        if (readLine.contains("PA3 OK"))
                        {
                            Log.i(TAG,"Successor replied. Insert Communication Successful..");
                            s.close();
                        }

                    }catch (Exception e)
                    {
                        e.printStackTrace();
                    }

                }

            }
            else  // predecessor is less than current node id
            {
                int val=genKey.compareTo(node.getNode_id());        // compare the key with current node_id
                int val2=genKey.compareTo(node.getPredecesssor());  // compare the key with the current node's predecessor

                if(val<=0 && val2>0)  // notice the '&&' operator. It means both the conditions should be satisfied. key should be greater than predecessor and less than or equal to current node
                {
                    SqliteDB db=new SqliteDB(this.getContext()); // Android standard for SQLiteOpenHelper class

                    SQLiteDatabase sqlDb=db.getWritableDatabase(); // To get a writeable database we create an object of type SQLDatabase

                    //  sqlDb.insert("database",null,values);

                    //------- insertWithOnConflict(table_name,null field,ContentValues,Conflict_Algorithm)----------

                    sqlDb.insertWithOnConflict("database",null,values,SQLiteDatabase.CONFLICT_REPLACE);  // To perform insertion and on Conflict i.e incase of duplicate records the old records are replaced by new ones.

                    sqlDb.close();   // Close the DB on insertion

                    Log.v("Inserting M ", values.toString());
                }
                else if(val>0||(val<0 && val2<0))  // If key is greater than the current node or if the key is less than current as well as predecessor node, fwd it
                {
                    Socket s;
                    try
                    {
                        s= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(node.getSuccessor_pno()));
                        OutputStreamWriter os = new OutputStreamWriter(s.getOutputStream());
                        BufferedWriter bwriter = new BufferedWriter(os);
                        String msg="Insert";

                        bwriter.write(msg+":"+node.getpNumber()+":"+key+":"+key_value);  // send insert, port number of current node and contentvalue.
                        bwriter.newLine();                             // We add a new line so that while writing bufferedwriter can write the entire line and then flush it
                        bwriter.flush();
                        Log.i("Insert Fwd","Msg sent to Successor");

                        InputStreamReader isstream = new InputStreamReader(s.getInputStream());
                        BufferedReader brC = new BufferedReader(isstream);

                        String readLine="";
                        readLine=brC.readLine();

                        if (readLine.contains("PA3 OK"))
                        {
                            Log.i(TAG,"Successor replied. Insert Communication Successful..");
                            s.close();
                        }

                    }catch (Exception e)
                    {
                        e.printStackTrace();
                    }

                }
            }
        }

        return uri;
    }

    @Override
    public boolean onCreate() {

        // According to Lifecycle onCreate() will be called first. Since this content provider is registered in AndroidManifest.xml, even before the main activity is called, oncreate of content provider will be called

        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        try {

            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            Log.i(TAG,"Server task created");
        } catch (IOException e) {

            Log.e(TAG, "Can't create a ServerSocket");

        }

        String msg="";
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msg,myPort);

        Log.i(TAG,"Client task created");

        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        Cursor cursor=null;

        if(node.getNode_id()==null||selection.equalsIgnoreCase("@")||myPort.equalsIgnoreCase(node.getSuccessor_pno())) // if selection is @ or if only one avd is active and it is either 5554 or not
        {
            SqliteDB dbRead=new SqliteDB(this.getContext()); //Android standard

            SQLiteDatabase sqlDBRead=dbRead.getReadableDatabase(); // A readable database to perform a query

            String[] selection1={selection}; // in query method of SQLiteDatabase object the selectionArgs field always contains an array for selection i.e array of values where the value lies in 0th index of array

            //--------------SQLDatabase.query(table_name,projection i.e columns to be returned,where_clause,value to be checked,OrderBY,Having and so on)

            /*-------------------LOGIC for querying specific keys, all keys on a single AVD (@), and all keys on all AVDs(*)----------------------------------*/

            if(selection.equalsIgnoreCase("*"))
            {
                cursor=sqlDBRead.query("database",null,null,null,null,null,null,null); //query method returns a cursor for all (*) key-value pairs
                Log.i(TAG,cursor.getCount()+"");
            }
            else if(selection.equalsIgnoreCase("@"))
            {
                cursor=sqlDBRead.query("database",null,null,null,null,null,null,null); //query method returns a cursor for all (@) key-value pairs on a single AVD
                Log.i(TAG,cursor.getCount()+"");
            }
            else
            {
                cursor=sqlDBRead.query("database",null,"key=?",selection1,null,null,null,null); //query method returns a cursor which can be passed to the UI element
                Log.i(TAG,cursor.getCount()+"");
            }

            Log.v("Querying (Usual)", selection);
        }
        //-----------------------------------------------------  * Query----------------------------------------------------------------------------------------------------------------------//
        else if(selection.equalsIgnoreCase("*"))
        {
            //1. Send *queryRequest till the end of the chord
                Socket st;
                try
                {
                    st= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(node.getSuccessor_pno()));
                    OutputStreamWriter o=new OutputStreamWriter(st.getOutputStream());
                    BufferedWriter b=new BufferedWriter(o);
                    b.write("*QueryRequest"+":"+node.getPredecesssor());
                    b.newLine();
                    b.flush();

                    InputStreamReader i=new InputStreamReader(st.getInputStream());
                    BufferedReader br=new BufferedReader(i);

                    Log.i("Query Fwd to S: ","*QueryRequest"+":"+node.getPredecesssor());

                    String readLine="";
                    readLine=br.readLine();

                    String[] f=readLine.split(":");

                    if(f.length==2 && f[0].equalsIgnoreCase("Empty"))  // if other nodes have no data
                    {
                        if (readLine.contains("PA3 OK"))   // close the socket for the successor
                        {
                            Log.i(TAG,"Successor replied Empty. * Query Communication Successful..");
                            st.close();
                        }

                        SqliteDB dbread2=new SqliteDB(getContext());
                        SQLiteDatabase sqlDBRead2=dbread2.getReadableDatabase();
                        cursor=sqlDBRead2.query("database",null,null,null,null,null,null,null);

                    }
                    else                                   // if the data is returned
                    {

                        SqliteDB dbrd=new SqliteDB(getContext());
                        SQLiteDatabase sqlDBRd=dbrd.getReadableDatabase();
                        cursor=sqlDBRd.query("database",null,null,null,null,null,null,null);                                    //Add the querying AVDs own data in the final string received from others

                        String strg=readLine;
                        Log.i("Received line: ",readLine);

                        // CODE FOR CURSOR WHEN IT IS Empty i.e the querying AVD itself has no data

                       if(cursor.getCount()!=0)   // if the current avd has data
                        {
                            cursor.moveToFirst();
                            while(!cursor.isAfterLast())
                            {
                                String stg=":"+cursor.getString(0)+":"+cursor.getString(1);
                                strg=strg+stg;
                                Log.i("Stg content: ",stg);
                                Log.i("Strg content",strg);
                                cursor.moveToNext();
                            }
                            Log.i("Final String: ",strg);

                            String[] splits=strg.split(":");           // Split this final list of all the key-value pairs

                            LinkedList<String> keyList=new LinkedList<String>();
                            LinkedList<String> valueList=new LinkedList<String>();
                            for(int x=0;x<splits.length;x++)
                            {
                                if(x%2==0)
                                {
                                    keyList.add(splits[x]);   // add all keys in 1 list
                                }
                                else if(x%2==1)
                                {
                                    valueList.add(splits[x]);  // add all values in other list
                                }
                            }
                            for(int x=0;x<keyList.size();x++)  // insert all the keys and values in the final 'starcursortable' table one by one
                            {
                                String fKey=keyList.get(x);
                                String fValue=valueList.get(x);

                                SqliteDB dbnew=new SqliteDB(getContext());
                                SQLiteDatabase sqldb2= dbnew.getWritableDatabase();
                                ContentValues cv=new ContentValues();
                                cv.put(KEY_FIELD,fKey);
                                cv.put(VALUE_FIELD,fValue);
                                sqldb2.insertWithOnConflict("starcursortable",null,cv,SQLiteDatabase.CONFLICT_REPLACE);
                                sqldb2.close();
                                Log.v("Inserting in * Tbl:",cv.toString());

                            }
                        }
                        else  // if current AVD has no data
                        {
                            LinkedList<String> keyList=new LinkedList<String>();
                            LinkedList<String> valueList=new LinkedList<String>();
                            for(int x=0;x<f.length;x++)
                            {
                                if(x%2==0)
                                {
                                    keyList.add(f[x]);   // add all keys in 1 list
                                }
                                else if(x%2==1)
                                {
                                    valueList.add(f[x]);  // add all values in other list
                                }
                            }
                            for(int x=0;x<keyList.size();x++)  // insert all the keys and values in the final 'starcursortable' table one by one
                            {
                                String fKey=keyList.get(x);
                                String fValue=valueList.get(x);

                                SqliteDB dbnew=new SqliteDB(getContext());
                                SQLiteDatabase sqldb2= dbnew.getWritableDatabase();
                                ContentValues cv=new ContentValues();
                                cv.put(KEY_FIELD,fKey);
                                cv.put(VALUE_FIELD,fValue);
                                sqldb2.insertWithOnConflict("starcursortable",null,cv,SQLiteDatabase.CONFLICT_REPLACE);
                                sqldb2.close();
                                Log.v("Inserting in * Tbl:",cv.toString());

                            }
                        }


                        SqliteDB dbread2=new SqliteDB(getContext());
                        SQLiteDatabase sqlDBRead2=dbread2.getReadableDatabase();
                        cursor=sqlDBRead2.query("starcursortable",null,null,null,null,null,null,null);
                        cursor.moveToFirst();
                        while(!cursor.isAfterLast())
                        {
                            Log.i("The Last Cursor: ",cursor.getString(0)+" "+cursor.getString(1));
                            cursor.moveToNext();
                        }

                    }
                }
                catch (Exception e)
                {
                    e.printStackTrace();
                }
        }
        //--------------------------------------------------------------- Single value query------------------------------------------------------------------------------------------------------------------------
        else
        {

            String genSelection="";
            Log.i("In Query: ",selection);
            try{
                genSelection=genHash(selection);                 // hash the key to be searched
                Log.i("Selection (key) hash: ",genSelection);
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }

            int value=node.getNode_id().compareTo(node.getPredecesssor());  // if predecessor is greater than current node
            if(value<0)
            {
                int val=genSelection.compareTo(node.getNode_id());        // compare the key with current node_id
                int val2=genSelection.compareTo(node.getPredecesssor());  // compare the key with the current node's predecessor
                if(val<=0 || val2>0)                              // if the value is greater than predecessor or if it is less than or equal to the current node_id
                {
                    Log.i("Querying 1 key-val","Current Node");
                    SqliteDB dbRead=new SqliteDB(this.getContext()); //Android standard

                    SQLiteDatabase sqlDBRead=dbRead.getReadableDatabase(); // A readable database to perform a query

                    String[] selection1={selection};

                    cursor=sqlDBRead.query("database",null,"key=?",selection1,null,null,null,null); //query method returns a cursor which can be passed to the UI element

                    cursor.moveToFirst();  // set the cursor to first position, because initially cursor is at index '-1'

                    Log.i("Cursor count: ",cursor.getCount()+" ");
                }
                else if(val>0||(val<0 && val2<0))
                {
                    Socket s;
                    try
                    {
                        s= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(node.getSuccessor_pno()));
                        OutputStreamWriter os = new OutputStreamWriter(s.getOutputStream());
                        BufferedWriter bwriter = new BufferedWriter(os);
                        String msg="Query";

                        bwriter.write(msg+":"+node.getpNumber()+":"+selection);  // send insert, port number of current node and contentvalue.
                        bwriter.newLine();                             // We add a new line so that while writing bufferedwriter can write the entire line and then flush it
                        bwriter.flush();
                        Log.i("Query Fwd: ","Msg sent to Successor");

                        InputStreamReader isstream = new InputStreamReader(s.getInputStream());
                        BufferedReader brC = new BufferedReader(isstream);

                        String readLine="";
                        readLine=brC.readLine();

                        String[] fragments=readLine.split(":");
                        Log.i("Received key-val from S",readLine);

                        if(fragments.length==3)
                        {
                            String incomingKey=fragments[0];
                            String incomingValue=fragments[1];
                            String ack=fragments[2];

                            SqliteDB dbnew=new SqliteDB(getContext());
                            SQLiteDatabase sqldb2= dbnew.getWritableDatabase();
                            ContentValues cv=new ContentValues();
                            cv.put(KEY_FIELD,incomingKey);
                            cv.put(VALUE_FIELD,incomingValue);
                            sqldb2.insertWithOnConflict("cursortable",null,cv,SQLiteDatabase.CONFLICT_REPLACE);
                            sqldb2.close();
                            Log.v("Inserting in Qry Tbl: ",cv.toString());

                            SqliteDB dbread2=new SqliteDB(getContext());
                            SQLiteDatabase sqlDBRead2=dbread2.getReadableDatabase();
                            String[] select={incomingKey};
                            cursor=sqlDBRead2.query("cursortable",null,"key=?",select,null,null,null,null);
                            cursor.moveToFirst();
                            Log.i("Final Cursor: ",cursor.getString(0)+" "+cursor.getString(1));
                        }

                        if (readLine.contains("PA3 OK"))
                        {
                            Log.i(TAG,"Successor replied. Single Query Communication Successful..");
                            s.close();
                        }

                    }catch (Exception e)
                    {
                        e.printStackTrace();
                    }

                }

            }
            else
            {
                int val=genSelection.compareTo(node.getNode_id());        // compare the key with current node_id
                int val2=genSelection.compareTo(node.getPredecesssor());  // compare the key with the current node's predecessor

                if(val<=0 && val2>0)  // notice the '&&' operator. It means both the conditions should be satisfied. key should be greater than predecessor and less than or equal to current node
                {
                    Log.i("Querying 1 key-val","Current Node");
                    SqliteDB dbRead=new SqliteDB(this.getContext()); //Android standard

                    SQLiteDatabase sqlDBRead=dbRead.getReadableDatabase(); // A readable database to perform a query

                    String[] selection1={selection};

                    cursor=sqlDBRead.query("database",null,"key=?",selection1,null,null,null,null); //query method returns a cursor which can be passed to the UI element

                    cursor.moveToFirst();  // move the cursor to first row,because initially cursor is at index '-1'

                    Log.i("Cursor count: ",cursor.getCount()+" ");


                }
                else if(val>0||(val<0 && val2<0))  // If key is greater than the current node or if the key is less than current as well as predecessor node, fwd it
                {
                    Socket s;
                    try
                    {
                        s= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(node.getSuccessor_pno()));
                        OutputStreamWriter os = new OutputStreamWriter(s.getOutputStream());
                        BufferedWriter bwriter = new BufferedWriter(os);
                        String msg="Query";

                        bwriter.write(msg+":"+node.getpNumber()+":"+selection);  // send insert, port number of current node and contentvalue.
                        bwriter.newLine();                             // We add a new line so that while writing bufferedwriter can write the entire line and then flush it
                        bwriter.flush();
                        Log.i("Query Fwd: ","Msg sent to Successor");

                        InputStreamReader isstream = new InputStreamReader(s.getInputStream());
                        BufferedReader brC = new BufferedReader(isstream);

                        String readLine="";
                        readLine=brC.readLine();

                        String[] fragments=readLine.split(":");
                        Log.i("Received key-val from S",readLine);

                        if(fragments.length==3)
                        {
                            String incomingKey=fragments[0];
                            String incomingValue=fragments[1];
                            String ack=fragments[2];

                            SqliteDB dbnew=new SqliteDB(getContext());
                            SQLiteDatabase sqldb2= dbnew.getWritableDatabase();
                            ContentValues cv=new ContentValues();
                            cv.put(KEY_FIELD,incomingKey);
                            cv.put(VALUE_FIELD,incomingValue);
                            sqldb2.insertWithOnConflict("cursortable",null,cv,SQLiteDatabase.CONFLICT_REPLACE);
                            sqldb2.close();
                            Log.v("Inserting in Qry Tbl: ",cv.toString());

                            SqliteDB dbread2=new SqliteDB(getContext());
                            SQLiteDatabase sqlDBRead2=dbread2.getReadableDatabase();
                            String[] select={incomingKey};
                            cursor=sqlDBRead2.query("cursortable",null,"key=?",select,null,null,null,null);
                            cursor.moveToFirst();
                            Log.i("Final Cursor: ",cursor.getString(0)+" "+cursor.getString(1));
                        }


                        if (readLine.contains("PA3 OK"))
                        {
                            Log.i(TAG,"Successor replied. Single Query Communication Successful..");
                            s.close();
                        }

                    }catch (Exception e)
                    {
                        e.printStackTrace();
                    }

                }
            }

        }

        return cursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }


    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            try {

                Socket socket=null;

                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT0));   // send 'Join' request to emulator 5554 (AVD0)

                String portNumber=msgs[1];          // Client (join requester) port number

                int pNumber=Integer.parseInt(portNumber);   // Convert port number--> ex: 111108 to 5554
                pNumber=pNumber/2;

                portNumber=String.valueOf(pNumber);

                String msgToSend = "Join";

                Log.i("Client Task: ","Msg to be sent: "+msgToSend+" "+portNumber);

                OutputStreamWriter ostream = new OutputStreamWriter(socket.getOutputStream());
                BufferedWriter bw = new BufferedWriter(ostream);

                bw.write(msgToSend+":"+portNumber);
                bw.newLine();                             // We add a new line so that while writing bufferedwriter can write the entire line and then flush it
                bw.flush();
                Log.i("Client task","Msg sent");

                InputStreamReader istream = new InputStreamReader(socket.getInputStream());
                BufferedReader brClient = new BufferedReader(istream);

                String readLine="";
                readLine=brClient.readLine();

                if (readLine.contains("PA3 OK"))
                {
                    Log.i(TAG,"Communication Successful..");
                    socket.close();
                }

            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException"+e);
            }

            return null;
        }
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            String str = "";
            Socket s;
            Cursor cursor=null;

            try{
                while(true)
                {
                    s=serverSocket.accept();
                    InputStreamReader istream=new InputStreamReader(s.getInputStream());
                    BufferedReader br=new BufferedReader(istream);
                    str=br.readLine();

                    Log.i("Server Task: ","Data received- "+str);

                    String[] fragments=str.split(":");  // 'readstring' String contains the port number of the proposed sequence number sender

                    if(fragments.length==2 && fragments[0].equalsIgnoreCase("Join"))        // If operation is 'Join'
                    {
                        String message=fragments[0];
                        String pnumber=fragments[1];

                        if(message.equalsIgnoreCase("Join") && myPort.equalsIgnoreCase("11108"))  // If AVD0 - emulator 5554 receives join request do the following
                        {
                            Node newnode=new Node();

                            int portNumber=Integer.parseInt(pnumber);   // Convert port number--> 5554 to 11108 and store in new node
                            portNumber=portNumber*2;
                            newnode.setpNumber(String.valueOf(portNumber));

                            newnode.setNode_id(genHash(pnumber));    // set the node id

                            Log.i("Server Tsk: ","Generated hash for "+pnumber+": "+newnode.getNode_id());

                            list.add(newnode);                                // add node in the list and sort it
                            Collections.sort(list,new HashCodeComparator());

                            for(int i=0;i<list.size();i++)
                            {
                                if(list.size()==1)
                                {
                                    list.get(i).setPredecesssor(list.get(i).getNode_id());   // set predecessor and successor of first node
                                    list.get(i).setSuccessor(list.get(i).getNode_id());

                                    list.get(i).setPredeccessor_pno(list.get(i).getpNumber());
                                    list.get(i).setSuccessor_pno(list.get(i).getpNumber());
                                }
                                else if(list.size()==2)                                     // set predecessor and successor of first 2 nodes NOTE: Remember CHORD is a Circular Linked List
                                {
                                    if(i==0)
                                    {
                                        list.get(i).setPredecesssor(list.get(i+1).getNode_id());
                                        list.get(i).setSuccessor(list.get(i+1).getNode_id());

                                        list.get(i).setPredeccessor_pno(list.get(i+1).getpNumber());
                                        list.get(i).setSuccessor_pno(list.get(i+1).getpNumber());
                                    }
                                    else if(i==1)
                                    {
                                        list.get(i).setPredecesssor(list.get(i-1).getNode_id());
                                        list.get(i).setSuccessor(list.get(i-1).getNode_id());

                                        list.get(i).setPredeccessor_pno(list.get(i-1).getpNumber());
                                        list.get(i).setSuccessor_pno(list.get(i-1).getpNumber());
                                    }
                                }
                                else                                                     // set predecessors and successors for all elements greater than or equal to 3
                                {
                                    if(i==0)
                                    {
                                        list.get(i).setPredecesssor(list.getLast().getNode_id());
                                        list.get(i).setSuccessor(list.get(i+1).getNode_id());

                                        list.get(i).setPredeccessor_pno(list.getLast().getpNumber());
                                        list.get(i).setSuccessor_pno(list.get(i+1).getpNumber());
                                    }
                                    else if(i==(list.size()-1))
                                    {
                                        list.get(i).setPredecesssor(list.get(i-1).getNode_id());
                                        list.get(i).setSuccessor(list.getFirst().getNode_id());

                                        list.get(i).setPredeccessor_pno(list.get(i-1).getpNumber());
                                        list.get(i).setSuccessor_pno(list.getFirst().getpNumber());
                                    }
                                    else
                                    {
                                        list.get(i).setPredecesssor(list.get(i-1).getNode_id());
                                        list.get(i).setSuccessor(list.get(i+1).getNode_id());

                                        list.get(i).setPredeccessor_pno(list.get(i-1).getpNumber());
                                        list.get(i).setSuccessor_pno(list.get(i+1).getpNumber());
                                    }
                                }

                            }

                            for(int i=0;i<list.size();i++)
                            {
                                Log.i("Server tsk: ","Node_id: "+list.get(i).getNode_id()+":"+"Predecessor: "+list.get(i).getPredecesssor()+":"+"Successor: "+list.get(i).getSuccessor());
                            }
                            for(int i=0;i<list.size();i++)
                            {
                                if(list.get(i).getpNumber().equals("11108"))         // If the emulator is 5554 itself, don't broadcast it, set the value there itself because a server socket can't connect to a server socket of itself when it is already connected to client
                                {
                                    node.setpNumber(list.get(i).getpNumber());
                                    node.setNode_id(list.get(i).getNode_id());
                                    node.setPredecesssor(list.get(i).getPredecesssor());
                                    node.setSuccessor(list.get(i).getSuccessor());
                                    node.setPredeccessor_pno(list.get(i).getPredeccessor_pno());
                                    node.setSuccessor_pno(list.get(i).getSuccessor_pno());

                                    Log.i("Final node specs: ",node.getpNumber()+" "+node.getNode_id()+" "+node.getPredecesssor()+" "+node.getSuccessor()+" "+node.getPredeccessor_pno()+" "+node.getSuccessor_pno());

                                    continue;  // skip the broadcast for 5554
                                }

                                Socket sock = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(list.get(i).getpNumber()));  // for all other emulators, broadcast the successors and predecessors
                                OutputStreamWriter osw1 = new OutputStreamWriter(sock.getOutputStream());
                                BufferedWriter bwServer1=new BufferedWriter(osw1);
                                bwServer1.write(list.get(i).getpNumber()+":"+list.get(i).getNode_id()+":"+list.get(i).getPredecesssor()+":"+list.get(i).getSuccessor()+":"+list.get(i).getPredeccessor_pno()+":"+list.get(i).getSuccessor_pno());
                                bwServer1.newLine();          // we add a new line so that while writing bufferedwriter can write the entire line and then flush it
                                bwServer1.flush();

                                InputStreamReader istream1 = new InputStreamReader(sock.getInputStream());  // wait till other server tasks send "PA3 OK" acknowledgment
                                BufferedReader brClient1 = new BufferedReader(istream1);

                                String readLine="";
                                readLine=brClient1.readLine();

                                if (readLine.contains("PA3 OK"))
                                {
                                    Log.i(TAG,"Server Communication Successful..");
                                    sock.close();
                                }

                            }
                        }
                    }
                    else if(fragments.length==6)   // if a server receives the broadcast msg (msg content: - node id,port number,predecessors,successors) set it as the node variable's attributes.. Set all the successors and predecessors
                    {
                        Log.i("Fragments>2:","Working");
                        Log.i("Fragments:",fragments[0]+" "+fragments[1]+" "+fragments[2]+" "+fragments[3]);
                        node.setpNumber(fragments[0]);
                        node.setNode_id(fragments[1]);
                        node.setPredecesssor(fragments[2]);
                        node.setSuccessor(fragments[3]);
                        node.setPredeccessor_pno(fragments[4]);
                        node.setSuccessor_pno(fragments[5]);

                        Log.i("Final node specs: ",node.getpNumber()+" "+node.getNode_id()+" "+node.getPredecesssor()+" "+node.getSuccessor()+" "+node.getPredeccessor_pno()+" "+node.getSuccessor_pno());
                    }

                    else if(fragments.length==4)  // Insert request from predecessor to current node
                    {
                        Log.i("In Server Insert: ","Working. Insert called on current node");
                        String msg=fragments[0];
                        String pred_pnumber=fragments[1];
                        String key=fragments[2];
                        String key_val=fragments[3];

                        Log.i("Fragments: ",msg+" "+pred_pnumber+" "+key+" "+key_val);

                        String genKey="";
                        Log.i("Key received: ",key);
                        try{
                            genKey=genHash(key);
                            Log.i("Key hash: ",genKey);
                        }
                        catch (Exception e)
                        {
                            e.printStackTrace();
                        }

                        // Find out if its a boundary node

                        int value=node.getNode_id().compareTo(node.getPredecesssor());   // predecessor is greater than current nodeid i.e its a boundary node
                        if(value<0)
                        {
                            int val=genKey.compareTo(node.getNode_id());        // compare the key with current node_id
                            int val2=genKey.compareTo(node.getPredecesssor());  // compare the key with the current node's predecessor
                            if(val<=0 || val2>0)                              // if the value is greater than predecessor or if it is less than or equal to the current node_id
                            {
                                // Using SqLite database to store the (key,value) pairs

                                SqliteDB db=new SqliteDB(getContext()); // Android standard for SQLiteOpenHelper class

                                SQLiteDatabase sqlDb=db.getWritableDatabase(); // To get a writeable database we create an object of type SQLDatabase

                                //  sqlDb.insert("database",null,values);

                                //------- insertWithOnConflict(table_name,null field,ContentValues,Conflict_Algorithm)----------

                                ContentValues cv=new ContentValues();
                                cv.put(KEY_FIELD,key);
                                cv.put(VALUE_FIELD,key_val);

                                sqlDb.insertWithOnConflict("database",null,cv,SQLiteDatabase.CONFLICT_REPLACE);  // To perform insertion and on Conflict i.e incase of duplicate records the old records are replaced by new ones.

                                sqlDb.close();   // Close the DB on insertion

                                Log.v("Inserting M ", cv.toString());
                            }
                            else if(val>0||(val<0 && val2<0))
                            {
                                Socket skt;
                                try
                                {
                                    skt= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(node.getSuccessor_pno()));
                                    OutputStreamWriter os = new OutputStreamWriter(skt.getOutputStream());
                                    BufferedWriter bwriter = new BufferedWriter(os);
                                    String mesg="Insert";


                                    bwriter.write(mesg+":"+node.getpNumber()+":"+key+":"+key_val);  // send insert, port number of current node and key.
                                    bwriter.newLine();                             // We add a new line so that while writing bufferedwriter can write the entire line and then flush it
                                    bwriter.flush();
                                    Log.i("Insert Fwd","Msg sent to Successor");

                                    InputStreamReader isstream = new InputStreamReader(skt.getInputStream());
                                    BufferedReader brC = new BufferedReader(isstream);

                                    String readLine="";
                                    readLine=brC.readLine();

                                    if (readLine.contains("PA3 OK"))
                                    {
                                        Log.i(TAG,"Successor replied.Insert Communication Successful..");
                                        skt.close();
                                    }

                                }catch (Exception e)
                                {
                                    e.printStackTrace();
                                }

                            }

                        }
                        else  // predecessor is less than current node id
                        {
                            int val=genKey.compareTo(node.getNode_id());        // compare the key with current node_id
                            int val2=genKey.compareTo(node.getPredecesssor());  // compare the key with the current node's predecessor

                            if(val<=0 && val2>0)  // notice the '&&' operator. It means both the conditions should be satisfied.
                            {
                                SqliteDB db=new SqliteDB(getContext()); // Android standard for SQLiteOpenHelper class

                                SQLiteDatabase sqlDb=db.getWritableDatabase(); // To get a writeable database we create an object of type SQLDatabase

                                //  sqlDb.insert("database",null,values);

                                //------- insertWithOnConflict(table_name,null field,ContentValues,Conflict_Algorithm)----------

                                ContentValues cv=new ContentValues();
                                cv.put(KEY_FIELD,key);
                                cv.put(VALUE_FIELD,key_val);

                                sqlDb.insertWithOnConflict("database",null,cv,SQLiteDatabase.CONFLICT_REPLACE);  // To perform insertion and on Conflict i.e incase of duplicate records the old records are replaced by new ones.

                                sqlDb.close();   // Close the DB on insertion

                                Log.v("Inserting M ",cv.toString());
                            }
                            else if(val>0||(val<0 && val2<0))
                            {
                                Socket skt;
                                try
                                {
                                    skt= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(node.getSuccessor_pno()));
                                    OutputStreamWriter os = new OutputStreamWriter(skt.getOutputStream());
                                    BufferedWriter bwriter = new BufferedWriter(os);
                                    String mesg="Insert";


                                    bwriter.write(mesg+":"+node.getpNumber()+":"+key+":"+key_val);  // send insert, port number of current node and key.
                                    bwriter.newLine();                             // We add a new line so that while writing bufferedwriter can write the entire line and then flush it
                                    bwriter.flush();
                                    Log.i("Insert Fwd","Msg sent to Successor");

                                    InputStreamReader isstream = new InputStreamReader(skt.getInputStream());
                                    BufferedReader brC = new BufferedReader(isstream);

                                    String readLine="";
                                    readLine=brC.readLine();

                                    if (readLine.contains("PA3 OK"))
                                    {
                                        Log.i(TAG,"Successor replied.Insert Communication Successful..");
                                        skt.close();
                                    }

                                }catch (Exception e)
                                {
                                    e.printStackTrace();
                                }

                            }
                        }

                    }
                    else if(fragments.length==3)     // single object query from predecessor to the current node
                    {
                        String msg=fragments[0];
                        String pred_pnumber=fragments[1];
                        String selection=fragments[2];

                        String genSelection="";
                        Log.i("In Query S: ",selection);
                        Log.i("Fragments: ",msg+" "+pred_pnumber+" "+selection);
                        try{
                            genSelection=genHash(selection);                 // hash the key to be searched
                            Log.i("Selection (key) hash: ",genSelection);
                        }
                        catch (Exception e)
                        {
                            e.printStackTrace();
                        }

                        int value=node.getNode_id().compareTo(node.getPredecesssor());  // if predecessor is greater than current node
                        if(value<0)
                        {
                            int val=genSelection.compareTo(node.getNode_id());        // compare the key with current node_id
                            int val2=genSelection.compareTo(node.getPredecesssor());  // compare the key with the current node's predecessor
                            if(val<=0 || val2>0)                              // if the value is greater than predecessor or if it is less than or equal to the current node_id
                            {
                                Log.i("Querying 1 key-val","Current Node");
                                SqliteDB dbRead=new SqliteDB(getContext()); //Android standard

                                SQLiteDatabase sqlDBRead=dbRead.getReadableDatabase(); // A readable database to perform a query

                                String[] selection1={selection};

                                cursor=sqlDBRead.query("database",null,"key=?",selection1,null,null,null,null); //query method returns a cursor which can be passed to the UI element
                                cursor.moveToFirst(); // move cursor to the first row, because initially cursor is at index '-1'
                                Log.i("Cursor count: ",cursor.getCount()+" ");
                            }
                            else if(val>0||(val<0 && val2<0))
                            {
                                Socket skt;
                                try
                                {
                                    skt= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(node.getSuccessor_pno()));
                                    OutputStreamWriter os = new OutputStreamWriter(skt.getOutputStream());
                                    BufferedWriter bwriter = new BufferedWriter(os);
                                    String mesg="Query";

                                    bwriter.write(mesg+":"+node.getpNumber()+":"+selection);  // send insert, port number of current node and contentvalue.
                                    bwriter.newLine();                             // We add a new line so that while writing bufferedwriter can write the entire line and then flush it
                                    bwriter.flush();
                                    Log.i("Query Fwd: ","Msg sent to Successor");

                                    InputStreamReader isstream = new InputStreamReader(skt.getInputStream());
                                    BufferedReader brC = new BufferedReader(isstream);

                                    String readLine="";
                                    readLine=brC.readLine();

                                    String[] f=readLine.split(":");
                                    Log.i("Received key-val from S",readLine);

                                    if(f.length==3)
                                    {
                                        String incomingKey=f[0];
                                        String incomingValue=f[1];
                                        String ack=f[2];

                                        SqliteDB dbnew=new SqliteDB(getContext());
                                        SQLiteDatabase sqldb2= dbnew.getWritableDatabase();
                                        ContentValues cv=new ContentValues();
                                        cv.put(KEY_FIELD,incomingKey);
                                        cv.put(VALUE_FIELD,incomingValue);
                                        sqldb2.insertWithOnConflict("cursortable",null,cv,SQLiteDatabase.CONFLICT_REPLACE);
                                        sqldb2.close();
                                        Log.v("Inserting in Qry Tbl: ",cv.toString());

                                        SqliteDB dbread2=new SqliteDB(getContext());
                                        SQLiteDatabase sqlDBRead2=dbread2.getReadableDatabase();
                                        String[] select={incomingKey};
                                        cursor=sqlDBRead2.query("cursortable",null,"key=?",select,null,null,null,null);
                                        cursor.moveToFirst();
                                        Log.i("Final Cursor: ",cursor.getString(0)+" "+cursor.getString(1));
                                    }

                                    if (readLine.contains("PA3 OK"))
                                    {
                                        Log.i(TAG,"Successor replied. Single Query Communication Successful..");
                                        skt.close();
                                    }

                                }catch (Exception e)
                                {
                                    e.printStackTrace();
                                }

                            }

                        }
                        else
                        {
                            int val=genSelection.compareTo(node.getNode_id());        // compare the key with current node_id
                            int val2=genSelection.compareTo(node.getPredecesssor());  // compare the key with the current node's predecessor

                            if(val<=0 && val2>0)  // notice the '&&' operator. It means both the conditions should be satisfied. key should be greater than predecessor and less than or equal to current node
                            {
                                Log.i("Querying 1 key-val","Current Node");
                                SqliteDB dbRead=new SqliteDB(getContext()); //Android standard

                                SQLiteDatabase sqlDBRead=dbRead.getReadableDatabase(); // A readable database to perform a query

                                String[] selection1={selection};

                                cursor=sqlDBRead.query("database",null,"key=?",selection1,null,null,null,null); //query method returns a cursor which can be passed to the UI element
                                cursor.moveToFirst();  // move the cursor to first row, because initially cursor is at index '-1'
                                Log.i("Cursor count: ",cursor.getCount()+" ");
                            }
                            else if(val>0||(val<0 && val2<0))  // If key is greater than the current node or if the key is less than current as well as predecessor node, fwd it
                            {
                                Socket skt;
                                try
                                {
                                    skt= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(node.getSuccessor_pno()));
                                    OutputStreamWriter os = new OutputStreamWriter(skt.getOutputStream());
                                    BufferedWriter bwriter = new BufferedWriter(os);
                                    String mesg="Query";

                                    bwriter.write(mesg+":"+node.getpNumber()+":"+selection);  // send insert, port number of current node and contentvalue.
                                    bwriter.newLine();                             // We add a new line so that while writing bufferedwriter can write the entire line and then flush it
                                    bwriter.flush();
                                    Log.i("Query Fwd: ","Msg sent to Successor");

                                    InputStreamReader isstream = new InputStreamReader(skt.getInputStream());
                                    BufferedReader brC = new BufferedReader(isstream);

                                    String readLine="";
                                    readLine=brC.readLine();

                                    String[] f=readLine.split(":");
                                    Log.i("Received key-val from S",readLine);

                                    if(f.length==3)
                                    {
                                        String incomingKey=f[0];
                                        String incomingValue=f[1];
                                        String ack=f[2];

                                        SqliteDB dbnew=new SqliteDB(getContext());
                                        SQLiteDatabase sqldb2= dbnew.getWritableDatabase();
                                        ContentValues cv=new ContentValues();
                                        cv.put(KEY_FIELD,incomingKey);
                                        cv.put(VALUE_FIELD,incomingValue);
                                        sqldb2.insertWithOnConflict("cursortable",null,cv,SQLiteDatabase.CONFLICT_REPLACE);
                                        sqldb2.close();
                                        Log.v("Inserting in Qry Tbl: ",cv.toString());

                                        SqliteDB dbread2=new SqliteDB(getContext());
                                        SQLiteDatabase sqlDBRead2=dbread2.getReadableDatabase();
                                        String[] select={incomingKey};
                                        cursor=sqlDBRead2.query("cursortable",null,"key=?",select,null,null,null,null);
                                        cursor.moveToFirst();
                                        Log.i("Final Cursor: ",cursor.getString(0)+" "+cursor.getString(1));
                                    }

                                    if (readLine.contains("PA3 OK"))
                                    {
                                        Log.i(TAG,"Successor replied. Single Query Communication Successful..");
                                        skt.close();
                                    }

                                }catch (Exception e)
                                {
                                    e.printStackTrace();
                                }

                            }
                        }

                    }

                    //-------------------------------------* Query code --------------------------------------------------------------------------------------------//

                    else if(fragments.length==2 && fragments[0].equalsIgnoreCase("*QueryRequest"))   // * Query request to node from predecessor
                    {
                        String m=fragments[0]; // msg '*QueryRequest'
                        String pred=fragments[1]; // predecessor's hash value

                        if(node.getNode_id().equalsIgnoreCase(pred))  // Reached the final server. Backtrack from here and send cursors
                        {
                            Log.i("Final server","Query * request");
                            String strg="";
                            SqliteDB dbread2=new SqliteDB(getContext());
                            SQLiteDatabase sqlDBRead2=dbread2.getReadableDatabase();
                            cursor=sqlDBRead2.query("database",null,null,null,null,null,null,null);
                            if(cursor.getCount()!=0)                                         // Not an empty cursor
                            {
                                cursor.moveToFirst();
                                while(!cursor.isAfterLast())
                                {
                                    if(!cursor.isLast())
                                    {
                                        String stg=cursor.getString(0)+":"+cursor.getString(1)+":";
                                        strg=strg+stg;
                                        Log.i("Stg content: ",stg);
                                        Log.i("Strg content",strg);
                                    }
                                    else if(cursor.isLast())
                                    {
                                        String stg=cursor.getString(0)+":"+cursor.getString(1);
                                        strg=strg+stg;
                                        Log.i("Stg content: ",stg);
                                        Log.i("Strg content",strg);
                                    }
                                    cursor.moveToNext();
                                }                                        // cursor in the form of string obtained
                                Log.i("String Fwding to P: ",strg);

                                OutputStreamWriter osw = new OutputStreamWriter(s.getOutputStream());  // send the data obtained from the current node to predecessor
                                BufferedWriter bwServer=new BufferedWriter(osw);
                                bwServer.write(strg);
                                bwServer.newLine();         // we add a new line so that while writing, bufferedwriter can write the entire line and then flush it
                                bwServer.flush();

                                s.close();

                            }
                            else  // if this node also has no data, it will send a msg 'Empty:PA3 OK' to its predecessor
                            {
                                Log.i("No data Here","Sending empty message back t Predecessor");
                                OutputStreamWriter osw = new OutputStreamWriter(s.getOutputStream());
                                BufferedWriter bwServer=new BufferedWriter(osw);
                                bwServer.write("Empty"+":"+"PA3 OK");
                                bwServer.newLine();         // we add a new line so that while writing, bufferedwriter can write the entire line and then flush it
                                bwServer.flush();

                                s.close();
                            }
                        }
                        else  // the current node is not the final server
                        {
                            Socket st;
                            try
                            {
                                st= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(node.getSuccessor_pno()));  // fwd request ahead and wait
                                OutputStreamWriter o=new OutputStreamWriter(st.getOutputStream());
                                BufferedWriter b=new BufferedWriter(o);
                                b.write(m+":"+pred);
                                b.newLine();
                                b.flush();
                                Log.i("Msg: ",m+":"+pred);
                                Log.i("Query Fwd: ","Msg sent to Successor");

                                InputStreamReader i=new InputStreamReader(st.getInputStream());
                                BufferedReader brc=new BufferedReader(i);

                                String readLine="";
                                readLine=brc.readLine();

                                Log.i("Received String from S",readLine);

                                String[] f=readLine.split(":");  // after getting a reply

                                if(f.length==2 && f[0].equalsIgnoreCase("Empty"))  // if an empty cursor is returned by the successor
                                {

                                    if (readLine.contains("PA3 OK"))   // close the socket for the successor
                                    {
                                        Log.i(TAG,"Successor replied Empty. * Query Communication Successful..");
                                        st.close();
                                    }

                                    String strg="";
                                    SqliteDB dbread2=new SqliteDB(getContext());
                                    SQLiteDatabase sqlDBRead2=dbread2.getReadableDatabase();
                                    cursor=sqlDBRead2.query("database",null,null,null,null,null,null,null);
                                    if(cursor.getCount()!=0)
                                    {
                                        cursor.moveToFirst();
                                        while(!cursor.isAfterLast())
                                        {
                                            if(!cursor.isLast())
                                            {
                                                String stg=cursor.getString(0)+":"+cursor.getString(1)+":";
                                                strg=strg+stg;
                                                Log.i("Stg content: ",stg);
                                                Log.i("Strg content",strg);
                                            }
                                            else if(cursor.isLast())
                                            {
                                                String stg=cursor.getString(0)+":"+cursor.getString(1);
                                                strg=strg+stg;
                                                Log.i("Stg content: ",stg);
                                                Log.i("Strg content",strg);
                                            }
                                            cursor.moveToNext();
                                        }                                        // cursor in the form of string obtained
                                        Log.i("String Fwding to P: ",strg);

                                        OutputStreamWriter osw = new OutputStreamWriter(s.getOutputStream());  // send the data obtained from the current node to predecessor
                                        BufferedWriter bwServer=new BufferedWriter(osw);
                                        bwServer.write(strg);
                                        bwServer.newLine();         // we add a new line so that while writing, bufferedwriter can write the entire line and then flush it
                                        bwServer.flush();

                                        s.close();

                                    }
                                    else  // if this node also has no data, it will send a msg 'Empty:PA3 OK' to its predecessor
                                    {
                                        Log.i("No data Here","Sending empty message back t Predecessor");
                                        OutputStreamWriter osw = new OutputStreamWriter(s.getOutputStream());
                                        BufferedWriter bwServer=new BufferedWriter(osw);
                                        bwServer.write("Empty"+":"+"PA3 OK");
                                        bwServer.newLine();         // we add a new line so that while writing, bufferedwriter can write the entire line and then flush it
                                        bwServer.flush();

                                        s.close();
                                    }


                                }
                                else  // if a cursor is returned by the successor
                                {
                                    st.close(); // close the socket with the successor since we have received the cursor

                                    String strg=readLine;
                                    SqliteDB dbread2=new SqliteDB(getContext());
                                    SQLiteDatabase sqlDBRead2=dbread2.getReadableDatabase();
                                    cursor=sqlDBRead2.query("database",null,null,null,null,null,null,null);

                                    if(cursor.getCount()!=0)              // if the current cursor has values, append them to the current string and send them back to predecessor
                                    {
                                        cursor.moveToFirst();
                                        while(!cursor.isAfterLast())
                                        {
                                            String stg=":"+cursor.getString(0)+":"+cursor.getString(1);
                                            strg=strg+stg;
                                            cursor.moveToNext();
                                        }
                                        Log.i("String Fwding to P: ",strg);

                                        OutputStreamWriter osw = new OutputStreamWriter(s.getOutputStream());  // send the appended cursor values to predecessor
                                        BufferedWriter bwServer=new BufferedWriter(osw);
                                        bwServer.write(strg);
                                        bwServer.newLine();         // we add a new line so that while writing, bufferedwriter can write the entire line and then flush it
                                        bwServer.flush();

                                        s.close();
                                    }
                                    else   // cursor is null, so the data that has been read by this node from successor will be sent as it is to predecessor
                                    {
                                        Log.i("No data Here","Sending empty message back t Predecessor");
                                        OutputStreamWriter osw = new OutputStreamWriter(s.getOutputStream());  // send the appended cursor values to predecessor
                                        BufferedWriter bwServer=new BufferedWriter(osw);
                                        bwServer.write(readLine);
                                        bwServer.newLine();         // we add a new line so that while writing, bufferedwriter can write the entire line and then flush it
                                        bwServer.flush();

                                        s.close();
                                    }
                                }



                            }
                            catch (Exception e)
                            {
                                e.printStackTrace();
                            }
                        }
                    }
                    if(fragments.length==3 && cursor!=null)  // If query operation, post a different acknowledgment
                    {
                        Log.i("Query: ","Key: "+cursor.getString(0)+"Value: "+cursor.getString(1));  // When cursor is not empty, send the cursor value back
                        OutputStreamWriter osw = new OutputStreamWriter(s.getOutputStream());
                        BufferedWriter bwServer=new BufferedWriter(osw);
                        bwServer.write(cursor.getString(0)+":"+cursor.getString(1)+":"+"PA3 OK");
                        bwServer.newLine();         // we add a new line so that while writing, bufferedwriter can write the entire line and then flush it
                        bwServer.flush();

                        s.close();
                    }
                    else if((fragments.length==2 && fragments[0].equalsIgnoreCase("Join"))||fragments.length==6||fragments.length==4||fragments.length==3 && cursor==null)
                    {
                        Log.i("Sending ACK","PA3 OK");
                        OutputStreamWriter osw = new OutputStreamWriter(s.getOutputStream());
                        BufferedWriter bwServer=new BufferedWriter(osw);
                        bwServer.write("PA3 OK");
                        bwServer.newLine();         // we add a new line so that while writing, bufferedwriter can write the entire line and then flush it
                        bwServer.flush();

                        s.close();
                    }

                }
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }

            return null;

        }

    }
    private String genHash(String input) throws NoSuchAlgorithmException
    {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

}


class HashCodeComparator implements Comparator<Node>
{
    @Override
    public int compare(Node node, Node t1) {
        int value=node.getNode_id().compareTo(t1.getNode_id());

        if(value<0)
        {
            return -1;
        }
        else if (value>0)
        {
            return 1;
        }

        return 0;
    }
}