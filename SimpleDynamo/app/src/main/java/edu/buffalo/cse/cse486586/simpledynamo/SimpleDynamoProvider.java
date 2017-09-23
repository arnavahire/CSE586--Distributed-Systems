package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {
	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	static final String REMOTE_PORT0 = "11108";
	static final String REMOTE_PORT1 = "11112";
	static final String REMOTE_PORT2 = "11116";
	static final String REMOTE_PORT3 = "11120";
	static final String REMOTE_PORT4 = "11124";
	static final int SERVER_PORT = 10000;
	static String myPort;
	static String myEmulatorNo;
	private static final String KEY_FIELD = "key";
	private static final String VALUE_FIELD = "value";
	private static final String OWNER_FIELD="owner";
	LinkedList<String> list=new LinkedList<String>();
	static Map<String,String> map;
	static volatile Boolean flag=true;
	private final Lock lock=new ReentrantLock();


	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

//------------------------------------------------------ SETTING CONTENT PROVIDER-----------------------------------------------------------------------------//

	Uri contentUri=buildUri("content","edu.buffalo.cse.cse486586.simpledynamo.provider");  // defining the content provider

	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

//-------------------------------------------------------------------------------DELETE-----------------------------------------------------------------------------------------------//

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

//--------------------------------------------------------Query other AVD-------------------------------------------------------------------------------------------------------------//

	public Cursor queryOtherAVD(String selection, int avdIndex,String distinguishingMsg)
	{
		Cursor cursor=null;

		try{
			String avdHash=list.get(avdIndex);
			Log.i("avd","Hash"+" "+avdHash);
			String avd_emulator_number="";
			if(map.containsKey(avdHash))
			{
				avd_emulator_number=map.get(avdHash);    // we will get the destination port number
				Log.i("AVD Emulator Port",avd_emulator_number);
			}
			Log.i("OUt","IF condition");
			String avdPort=String.valueOf(Integer.parseInt(avd_emulator_number)*2);  // ex: 5554 to 11108
			Log.i("AVD Port",avdPort);

			String mesg=selection+":"+distinguishingMsg;

			Log.w("Querying "+avd_emulator_number,mesg);

			Socket skt;
			skt= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(avdPort));
			OutputStreamWriter new_os = new OutputStreamWriter(skt.getOutputStream());
			BufferedWriter new_bwriter = new BufferedWriter(new_os);

			new_bwriter.write(mesg);  // send insert, port number of current node and contentvalue.
			new_bwriter.newLine();                             // We add a new line so that while writing bufferedwriter can write the entire line and then flush it
			new_bwriter.flush();
			Log.w("Single Query Fwd","Msg sent to "+avd_emulator_number);

			InputStreamReader new_isstream = new InputStreamReader(skt.getInputStream());
			BufferedReader new_brC = new BufferedReader(new_isstream);

			String new_readLine="";
			new_readLine=new_brC.readLine();

			if(new_readLine==null)
			{
				Log.w("Connection","Terminated. "+avd_emulator_number+" is down.");
				Log.i("Returning","Empty cursor");
				cursor=performQuery("database","RandomString");
				skt.close();
			}
			else
			{
				String[] fragments=new_readLine.split(":");
				Log.w("Received key-val","From ServerTask @ "+avd_emulator_number+" "+new_readLine);

				if(fragments.length==4)
				{
					String incomingKey=fragments[0];
					String incomingValue=fragments[1];
					String owner=fragments[2];
					String ack=fragments[3];

					Log.v("Inserting in Qry Tbl: ","Key:"+incomingKey+" Value:"+incomingValue);
					performInsert("cursortable",incomingKey,incomingValue,owner,"Insert Operation Called (Cursor table)");

					cursor=performQuery("cursortable",incomingKey);
					Log.i("Final Cursor: ",cursor.getString(0)+" "+cursor.getString(1)+" "+cursor.getString(2));
				}

				if(new_readLine.contains("PA4 OK"))
				{
					Log.i(TAG,"AVD replied. Single Query Communication Successful..");
					skt.close();
				}
			}

		}
		catch (Exception e)
		{
			e.printStackTrace();
		}

		return cursor;
	}

//----------------------------------------------------QUOROM (For Single Value Query)----------------------------------------------------------------------------------------------------------------------------//

	public Cursor performQuorum(String selection,String EmulatorNo)
	{
		Cursor cursor=null;
		Cursor cursor1=null,cursor2=null,cursor3=null;

		Log.w("Inside","Quorum");
		Log.w("Quorum","Query performed @ "+EmulatorNo);
		String genSelection="";
		Log.i("In Query: ",selection);

		try {
			genSelection = genHash(selection);                 // hash the key to be searched
			Log.i("Selection (key) hash: ", genSelection);

			int list_index = keyCompare(genSelection);  // perform key comparison to check to which AVD it belongs

			Log.i("List index", String.valueOf(list_index));  // we will have the replicas' indices and the co-ordinator index in replica_index_no,list_index
			int replica_1_index=0,replica_2_index=0;
			for(int i=0;i<list.size();i++)
			{
				if(i==list_index && list_index==(list.size()-2))   // If Index=3 then its replica's will be 4,0
				{
					replica_1_index=(list.size()-1);
					replica_2_index=0;
					Log.i("Replication","Index:"+i+" Replica1 "+replica_1_index+" Replica2 "+replica_2_index);
				}
				else if(i==list_index && list_index==(list.size()-1))  // If Index=4 then its replica's will be 0,1
				{
					replica_1_index=0;
					replica_2_index=1;
					Log.i("Replication","Index:"+i+" Replica1 "+replica_1_index+" Replica2 "+replica_2_index);
				}
				else if(i==list_index)  /// If Index=0,1,2 then its replica's will be i+1,i+2
				{
					replica_1_index=i+1;
					replica_2_index=i+2;
					Log.i("Replication","Index:"+i+" Replica1 "+replica_1_index+" Replica2 "+replica_2_index);
				}
			}



			{
				String hash_value=list.get(list_index); // contains the hash_value of the node (owner) in which the respective key is stored
				String owner="";
				if(map.containsKey(hash_value))
				{
					owner=map.get(hash_value);    // we will get the owner's port number
				}

				String ownerPort=String.valueOf(Integer.parseInt(owner)*2);  // ex: 5554 to 11108

				cursor1=queryOtherAVD(selection,list_index,ownerPort);                  // Query Owner, Replica1 and Replica2 each time
				cursor2=queryOtherAVD(selection,replica_1_index,"QueryReplica");
				cursor3=queryOtherAVD(selection,replica_2_index,"QueryReplica");

				// here cursor1 can never be null, since this node is not failing, so check for cursor2 and cursor3

				if(cursor1.getCount()==0)
				{
					Log.w("Quorum","Original Node failed");
					Log.w("Quorum","Value from Replica 1 node:"+cursor2.getString(0)+" "+cursor2.getString(1)+" "+cursor2.getString(2));
					Log.w("Quorum","Value from Replica 2 node:"+cursor3.getString(0)+" "+cursor3.getString(1)+" "+cursor3.getString(2));
					String k2=cursor2.getString(0);
					String v2=cursor2.getString(1);
					String k3=cursor3.getString(0);
					String v3=cursor3.getString(1);

					if(k2.equalsIgnoreCase(k3) && v2.equalsIgnoreCase(v3))
					{
						cursor=cursor2;
					}
					else   // default cursor to be selected
					{
						cursor=cursor2;
					}
				}
				else if(cursor2.getCount()==0)
				{
					Log.w("Quorum","Replica 1 failed");
					Log.w("Quorum","Value from original node:"+cursor1.getString(0)+" "+cursor1.getString(1)+" "+cursor1.getString(2));
					Log.w("Quorum","Value from Replica 2 node:"+cursor3.getString(0)+" "+cursor3.getString(1)+" "+cursor3.getString(2));
					String k1=cursor1.getString(0);
					String v1=cursor1.getString(1);
					String k3=cursor3.getString(0);
					String v3=cursor3.getString(1);

					if(k1.equalsIgnoreCase(k3) && v1.equalsIgnoreCase(v3))
					{
						cursor=cursor1;
					}
					else   // default cursor to be selected
					{
						cursor=cursor1;
					}
				}
				else if(cursor3.getCount()==0)
				{
					Log.w("Quorum","Replica 2 failed");
					Log.w("Quorum","Value from original node:"+cursor1.getString(0)+" "+cursor1.getString(1)+" "+cursor1.getString(2));
					Log.w("Quorum","Value from Replica 1 node:"+cursor2.getString(0)+" "+cursor2.getString(1)+" "+cursor2.getString(2));
					String k1=cursor1.getString(0);
					String v1=cursor1.getString(1);
					String k2=cursor2.getString(0);
					String v2=cursor2.getString(1);

					if(k1.equalsIgnoreCase(k2) && v1.equalsIgnoreCase(v2))
					{
						cursor=cursor1;
					}
					else   // default cursor to be selected
					{
						cursor=cursor1;
					}
				}
				else if(cursor1.getCount()>0 && cursor2.getCount()>0 && cursor3.getCount()>0)
				{
					Log.w("Quorum","No AVD failed");
					Log.w("Quorum","Value from original node:"+cursor1.getString(0)+" "+cursor1.getString(1)+" "+cursor1.getString(2));
					Log.w("Quorum","Value from Replica 1 node:"+cursor2.getString(0)+" "+cursor2.getString(1)+" "+cursor2.getString(2));
					Log.w("Quorum","Value from Replica 2 node:"+cursor3.getString(0)+" "+cursor3.getString(1)+" "+cursor3.getString(2));

					String k1=cursor1.getString(0);
					String v1=cursor1.getString(1);

					String k2=cursor2.getString(0);
					String v2=cursor2.getString(1);
					String k3=cursor3.getString(0);
					String v3=cursor3.getString(1);

					LinkedList<String> valueList=new LinkedList<String>();

					valueList.add(v1);
					valueList.add(v2);
					valueList.add(v3);

					Map<String,Integer> valCountMap=new HashMap<String,Integer>();  // maintain a hashmap of different values for keys and their corresponding counts. Majority should be >=2

					for(int i=0;i<valueList.size();i++)
					{
						if(valCountMap.containsKey(valueList.get(i)))
						{
							int value=valCountMap.get(valueList.get(i));
							value++;
							valCountMap.put(valueList.get(i),value);
							Log.i("Local Map values",valueList.get(i)+" "+value);
						}
						else
						{
							valCountMap.put(valueList.get(i),1);
							Log.i("Local Map values",valueList.get(i)+" One");
						}
					}

					for(String key:valCountMap.keySet())
					{
						int val=valCountMap.get(key);
						if(val>=2)                         // Quorum majority
						{
							if(key.equalsIgnoreCase(v1))
							{
								Log.i("Match","Majority found 1");
								cursor=cursor1;
								break;
							}
							else if(key.equalsIgnoreCase(v2))
							{
								Log.i("Match","Majority found 2");
								cursor=cursor2;
								break;
							}
							else if(key.equalsIgnoreCase(v3))
							{
								Log.i("Match","Majority found 3");
								cursor=cursor3;
								break;
							}
						}
						else            // default key-val to consider would be that of the original AVD
						{
							cursor=cursor1;
							Log.i("Match","No majority found");
						}
					}
				}
			}
		}
		catch (NoSuchAlgorithmException e)
		{
			e.printStackTrace();
		}
		try {
			Log.w("Returning cursor",cursor.getString(0)+" "+cursor.getString(1)+" "+cursor.getString(2)+" hash(key):"+genHash(cursor.getString(0)));
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		MatrixCursor cur = new MatrixCursor(new String[]{"key","value","owner"});
		cur.addRow(new Object[]{cursor.getString(0),cursor.getString(1),cursor.getString(2)});

		return cur;
	}
//------------------------------------------------------------------------------PERFORM QUERY METHOD-----------------------------------------------------------------------------------//

	public Cursor performQuery(String tablename,String selection)             // not useful for * query
	{
		Cursor cursor1;

		SqliteDB dbRead=new SqliteDB(this.getContext()); //Android standard

		SQLiteDatabase sqlDBRead=dbRead.getReadableDatabase(); // A readable database to perform a query

		String[] selection1={selection};

		if(selection.equalsIgnoreCase("*"))
		{
			cursor1=sqlDBRead.query(tablename,null,null,null,null,null,null,null); //query method returns a cursor for all (*) key-value pairs
			Log.i("Cursor count: ",cursor1.getCount()+"");
		}
		else if(selection.equalsIgnoreCase("@"))
		{
			cursor1=sqlDBRead.query(tablename,null,null,null,null,null,null,null); //query method returns a cursor for all (@) key-value pairs on a single AVD
			Log.i("Cursor count: ",cursor1.getCount()+"");
		}
		else
		{
			cursor1=sqlDBRead.query(tablename,null,"key=?",selection1,null,null,null,null); //query method returns a cursor which can be passed to the UI element

			cursor1.moveToFirst();  // move the cursor to first row,because initially cursor is at index '-1'

			Log.i("Cursor count: ",cursor1.getCount()+" ");
		}

		return cursor1;

	}

//--------------------------------------------------------------------------------QUERY------------------------------------------------------------------------------------------------//

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
						String[] selectionArgs, String sortOrder) {
		Cursor cursor=null;
		synchronized(uri){

			if(selection.equalsIgnoreCase("@"))
			{
				Log.w("Query from grader","@ query");
				try {
					Socket st = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(myPort));
					OutputStreamWriter o=new OutputStreamWriter(st.getOutputStream());
					BufferedWriter b=new BufferedWriter(o);
					b.write("@QueryRequest"+":");
					b.newLine();
					b.flush();

					InputStreamReader isr=new InputStreamReader(st.getInputStream());
					BufferedReader br=new BufferedReader(isr);

					Log.i("Query Fwd to S: ","@QueryRequest"+":");

					String readLine="";
					readLine=br.readLine();

					String strg=readLine;
					Log.w("Received line: ",readLine);

					String[] splits=strg.split(":");           // Split this list of all the key-value pairs

					LinkedList<String> keyList=new LinkedList<String>();
					LinkedList<String> valueList=new LinkedList<String>();
					LinkedList<String> ownerList=new LinkedList<String>();


					int count=0;
					for(int x=0;x<splits.length;x++)
					{
						count++;
						if(count==1)
						{
							keyList.add(splits[x]);   // add all keys in 1 list
							Log.i("Key:",splits[x]);
						}
						else if(count==2)
						{
							valueList.add(splits[x]);  // add all values in other list
							Log.i("Value:",splits[x]);
						}
						else if(count==3)
						{
							ownerList.add(splits[x]);
							Log.i("Owner:",splits[x]);
							count=0;
						}
					}

					st.close();

					for(int x=0;x<keyList.size();x++)  // insert all the keys and values in a new table 'atcursortable'
					{
						String fKey=keyList.get(x);
						String fValue=valueList.get(x);
						String fOwner=ownerList.get(x);
						performInsert("atcursortable",fKey,fValue,fOwner,"Insert Operation Called (atcursortable)");
					}

					cursor=performQuery("atcursortable","@");  // perform query on this final atcursortable


				}
				catch(Exception e)
				{
					e.printStackTrace();
				}


			}
			else if(selection.equalsIgnoreCase("*"))   // retrieve all keys
			{
				Socket st=null;
				try{
					for(int i=0;i<5;i++)  // fetch the key-val pairs from other AVDs
					{
						if(i==0)
						{
							if(REMOTE_PORT0.equalsIgnoreCase(myPort))           // do not create a socket connection with yourself
							{
								continue;
							}
							else
							{
								st= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT0));
							}
						}
						if(i==1)
						{
							if(REMOTE_PORT1.equalsIgnoreCase(myPort))
							{
								continue;
							}
							else
							{
								st= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT1));
							}
						}
						if(i==2)
						{
							if(REMOTE_PORT2.equalsIgnoreCase(myPort))
							{
								continue;
							}
							else
							{
								st= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT2));
							}
						}
						if(i==3)
						{
							if(REMOTE_PORT3.equalsIgnoreCase(myPort))
							{
								continue;
							}
							else
							{
								st= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT3));
							}
						}
						if(i==4)
						{
							if(REMOTE_PORT4.equalsIgnoreCase(myPort))
							{
								continue;
							}
							else
							{
								st= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT4));
							}
						}

						OutputStreamWriter o=new OutputStreamWriter(st.getOutputStream());
						BufferedWriter b=new BufferedWriter(o);
						b.write("*QueryRequest"+":");
						b.newLine();
						b.flush();

						InputStreamReader isr=new InputStreamReader(st.getInputStream());
						BufferedReader br=new BufferedReader(isr);

						Log.i("Query Fwd to S: ","*QueryRequest"+":");

						String readLine="";
						readLine=br.readLine();

						if(readLine==null)  // if the connecting AVD has failed, do nothing
						{
							st.close();
							continue;
						}
						else               // if connecting AVD has not failed
						{
							String[] f=readLine.split(":");

							if(f.length==2 && f[0].equalsIgnoreCase("Empty"))  // if other node has no data
							{
								if (readLine.contains("PA4 OK"))   // close the socket for that node
								{
									Log.i(TAG,"Successor replied Empty. * Query Communication Successful..");
									st.close();
								}
								continue;
							}

							else           // if data returned
							{
								String strg=readLine;
								Log.w("Received line: ",readLine);

								String[] splits=strg.split(":");           // Split this list of all the key-value pairs

								LinkedList<String> keyList=new LinkedList<String>();
								LinkedList<String> valueList=new LinkedList<String>();
								LinkedList<String> ownerList=new LinkedList<String>();


								int count=0;
								for(int x=0;x<splits.length;x++)
								{
									count++;
									if(count==1)
									{
										keyList.add(splits[x]);   // add all keys in 1 list
										Log.i("Key:",splits[x]);
									}
									else if(count==2)
									{
										valueList.add(splits[x]);  // add all values in other list
										Log.i("Value:",splits[x]);
									}
									else if(count==3)
									{
										ownerList.add(splits[x]);
										Log.i("Owner:",splits[x]);
										count=0;
									}
								}

								for(int x=0;x<keyList.size();x++)  // insert all the keys and values in the database of the recovered AVD
								{
									String fKey=keyList.get(x);
									String fValue=valueList.get(x);
									String fOwner=ownerList.get(x);
									performInsert("starcursortable",fKey,fValue,fOwner,"Insert Operation Called (starcursortable)");
								}

							}
							st.close();
						}

					}

					cursor=performQuery("database","@"); // Get all the key-val pairs from DB of current AVD

					if(cursor.getCount()!=0)  // If the current AVD contains data then add it to the starcursortable
					{
						cursor.moveToFirst();
						while(!cursor.isAfterLast())
						{
							performInsert("starcursortable",cursor.getString(0),cursor.getString(1),cursor.getString(2),"Insert Operation Called (starcursortable)");
							cursor.moveToNext();
						}
					}

					cursor=performQuery("starcursortable","*");  // perform query on this final starcursortable
					cursor.moveToFirst();
					while(!cursor.isAfterLast())
					{
						Log.i("The Last Cursor: ",cursor.getString(0)+" "+cursor.getString(1));
						cursor.moveToNext();
					}

				}
				catch (Exception e)
				{
					e.printStackTrace();
				}

			}
			else          // Single value query
			{

				cursor=performQuorum(selection,myEmulatorNo);

			}
		}

		if(cursor==null||cursor.getCount()==0)
		{

		}
		else
		{
			MatrixCursor cur = new MatrixCursor(new String[]{"key","value"});
			cursor.moveToFirst();
			while(!cursor.isAfterLast())
			{
				Log.w("Query cursor",cursor.getString(0)+" "+cursor.getString(1));
				cur.addRow(new Object[]{cursor.getString(0),cursor.getString(1)});
				cursor.moveToNext();

			}

			return cur;
		}
		return cursor;

	}

//------------------------------------------------------------------------PERFORM INSERT METHOD-----------------------------------------------------------------------------------------//

	public void performInsert(String tableName,String keyToPut,String valueToPut,String Owner,String WhenInsertTakingPlace)
	{
		// Using SqLite database to store the (key,value) pairs

		SqliteDB db=new SqliteDB(getContext()); // Android standard for SQLiteOpenHelper class

		SQLiteDatabase sqlDb=db.getWritableDatabase(); // To get a writeable database we create an object of type SQLDatabase

		//------- insertWithOnConflict(table_name,null field,ContentValues,Conflict_Algorithm)----------

		ContentValues cv=new ContentValues();
		cv.put(KEY_FIELD,keyToPut);
		cv.put(VALUE_FIELD,valueToPut);
		cv.put(OWNER_FIELD,Owner);

		sqlDb.insertWithOnConflict(tableName,null,cv,SQLiteDatabase.CONFLICT_REPLACE);  // To perform insertion and on Conflict i.e incase of duplicate records the old records are replaced by new ones.

		sqlDb.close();   // Close the DB on insertion

		Log.w("Inserting ", WhenInsertTakingPlace+" "+cv.toString());
	}

//-------------------------------------------------------------------------------INSERT--------------------------------------------------------------------------------------------------------//

	@Override
	public Uri insert(Uri uri, ContentValues values){

		synchronized(uri){
			String key=(String)values.get(KEY_FIELD);
			String key_value=(String)values.get(VALUE_FIELD);
			String genKey="";
			Log.i("In Insert: ",key);
			try{
				genKey=genHash(key);
				Log.i("Key hash: ",genKey);

				int list_index=keyCompare(genKey);  // perform key comparison to check to which AVD it belongs

				Log.i("List index",String.valueOf(list_index));


				String hash_value=list.get(list_index); // contains the hash_value of the node (owner )in which the respective key will be stored
				String owner="";
				if(map.containsKey(hash_value))
				{
					owner=map.get(hash_value);    // we will get the owner's port number
				}

				String ownerPort=String.valueOf(Integer.parseInt(owner)*2);  // ex: 5554 to 11108

				String msg=key+":"+key_value+":"+owner;    // key-val-owner

				Log.i("Msg to other AVD: ",msg);

				Socket s=null;
				try{
					s= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(ownerPort));
					OutputStreamWriter os = new OutputStreamWriter(s.getOutputStream());
					BufferedWriter bwriter = new BufferedWriter(os);
					bwriter.write(msg);                              // send insert, port number of current node and contentvalue.
					bwriter.newLine();                             // We add a new line so that while writing bufferedwriter can write the entire line and then flush it
					bwriter.flush();
					Log.i("Insert Fwd","Msg sent to "+owner);

					InputStreamReader isstream = new InputStreamReader(s.getInputStream());
					BufferedReader brC = new BufferedReader(isstream);

					String readLine="";
					readLine=brC.readLine();

					if(readLine==null)               // incase a node fails, close the socket and insert the data in it's replica
					{
						Log.i(TAG,"Connection terminated");
						s.close();
					}

					else if(readLine.contains("PA4 OK"))
					{
						Log.i(TAG,"AVD replied. Insert Communication Successful..");
						s.close();
					}

					performReplication(list_index,"database",key,key_value);   // replicate the key-values on other replicas
				}
				catch (Exception e)
				{
					e.printStackTrace();

				}

			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
		}

		return uri;
	}

//--------------------------------------------------------------------------PERFORM REPLICATION METHOD-------------------------------------------------------------------------------------------//

	public void performReplication(int l_index,String tablename,String keyToPut,String valueToPut)
	{
		int replica_1_index=0,replica_2_index=0;

		for(int i=0;i<list.size();i++)
		{
			if(i==l_index && l_index==(list.size()-2))   // If Index=3 then its replica's will be 4,0
			{
				replica_1_index=(list.size()-1);
				replica_2_index=0;
				Log.i("Replication","Index:"+i+" Replica1 "+replica_1_index+" Replica2 "+replica_2_index);
			}
			else if(i==l_index && l_index==(list.size()-1))  // If Index=4 then its replica's will be 0,1
			{
				replica_1_index=0;
				replica_2_index=1;
				Log.i("Replication","Index:"+i+" Replica1 "+replica_1_index+" Replica2 "+replica_2_index);
			}
			else if(i==l_index)  /// If Index=0,1,2 then its replica's will be i+1,i+2
			{
				replica_1_index=i+1;
				replica_2_index=i+2;
				Log.i("Replication","Index:"+i+" Replica1 "+replica_1_index+" Replica2 "+replica_2_index);
			}
		}

//-----------------------------------------------Calculating the port which is originally intended to receive the key-value------------------------------------------------//

		String hash_value=list.get(l_index); // contains the hash_value of the node in which the respective key will be stored
		String owner="";
		if(map.containsKey(hash_value))
		{
			owner=map.get(hash_value);    // we will get the destination port number
		}

		String ownerPort=String.valueOf(Integer.parseInt(owner)*2);  // ex: 5554 to 11108
		Log.i("Intended Port: ",ownerPort);


//----------------------------------------------------------------------Sending data to replica 1-----------------------------------------------------------------------------//

		String replica1_hash_value=list.get(replica_1_index); // contains the hash_value of the node in which the respective key will be stored
		String replica1Emulator="";
		if(map.containsKey(replica1_hash_value))
		{
			replica1Emulator=map.get(replica1_hash_value);    // we will get the destination port number
		}

		String replica1Port=String.valueOf(Integer.parseInt(replica1Emulator)*2);  // ex: 5554 to 11108

		String msg=keyToPut+":"+valueToPut+":"+replica1Port+":"+owner+":"+tablename;   // Send (key:value:ReplicaPort:owner:Table which will store key-val) owner=Emulator No

		Log.i("Msg to Replica1: ",msg);

		Socket s;
		try{
			s= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(replica1Port));
			OutputStreamWriter os = new OutputStreamWriter(s.getOutputStream());
			BufferedWriter bwriter = new BufferedWriter(os);

			bwriter.write(msg);  // send insert, port number of current node and contentvalue.
			bwriter.newLine();                             // We add a new line so that while writing bufferedwriter can write the entire line and then flush it
			bwriter.flush();
			Log.i("Insert Fwd","Msg sent to Replica1");

			InputStreamReader isstream = new InputStreamReader(s.getInputStream());
			BufferedReader brC = new BufferedReader(isstream);

			String readLine="";
			readLine=brC.readLine();
			if(readLine==null)
			{
				Log.i(TAG,"Connection terminated");
				s.close();
			}

			else if(readLine.contains("PA4 OK"))
			{
				Log.i(TAG,"Replica1 replied. Insert Communication Successful..");
				s.close();
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();

		}

//---------------------------------------------------------------------------Sending data to replica2------------------------------------------------------------------------------------//

		String replica2_hash_value=list.get(replica_2_index); // contains the hash_value of the node in which the respective key will be stored
		String replica2Emulator="";
		if(map.containsKey(replica2_hash_value))
		{
			replica2Emulator=map.get(replica2_hash_value);    // we will get the destination port number
		}

		String replica2Port=String.valueOf(Integer.parseInt(replica2Emulator)*2);  // ex: 5554 to 11108

		String msg2=keyToPut+":"+valueToPut+":"+replica2Port+":"+owner+":"+tablename;

		Log.i("Msg to Replica2: ",msg2);

		Socket s2;
		try{
			s2= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(replica2Port));
			OutputStreamWriter os2 = new OutputStreamWriter(s2.getOutputStream());
			BufferedWriter bwriter2 = new BufferedWriter(os2);

			bwriter2.write(msg2);  // send insert, port number of current node and contentvalue.
			bwriter2.newLine();                             // We add a new line so that while writing bufferedwriter can write the entire line and then flush it
			bwriter2.flush();
			Log.i("Insert Fwd","Msg sent to Replica2");

			InputStreamReader isstream = new InputStreamReader(s2.getInputStream());
			BufferedReader brC = new BufferedReader(isstream);

			String readLine="";
			readLine=brC.readLine();
			if(readLine==null)
			{
				Log.i(TAG,"Connection terminated");
				s2.close();
			}

			else if(readLine.contains("PA4 OK"))
			{
				Log.i(TAG,"Replica2 replied. Insert Communication Successful..");
				s2.close();
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}

	}

//-----------------------------------------------------------------------------KEY COMPARISON METHOD-----------------------------------------------------------------------------------------//

	public int keyCompare(String gKey)
	{
		for(int i=0;i<list.size();i++)
		{
			int value=gKey.compareTo(list.get(i));

			if(i==0)         // i.e (AVD=5562 ) Lowest hash - 177 ....
			{
				int value2=gKey.compareTo(list.getLast());
				if(value<=0 || value2>0)
				{
					return i;
				}
			}
			else
			{
				int value2=gKey.compareTo(list.get(i-1));
				if(value<=0 && value2>0)
				{
					return i;
				}
			}
		}
		return 0;
	}

//-------------------------------------------------------------------------------------ONCREATE -------------------------------------------------------------------------------------------//

	@Override
	public boolean onCreate() {
		// According to Lifecycle onCreate() will be called first. Since this content provider is registered in AndroidManifest.xml, even before the main activity is called, oncreate of content provider will be called

		lock.lock();  // We will perform a lock so that the ServerTask which is also trying to apply the same lock, will be blocked. This we are doing because recovery should happen before,then only insert and query tasks should happen

		Log.w("AVD Started","Oncreate invoked");
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = String.valueOf((Integer.parseInt(portStr) * 2));
		myEmulatorNo = String.valueOf((Integer.parseInt(portStr)));
		Cursor c=null;

		Log.i("My emulator: ",myEmulatorNo);

		String EmulatorNo1=String.valueOf(Integer.parseInt(REMOTE_PORT0)/2);
		String EmulatorNo2=String.valueOf(Integer.parseInt(REMOTE_PORT1)/2);
		String EmulatorNo3=String.valueOf(Integer.parseInt(REMOTE_PORT2)/2);
		String EmulatorNo4=String.valueOf(Integer.parseInt(REMOTE_PORT3)/2);
		String EmulatorNo5=String.valueOf(Integer.parseInt(REMOTE_PORT4)/2);

		try {
			String Em1hash=genHash(EmulatorNo1);
			String Em2hash=genHash(EmulatorNo2);
			String Em3hash=genHash(EmulatorNo3);
			String Em4hash=genHash(EmulatorNo4);
			String Em5hash=genHash(EmulatorNo5);

			Log.i("Emulator No 1:",EmulatorNo1);

			for(int i=0;i<5;i++)            // create list of each node id (hash value of emulator id)
			{
				if(i==0)
				{
					list.add(Em1hash);
				}
				if(i==1)
				{
					list.add(Em2hash);
				}
				if(i==2)
				{
					list.add(Em3hash);
				}
				if(i==3)
				{
					list.add(Em4hash);
				}
				if(i==4)
				{
					list.add(Em5hash);
				}
				Log.i("Emulator list: ",list.get(i));
			}

			map=new HashMap<String, String>();
			map.put(Em1hash,EmulatorNo1);
			map.put(Em2hash,EmulatorNo2);
			map.put(Em3hash,EmulatorNo3);
			map.put(Em4hash,EmulatorNo4);
			map.put(Em5hash,EmulatorNo5);

			for(String key:map.keySet())               // this hashmap consists of (Emulator-hash:Emulator Id) mapping
			{
				Log.i("MAP key:",key +" Map Values: "+map.get(key));
			}

			Collections.sort(list);                   // sort the list by hash values
			for(int i=0;i<list.size();i++)
			{
				Log.i("Sorted List: ",list.get(i));
			}


//---------------------------------------------------Create server task---------------------------------------------------------------------//

			try {

				ServerSocket serverSocket = new ServerSocket();
				serverSocket.setReuseAddress(true);
				serverSocket.bind(new InetSocketAddress(SERVER_PORT));
				new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
				Log.i(TAG,"Server task created");
			}
			catch (IOException e) {

				e.printStackTrace();

			}

			//------------------------------------------------------Create client task-----------------------------------------------------------------//

			c=performQuery("database","@");
			if(c.getCount()!=0)                    // If the AVD has some data i.e no delete operation has been performed, just the AVD has been force-stopped
			{
				String msg="";
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msg,myPort);  // client task is created so that the missed values can be taken by failure recovered AVD

				Log.i(TAG,"Client task created");

			}
			else
			{
				lock.unlock(); //If no recovery needs to be done, i.e application has not been forced stopped or a delete operation has taken place, then no need to keep the lock, other operations can take place.
			}

		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}



		return false;
	}



	@Override
	public int update(Uri uri, ContentValues values, String selection,
					  String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	private String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}

//------------------------------------------------------RECOVERY of Node (Client Task)----------------------------------------------------------------------------------------------------//

	private class ClientTask extends AsyncTask<String, Void, Void> {

		@Override
		protected Void doInBackground(String... msgs) {

//------------------------------------Take the data from replicas-------------------------------------------------------------------//

			String myHash="";
			try {
				myHash = genHash(myEmulatorNo);
			}
			catch(NoSuchAlgorithmException e)
			{
				e.printStackTrace();
			}

			int listIndex=0;

			for(int i=0;i<list.size();i++)
			{
				if(myHash.equalsIgnoreCase(list.get(i)))
				{
					listIndex=i;                               // get the current AVD's index from the list
					break;
				}
			}

			int next_index=0;
			int prev_index=0;
			int prev_prev_index=0;

			for(int i=0;i<list.size();i++)
			{
				if(i==listIndex && listIndex==0)   // index =0
				{
					next_index=i+1;
					prev_index=(list.size()-1);
					prev_prev_index=(list.size()-2);
					Log.i("List Location:","Index:"+i+" Prev-Prev: "+prev_prev_index+" Prev: "+prev_index+" Next: "+next_index);
				}
				else if(i==listIndex && listIndex==(list.size()-1))  // index =4
				{
					next_index=0;
					prev_index=(list.size()-2);
					prev_prev_index=(list.size()-3);
					Log.i("List Location:","Index:"+i+" Prev-Prev: "+prev_prev_index+" Prev: "+prev_index+" Next: "+next_index);
				}
				else if(i==listIndex && listIndex==1)  // index =1
				{
					next_index=i+1;
					prev_index=0;
					prev_prev_index=(list.size()-1);
					Log.i("List Location:","Index:"+i+" Prev-Prev: "+prev_prev_index+" Prev: "+prev_index+" Next: "+next_index);
				}
				else if(i==listIndex)      // index=other values 2,3
				{
					next_index=i+1;
					prev_index=i-1;
					prev_prev_index=i-2;
					Log.i("List Location:","Index:"+i+" Prev-Prev: "+prev_prev_index+" Prev: "+prev_index+" Next: "+next_index);
				}
			}

//------------------------------------------------Calculate hash values of next as well as previous node----------------------------------------------//

			String next_hash_value=list.get(next_index); // contains the hash_value of the node in which the respective key is stored
			String prev_hash_value=list.get(prev_index);
			String prev_prev_hash_value=list.get(prev_prev_index);

			String nextEmulatorNumber="";
			String prevEmulatorNumber="";
			String prevprevEmulatorNumber="";

			if(map.containsKey(next_hash_value))
			{
				nextEmulatorNumber=map.get(next_hash_value);   // we will get emulator no of next here (Ex:5554)
			}

			if(map.containsKey(prev_hash_value))
			{
				prevEmulatorNumber=map.get(prev_hash_value);    // we will get emulator no of previous here
			}

			if(map.containsKey(prev_hash_value))
			{
				prevprevEmulatorNumber=map.get(prev_prev_hash_value);    // we will get the emulator no of previous to previous node here
			}

			String nextPNumber=String.valueOf(Integer.parseInt(nextEmulatorNumber)*2);  // ex: 5554 to 11108
			String prevPNumber=String.valueOf(Integer.parseInt(prevEmulatorNumber)*2);  // ex: 5554 to 11108
			String prevprevPNumber=String.valueOf(Integer.parseInt(prevprevEmulatorNumber)*2);  // ex: 5554 to 11108


//------------------------------------------------------Query data from first replica (next node)--------------------------------------------------------------------//


			String msg="sendMeMyData"+":"+myEmulatorNo+":"+prevEmulatorNumber;  //   (sendMeMyData:owner:previous)

			Log.i("Msg to Next Node: ",msg);

			Socket s;

			try{
				s= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(nextPNumber));
				OutputStreamWriter os = new OutputStreamWriter(s.getOutputStream());
				BufferedWriter bwriter = new BufferedWriter(os);

				bwriter.write(msg);  // send insert, port number of current node and contentvalue.
				bwriter.newLine();                             // We add a new line so that while writing bufferedwriter can write the entire line and then flush it
				bwriter.flush();
				Log.w("Ask for Backup","Msg sent to Next Node "+nextEmulatorNumber);

				InputStreamReader isstream = new InputStreamReader(s.getInputStream());
				BufferedReader brC = new BufferedReader(isstream);
				Log.i("Test","1");
				String readLine="";
				readLine=brC.readLine();
				Log.i("Test","2");
				String strg=readLine;
				Log.i("Test","3");

				if(strg==null||strg.equalsIgnoreCase("Empty")||strg.equals(""))
				{
					Log.w("Replica",nextEmulatorNumber +" is down.");// Do nothing
				}
				else
				{
					Log.i("Test","5");
					if(strg.equalsIgnoreCase(""))
					{
						Log.i("Test","6");
					}
					Log.w("Received line: ",strg);
					String[] splits=strg.split(":");           // Split this list of all the key-value-owner
					Log.i("Received line: ",strg);
					LinkedList<String> keyList=new LinkedList<String>();
					LinkedList<String> valueList=new LinkedList<String>();
					LinkedList<String> ownerList=new LinkedList<String>();


					int count=0;
					for(int x=0;x<splits.length;x++)
					{
						count++;
						if(count==1)
						{
							keyList.add(splits[x]);   // add all keys in 1 list
							Log.i("Key:",splits[x]);
						}
						else if(count==2)
						{
							valueList.add(splits[x]);  // add all values in other list
							Log.i("Value:",splits[x]);
						}
						else if(count==3)
						{
							ownerList.add(splits[x]);
							Log.i("Owner:",splits[x]);
							count=0;
						}
					}

					for(int x=0;x<keyList.size();x++)  // insert all the keys and values in the database of the recovered AVD
					{
						if(x<keyList.size())
						{
							String fKey=keyList.get(x);
							String fValue=valueList.get(x);
							String fOwner=ownerList.get(x);
							performInsert("database",fKey,fValue,fOwner,"Recovery");  // put the data in the current (recovered) AVD's database
						}
						else
						{
							Log.e("List","Out of bound exception "+"Index: "+x);
						}

					}
				}
				s.close();
			}
			catch (IOException e)
			{
				e.printStackTrace();
			}
			catch(NullPointerException e)
			{
				e.printStackTrace();
			}


//------------------------------------------------------------------Query data from previous node-------------------------------------------------------------------------//


			String msg2="sendMeMyData"+":"+myEmulatorNo+":"+prevprevEmulatorNumber+":"+"DummyString";   // Send (sendmemydata:myEmulatorNo,ownerPort for prev to prev msgs:Dummy String)

			Log.i("Msg to Previous Node: ",msg2);

			Socket s2;

			try{
				s2= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(prevPNumber));
				OutputStreamWriter os = new OutputStreamWriter(s2.getOutputStream());
				BufferedWriter bwriter = new BufferedWriter(os);

				bwriter.write(msg2);  // send insert, port number of current node and contentvalue.
				bwriter.newLine();                             // We add a new line so that while writing bufferedwriter can write the entire line and then flush it
				bwriter.flush();
				Log.w("Ask for Backup","Msg sent to Previous Node "+prevEmulatorNumber);

				InputStreamReader isstream = new InputStreamReader(s2.getInputStream());
				BufferedReader brC = new BufferedReader(isstream);

				String readLine="";
				readLine=brC.readLine();

				String strg=readLine;

				if(strg==null||strg.equalsIgnoreCase("Empty") || strg.equals(""))
				{
					Log.w("Replica",prevEmulatorNumber +" is down.");//Do nothing
				}
				else
				{
					Log.w("Received line: ",strg);
					String[] splits=strg.split(":");           // Split this list of all the key-value pairs
					LinkedList<String> keyList=new LinkedList<String>();
					LinkedList<String> valueList=new LinkedList<String>();
					LinkedList<String> ownerList=new LinkedList<String>();

					int count=0;
					for(int x=0;x<splits.length;x++)
					{
						count++;
						if(count==1)
						{
							keyList.add(splits[x]);   // add all keys in 1 list
							Log.i("Key:",splits[x]);
						}
						else if(count==2)
						{
							valueList.add(splits[x]);  // add all values in other list
							Log.i("Value:",splits[x]);
						}
						else if(count==3)
						{
							ownerList.add(splits[x]);
							Log.i("Owner:",splits[x]);
							count=0;
						}
					}

					for(int x=0;x<keyList.size();x++)  // insert all the keys and values in the database of the recovered AVD
					{
						if(x<keyList.size())
						{
							String fKey=keyList.get(x);
							String fValue=valueList.get(x);
							String fOwner=ownerList.get(x);
							performInsert("database",fKey,fValue,fOwner,"Recovery");  // put the data in the current (recovered) AVD's database
						}
						else
						{
							Log.e("List","Out of bound exception "+"Index: "+x);
						}

					}
				}
				s2.close();
			}
			catch (IOException e)
			{
				e.printStackTrace();
			}
			catch(NullPointerException e)
			{
				e.printStackTrace();
			}

			return null;
		}

		/* We have acquired lock in OnCreate() method which runs on main thread, so we will unlock it in onPostExecute() which also runs on main thread. We do this because the thread that
		   acquires lock can only unlock it, no other thread can perform that operation.
		*/
		@Override
		protected void onPostExecute(Void aVoid) {
			super.onPostExecute(aVoid);
			lock.unlock();                      // unlock the acquired lock so that all operations (insert, query, delete) can start after the recovery..
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

					lock.lock();       // we will use this lock so that the server gets blocked, since the same lock is applied by onCreate() method, but that method obtains the lock before ServerTask,
					                  // so untill it unlocks it, servertask can't go ahead. This we are doing because we want to perform recovery before than any other operation and it is happening in clienttask of oncreate()
					lock.unlock();

					Log.w("In","Server task");
					InputStreamReader istream=new InputStreamReader(s.getInputStream());
					BufferedReader br=new BufferedReader(istream);
					str=br.readLine();

					Log.w("Received Msg",str);

					String[] fragments=str.split(":");

//----------------------------------------------------------------------------Giving replicated backup of previous-to-previous node from previous node to failure recovered node------------------------------------------//

					if(fragments.length==4)
					{
						Log.i("In Previous node: ","Previous Node");
						String previous_to_previous_of_RecoveredEmulator=fragments[2];
						Log.i("At Previous Node","Previous-to-previous node"+" "+previous_to_previous_of_RecoveredEmulator);
						String strg="";

						Cursor c1;
						c1=performQuery("database","@");
						if(c1==null||c1.getCount()==0)  // no data in map, so do nothing
						{
							strg="Empty";
							Log.i("RdataMap","Empty");

						}
						else
						{
							c1.moveToFirst();
							while(!c1.isAfterLast())
							{
								if(c1.getString(2).equalsIgnoreCase(previous_to_previous_of_RecoveredEmulator))
								{
									Log.w("Found @ Replica:",c1.getString(0)+" "+c1.getString(1)+" "+c1.getString(2));
									strg=strg+c1.getString(0)+":"+c1.getString(1)+":"+c1.getString(2)+":";
								}
								c1.moveToNext();
							}
							Log.i("Replica String",strg);
						}

						Log.i("Replica String: ",strg);

						OutputStreamWriter osw = new OutputStreamWriter(s.getOutputStream());  // send the data obtained from the current node to the requesting AVD
						BufferedWriter bwServer=new BufferedWriter(osw);
						bwServer.write(strg);
						bwServer.newLine();         // we add a new line so that while writing, bufferedwriter can write the entire line and then flush it
						bwServer.flush();

						s.close();

					}


//-------------------------------------------------------- Giving replicated backup from next node to failure recovered node-------------------------------------------------------------------------//

					if(fragments.length==3 && fragments[0].equalsIgnoreCase("sendMeMyData"))
					{
						Log.w("In Next node: ","(Replica)");
						String recoveredOwner=fragments[1];
						String previousofRecoveredOwner=fragments[2];
						Log.w("At replica","Recovered node"+" "+recoveredOwner+" Previous of Recovered Node"+" "+previousofRecoveredOwner);

						String strg="";

						Cursor c1;
						c1=performQuery("database","@");

						if(c1==null||c1.getCount()==0)  // no data in map, so do nothing
						{
							strg="Empty";
							Log.i("Replica String",strg);
						}
						else
						{
							c1.moveToFirst();
							while(!c1.isAfterLast())
							{
								if(c1.getString(2).equalsIgnoreCase(recoveredOwner)||c1.getString(2).equalsIgnoreCase(previousofRecoveredOwner))
								{
									Log.w("Found @ Replica:",c1.getString(0)+" "+c1.getString(1)+" "+c1.getString(2));
									strg=strg+c1.getString(0)+":"+c1.getString(1)+":"+c1.getString(2)+":";
								}
								c1.moveToNext();
							}
							Log.i("Replica String",strg);
						}

						OutputStreamWriter osw = new OutputStreamWriter(s.getOutputStream());  // send the data obtained from the current node to the requesting AVD
						BufferedWriter bwServer=new BufferedWriter(osw);
						bwServer.write(strg);
						bwServer.newLine();         // we add a new line so that while writing, bufferedwriter can write the entire line and then flush it
						bwServer.flush();

						s.close();

					}


//------------------------------------------------------------------------------SERVER TASK INSERT HANDLING--------------------------------------------------------------------------------------//

					else if(fragments.length==3)  //--------------------- Insert forwarded to rightful owner of the message---------------------//
					{
						String rcvdKey=fragments[0];  // key
						String rcvdVal=fragments[1];  // value
						String owner=fragments[2];      // server's port number
						Log.i("Key Hash @ S",genHash(rcvdKey));

						performInsert("database",rcvdKey,rcvdVal,owner,"Insert Operation Called");           // perform actual insertion in DB

						Log.i("Only acknowledgment: ","To Client");
						OutputStreamWriter osw = new OutputStreamWriter(s.getOutputStream());
						BufferedWriter bwServer = new BufferedWriter(osw);
						bwServer.write("PA4 OK");
						bwServer.newLine();
						bwServer.flush();
						s.close();
					}

					//-------------------- Replication (Insert into replicas)	--------------------------------------------//

					else if(fragments.length==5)
					{
						String rcvdKey=fragments[0];  // key
						String rcvdVal=fragments[1];  // value
						String replicaPNo=fragments[2];      // server's port number
						String owner=fragments[3];     // Owner of the message which is being replicated
						String tblName=fragments[4];
						Log.i("Key Hash @ Replicated S",genHash(rcvdKey));

						performInsert(tblName,rcvdKey,rcvdVal,owner,"Insert Operation Called");           // perform actual insertion in DB

						Log.i("Only acknowledgment: ","To Client");
						OutputStreamWriter osw = new OutputStreamWriter(s.getOutputStream());
						BufferedWriter bwServer = new BufferedWriter(osw);
						bwServer.write("PA4 OK");
						bwServer.newLine();
						bwServer.flush();
						s.close();
					}

//--------------------------------------------------------------------------SERVER TASK SINGLE QUERY HANDLING-------------------------------------------------------------------------------------------//

					// --------------------- Query from replica (Incase the co-ordinator i.e the main AVD to be queried fails)---------------------------//

					else if(fragments.length==2 && fragments[1].equalsIgnoreCase("QueryReplica"))
					{
						String selection=fragments[0];
						String qrymsg=fragments[1];
						Log.w("Querying replica: ",myEmulatorNo+" "+selection);
						Log.i("Fragments: ",selection+" "+qrymsg);
						try{
							cursor=performQuery("database",selection);
						}
						catch (Exception e)
						{
							e.printStackTrace();
						}

					}

					//------------------normal single query when received by an AVD---------------------------------------------------------//

					else if(fragments.length==2)
					{
						String selection=fragments[0];
						String prt=fragments[1];
						String genSelection="";
						Log.w("Querying: ",myEmulatorNo+" "+selection);
						Log.i("Fragments: ",prt+" "+selection);
						try{
							genSelection=genHash(selection);                 // hash the key to be searched
							Log.i("Selection (key) hash: ",genSelection);

							int list_index=keyCompare(genSelection);  // perform key comparison to check to which AVD it belongs

							if(genHash(myEmulatorNo).equalsIgnoreCase(list.get(list_index)))
							{
								cursor=performQuery("database",selection);
							}

						}
						catch (Exception e)
						{
							e.printStackTrace();
						}

					}


//------------------------------------------------------------------------------SERVER TASK * and @ QUERY HANDLING--------------------------------------------------------------------------------------------//

					else if(fragments.length==1 && (fragments[0].equalsIgnoreCase("*QueryRequest")||fragments[0].equalsIgnoreCase("@QueryRequest")))
					{
						String strg="";
						cursor=performQuery("database","@");
						if(cursor.getCount()!=0)                 // If server contains data
						{
							cursor.moveToFirst();
							while(!cursor.isAfterLast())
							{
								if(!cursor.isLast())
								{
									String stg=cursor.getString(0)+":"+cursor.getString(1)+":"+cursor.getString(2)+":";
									strg=strg+stg;
									Log.i("Stg content: ",stg);
									Log.i("Strg content",strg);
								}
								else if(cursor.isLast())
								{
									String stg=cursor.getString(0)+":"+cursor.getString(1)+":"+cursor.getString(2);
									strg=strg+stg;
									Log.i("Stg content: ",stg);
									Log.i("Strg content",strg);
								}
								cursor.moveToNext();
							}                                        // cursor in the form of string obtained
							Log.i("String Fwding Back: ",strg);

							OutputStreamWriter osw = new OutputStreamWriter(s.getOutputStream());  // send the data obtained from the current node to the requesting AVD
							BufferedWriter bwServer=new BufferedWriter(osw);
							bwServer.write(strg);
							bwServer.newLine();         // we add a new line so that while writing, bufferedwriter can write the entire line and then flush it
							bwServer.flush();

							s.close();
						}
						else
						{
							OutputStreamWriter osw = new OutputStreamWriter(s.getOutputStream());  // send the data obtained from the current node to the requesting AVD
							BufferedWriter bwServer=new BufferedWriter(osw);
							bwServer.write("Empty"+":"+"PA4 OK");
							bwServer.newLine();         // we add a new line so that while writing, bufferedwriter can write the entire line and then flush it
							bwServer.flush();

							s.close();
						}
					}

					if(fragments.length==2 && cursor.getCount()>0)  // If SINGLE query operation, post a different acknowledgment
					{
						Log.i("Sending back Query: ","Key: "+cursor.getString(0)+"Value: "+cursor.getString(1));  // When cursor is not empty, send the cursor value back
						OutputStreamWriter osw = new OutputStreamWriter(s.getOutputStream());
						BufferedWriter bwServer=new BufferedWriter(osw);
						bwServer.write(cursor.getString(0)+":"+cursor.getString(1)+":"+cursor.getString(2)+":"+"PA4 OK");
						bwServer.newLine();         // we add a new line so that while writing, bufferedwriter can write the entire line and then flush it
						bwServer.flush();

						Log.i("Query result","Sent");

						s.close();
					}

				}
			}
			catch(IndexOutOfBoundsException e)
			{
				e.printStackTrace();
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}

			return null;

		}

	}

}
