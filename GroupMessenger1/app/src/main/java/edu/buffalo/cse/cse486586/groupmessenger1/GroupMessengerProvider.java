package edu.buffalo.cse.cse486586.groupmessenger1;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.inputmethodservice.Keyboard;
import android.net.Uri;
import android.util.Log;

import java.io.FileInputStream;
import java.io.FileOutputStream;

/**
 * GroupMessengerProvider is a key-value table. Once again, please note that we do not implement
 * full support for SQL as a usual ContentProvider does. We re-purpose ContentProvider's interface
 * to use it as a key-value table.
 * 
 * Please read:
 * 
 * http://developer.android.com/guide/topics/providers/content-providers.html
 * http://developer.android.com/reference/android/content/ContentProvider.html
 * 
 * before you start to get yourself familiarized with ContentProvider.
 * 
 * There are two methods you need to implement---insert() and query(). Others are optional and
 * will not be tested.
 * 
 * @author stevko
 *
 */
public class GroupMessengerProvider extends ContentProvider {

    private static final String TAG = GroupMessengerProvider.class.getName();

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // You do not need to implement this.
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // You do not need to implement this.
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        /*
         * TODO: You need to implement this method. Note that values will have two columns (a key
         * column and a value column) and one row that contains the actual (key, value) pair to be
         * inserted.
         * 
         * For actual storage, you can use any option. If you know how to use SQL, then you can use
         * SQLite. But this is not a requirement. You can use other storage options, such as the
         * internal storage option that we used in PA1. If you want to use that option, please
         * take a look at the code for PA1.
         */

        // Using SqLite database to store the (key,value) pairs

        SqliteDB db=new SqliteDB(this.getContext()); // Android standard for SQLiteOpenHelper class

        SQLiteDatabase sqlDb=db.getWritableDatabase(); // To get a writeable database we create an object of type SQLDatabase

      //  sqlDb.insert("database",null,values);

        //------- insertWithOnConflict(table_name,null field,ContentValues,Conflict_Algorithm)----------

        sqlDb.insertWithOnConflict("database",null,values,SQLiteDatabase.CONFLICT_REPLACE);  // To perform insertion and on Conflict i.e incase of duplicate records the old records are replaced by new ones.

        sqlDb.close();   // Close the DB on insertion

        Log.v("insert", values.toString());
        return uri;
    }

    @Override
    public boolean onCreate() {
        // If you need to perform any one-time initialization task, please do it here.
        return false;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // You do not need to implement this.
        return 0;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        /*
         * TODO: You need to implement this method. Note that you need to return a Cursor object
         * with the right format. If the formatting is not correct, then it is not going to work.
         *
         * If you use SQLite, whatever is returned from SQLite is a Cursor object. However, you
         * still need to be careful because the formatting might still be incorrect.
         *
         * If you use a file storage option, then it is your job to build a Cursor * object. I
         * recommend building a MatrixCursor described at:
         * http://developer.android.com/reference/android/database/MatrixCursor.html
         */


        SqliteDB dbRead=new SqliteDB(this.getContext()); //Android standard

        SQLiteDatabase sqlDBRead=dbRead.getReadableDatabase(); // A readable database to perform a query

        String[] selection1={selection}; // in query method of SQLiteDatabase object the selectionArgs field always contains an array for selection i.e array of values where the value lies in 0th index of array

        //--------------SQLDatabase.query(table_name,projection i.e columns to be returned,where_clause,value to be checked,OrderBY,Having and so on)

        Cursor cursor=sqlDBRead.query("database",null,"key=?",selection1,null,null,null,null); //query method returns a cursor which can be passed to the UI element

        Log.v("query", selection);
        return cursor;
    }
}
