package edu.buffalo.cse.cse486586.simpledht;

import android.content.Context;
import android.database.SQLException;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

/**
 * Created by arnav on 4/6/17.
 */

public class SqliteDB extends SQLiteOpenHelper {

    //We mention the database name, its version, the table which is to be manipulated and a table creation query

    private static final String DBName="ProgrammingAssignment3.db";
    private static final int DB_Version=2;
    private static final String tableName="database";
    private static final String table2Name="cursortable";  // to insert values in cursor during 'Query' operation
    private static final String table3Name="starcursortable"; // to insert all cursor values during '*' operation
    private static final String tableCreation="CREATE TABLE "+tableName+"(key VARCHAR PRIMARY KEY,value VARCHAR);"; //We mention primary key as this field should never be null
    private static final String table2Creation="CREATE TABLE "+table2Name+"(key VARCHAR PRIMARY KEY,value VARCHAR);";
    private static final String table3Creation="CREATE TABLE "+table3Name+"(key VARCHAR PRIMARY KEY,value VARCHAR);";

    //Constructor

    SqliteDB(Context context) throws SQLException
    {
        super(context,DBName,null,DB_Version);   // sets the above mentioned values in the constructor of super class
    }

    //Method to be over-ridden

    @Override
    public void onUpgrade(SQLiteDatabase sqLiteDatabase, int i, int i1) {
        //not used
    }

    //Method to be over-ridden

    @Override
    public void onCreate(SQLiteDatabase sqLiteDatabase) throws SQLException{
        sqLiteDatabase.execSQL(tableCreation);                               // execSql command is used to execute the required SQL command
        sqLiteDatabase.execSQL(table2Creation);
        sqLiteDatabase.execSQL(table3Creation);
    }
}