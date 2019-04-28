package edu.buffalo.cse.cse486586.simpledynamo;

import android.provider.BaseColumns;

public class SimpleDynamoContract {
    SimpleDynamoContract(){}
    public static final class MessageEntry implements BaseColumns {
        public static final String TABLE_NAME = "messages";
        public static final String COLUMN_KEY = "key";
        public static final  String COLUMN_VALUE = "value";

        public static final String CREATE_TABLE =
                "CREATE TABLE " + TABLE_NAME + " ( " +
                        COLUMN_KEY + " TEXT UNIQUE NOT NULL, " +
                        COLUMN_VALUE + " TEXT NOT NULL )" ;
    }
}

