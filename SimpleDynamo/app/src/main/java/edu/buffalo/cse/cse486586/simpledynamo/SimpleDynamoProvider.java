package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
    static final int SERVER_PORT = 10000;
    DatabaseHelper dbHelper;
    private static Uri mUri;
    SQLiteDatabase dbWriter;
    SQLiteDatabase dbReader;
    static DynamoRing dynamoRing;
    private static String KEY = "key";
    private static String VALUE = "value";
    static String MY_PORT;
    private static final String COORDINATOR_REPLICATION = "COORDINATOR_REPLICATION";
    private static final String COORDINATOR_QUERY = "COORDINATOR_QUERY";
    private static final String COORDINATOR_QUERY_ACK = "COORDINATOR_QUERY_ACK";
    private static final String COORDINATOR_ALL_QUERY = "COORDINATOR_ALL_QUERY";
    private static final String COORDINATOR_ALL_QUERY_ACK = "COORDINATOR_ALL_QUERY_ACK";
    private static final String REPLICATION_DELETE = "REPLICATION_DELETE";
    private static final String REPLICATION_DELETE_ACK = "REPLICATION_DELETE_ACK";
    private static final String DELETE_ALL_DATA = "DELETE_ALL_DATA";
    private static final String DELETE_ALL_DATA_ACK = "DELETE_ALL_DATA_ACK";
    private static final String COORDINATOR_DELETE = "COORDINATOR_DELETE";
    private static final String COORDINATOR_DELETE_ACK = "COORDINATOR_DELETE_ACK";
    private static final String ACK_MESSAGE = "ACK_MESSAGE";
    private static final String RECOVERY = "RECOVERY";
    private static final String RECOVERY_ACK = "RECOVERY_ACK";
    private static final String QUERY_REPLICA = "QUERY_REPLICA";
    private static final String QUERY_REPLICA_ACK = "QUERY_REPLICA_ACK";
    private static final String REPLICATE_TO_SUCCESSOR = "REPLICATE_TO_SUCCESSOR";

    private HashMap<String, String> myData = new HashMap<String, String>();
    private HashMap<String, String> predecessor1Data = new HashMap<String, String>();
    private HashMap<String, String> predecessor2Data = new HashMap<String, String>();

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        String selectionItem = SimpleDynamoContract.MessageEntry.COLUMN_KEY + " = ?";
        selectionArgs = new String[]{selection};
        String coordinator = dynamoRing.getCoordinator(genHash(selection));
        int deleteRows = 0;
        if (selection.equals("*")) {
            for (String port : dynamoRing.getAllNodesPort()) {
                if (port.equals(MY_PORT)) {
                    dbWriter.delete(SimpleDynamoContract.MessageEntry.TABLE_NAME, selectionItem, selectionArgs);
                    continue;
                }
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(port));
                    socket.setSoTimeout(1500);
                    PrintStream printStream = new PrintStream(socket.getOutputStream());
                    String messageToSend = DELETE_ALL_DATA + ":" + selection;
                    printStream.println(messageToSend);
                    printStream.flush();

                    InputStreamReader inputStreamReader = new InputStreamReader(socket.getInputStream());
                    BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
                    String messageReceived = bufferedReader.readLine();
                    String[] msg = messageReceived.split(":");

                    if (msg[0].equals(DELETE_ALL_DATA_ACK)) {
                        socket.close();
                    }

                } catch (NullPointerException ne) {
                    ne.printStackTrace();
                } catch (SocketException se) {
                    se.printStackTrace();
                } catch (SocketTimeoutException set) {
                    set.printStackTrace();
                } catch (IOException io) {
                    io.printStackTrace();
                }
            }
        } else if (selection.equals("@")) {
            return dbWriter.delete(SimpleDynamoContract.MessageEntry.TABLE_NAME, null, null);
        } else {
            String[] replicationList = dynamoRing.getReplicationList(coordinator);
            if (MY_PORT.equals(coordinator)) {
                deleteRows = dbWriter.delete(SimpleDynamoContract.MessageEntry.TABLE_NAME, selectionItem, selectionArgs);
                replicateDelete(selection, replicationList);
            } else {
                boolean success = contactCoordinatorAndDelete(selection, coordinator);
                if (!success) {
                    Log.e("delete", "coordinator is dead!");
                    replicateDelete(selection, replicationList);
                }
            }
        }
        return deleteRows;
    }

    private boolean contactCoordinatorAndDelete(String selection, String coordinator) {
        try {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(coordinator));
            socket.setSoTimeout(1500);
            PrintStream printStream = new PrintStream(socket.getOutputStream());
            String messageToSend = COORDINATOR_DELETE + ":" + selection;
            Log.e("conCoordForInsert", "message to send " + messageToSend);
            printStream.println(messageToSend);

            InputStreamReader inputStreamReader = new InputStreamReader(socket.getInputStream());
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
            String messageReceived = bufferedReader.readLine();
            Log.e("conCoordForInsert", "message to received " + messageReceived);
            String[] msg = messageReceived.split(":");
            if (msg[0].contains(COORDINATOR_DELETE_ACK)) {
                socket.close();
            }
        } catch (NullPointerException ne) {
            ne.printStackTrace();
            return false;
        } catch (SocketException se) {
            se.printStackTrace();
            return false;
        } catch (SocketTimeoutException set) {
            set.printStackTrace();
            return false;
        } catch (IOException io) {
            io.printStackTrace();
            return false;
        }
        return true;
    }

    private void replicateDelete(String selection, String[] replicationList) {
        for (String port : replicationList) {
            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(port));
                socket.setSoTimeout(1500);
                PrintStream printStream = new PrintStream(socket.getOutputStream());
                String messageToSend = REPLICATION_DELETE + ":" + selection;
                Log.e("replicateData", "message to send " + messageToSend);
                printStream.println(messageToSend);

                InputStreamReader inputStreamReader = new InputStreamReader(socket.getInputStream());
                BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
                String messageReceived = bufferedReader.readLine();
                Log.e("replicateData", "message to received " + messageReceived);
                String[] msg = messageReceived.split(":");
                if (msg[0].equals(REPLICATION_DELETE_ACK)) {
                    socket.close();
                }
            } catch (NullPointerException ne) {
                ne.printStackTrace();
            } catch (SocketException se) {
                se.printStackTrace();
            } catch (SocketTimeoutException set) {
                set.printStackTrace();
            } catch (IOException io) {
                io.printStackTrace();
            }
        }
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        String hashedKey = genHash(values.getAsString(KEY));
        String key = values.getAsString(KEY);
        String value = values.getAsString(VALUE);
        String coordinator = dynamoRing.getCoordinator(hashedKey);
        Log.e("insert", "insert request for " + key + " at " + MY_PORT);
        Log.e("insert", "coordinator " + coordinator + " for hash " + hashedKey);
        String[] replicationList = dynamoRing.getReplicationList(coordinator);
        if (MY_PORT.equals(coordinator)) {
            myData.put(key, value);
            dbWriter.insertWithOnConflict(SimpleDynamoContract.MessageEntry.TABLE_NAME, null,
                    values, SQLiteDatabase.CONFLICT_REPLACE);
            Log.e("insert", "replication List " + replicationList[0] + " " + replicationList[1]);
//            replicateToNext(key, value, coordinator, replicationList[0]);
            boolean success = replicateFromCoordinator(key, value, coordinator, replicationList[0]);
            if (!success)
                replicateToNext(key, value, coordinator, replicationList[1]);
        } else {
            Log.e("insert else", "contactCoordinatorForInsert");
            boolean success = contactCoordinatorForInsert(key, value, coordinator);
            if (!success) {
                Log.e("insert", "coordinator is dead!");
                replicateToNext(key, value, coordinator, replicationList[0]);
            }
        }
        return null;
    }

    private boolean replicateFromCoordinator(String key, String value, String coordinator, String port) {
        try {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(port));
            socket.setSoTimeout(1500);
            PrintStream printStream = new PrintStream(socket.getOutputStream());
            String messageToSend = REPLICATE_TO_SUCCESSOR + ":" + coordinator + ":" + key + ":" + value;
            Log.e("conCoordForInsert", "message to send " + messageToSend);
            printStream.println(messageToSend);

            InputStreamReader inputStreamReader = new InputStreamReader(socket.getInputStream());
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
            String messageReceived = bufferedReader.readLine();
            Log.e("conCoordForInsert", "message to received " + messageReceived);
            if (messageReceived != null)
                socket.close();
            else
                return false;

        } catch (NullPointerException ne) {
            ne.printStackTrace();
            return false;
        } catch (SocketException se) {
            se.printStackTrace();
            return false;
        } catch (SocketTimeoutException set) {
            set.printStackTrace();
            return false;
        } catch (IOException io) {
            io.printStackTrace();
            return false;
        }
        return true;
    }

    private boolean contactCoordinatorForInsert(String key, String value, String coordinator) {
        try {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(coordinator));
            socket.setSoTimeout(1500);
            PrintStream printStream = new PrintStream(socket.getOutputStream());
            String messageToSend = COORDINATOR_REPLICATION + ":" + coordinator + ":" + key + ":" + value;
            Log.e("conCoordForInsert", "message to send " + messageToSend);
            printStream.println(messageToSend);

            InputStreamReader inputStreamReader = new InputStreamReader(socket.getInputStream());
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
            String messageReceived = bufferedReader.readLine();
            Log.e("conCoordForInsert", "message to received " + messageReceived);
            if (messageReceived != null)
                socket.close();
            else
                return false;
        } catch (NullPointerException ne) {
            ne.printStackTrace();
            return false;
        } catch (SocketException se) {
            se.printStackTrace();
            return false;
        } catch (SocketTimeoutException set) {
            set.printStackTrace();
            return false;
        } catch (IOException io) {
            io.printStackTrace();
            return false;
        }
        return true;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {
        Cursor cursor = null;
        String[] selectionItems = selection.split(":");
        if (selection.equals("@")) {
            Log.e("if query", "selection " + selection);
            cursor = dbReader.query(SimpleDynamoContract.MessageEntry.TABLE_NAME, null, null, null,
                    null, null, null);
            return cursor;
        } else if (selection.equals("*")) {
            Log.e("elif query", "selection " + selection);
            MatrixCursor matrixCursor = new MatrixCursor(new String[]{KEY, VALUE});
            for (String port : dynamoRing.getAllNodesPort()) {
                if (MY_PORT.equals(port)) {
                    Log.e("query", "from my port");
                    Cursor myCursor = dbReader.query(SimpleDynamoContract.MessageEntry.TABLE_NAME, null, null, null,
                            null, null, null);
                    if (myCursor != null && myCursor.getCount() > 0) {
                        myCursor.moveToFirst();
                        while (!myCursor.isAfterLast()) {
                            String key = myCursor.getString(myCursor.getColumnIndex(KEY));
                            String value = myCursor.getString(myCursor.getColumnIndex(VALUE));
                            matrixCursor.newRow().add(KEY, key)
                                    .add(VALUE, value);
                            myCursor.moveToNext();
                        }
                    }
                    continue;
                }
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(port));
                    socket.setSoTimeout(1500);
                    PrintStream printStream = new PrintStream(socket.getOutputStream());
                    String messageToSend = COORDINATOR_ALL_QUERY + ":" + "*";
                    printStream.println(messageToSend);
                    Log.e("query", "message to send " + messageToSend);

                    InputStreamReader inputStreamReader = new InputStreamReader(socket.getInputStream());
                    BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
                    String messageReceived = bufferedReader.readLine();
                    Log.e("query", "message to send " + messageToSend);
                    String[] msg = messageReceived.split(":");
                    if (msg[0].contains(COORDINATOR_ALL_QUERY_ACK)) {
                        String[] content = msg[1].split("==");
                        for (int j = 0; j < content.length; j++) {
                            matrixCursor.newRow().add(KEY, content[j].split(";")[0])
                                    .add(VALUE, content[j].split(";")[1]);
                        }
                    }
                    socket.close();
                } catch (NullPointerException ne) {
                    ne.printStackTrace();
                } catch (SocketException se) {
                    se.printStackTrace();
                } catch (SocketTimeoutException set) {
                    set.printStackTrace();
                } catch (IOException io) {
                    io.printStackTrace();
                }
            }
            return matrixCursor;
        } else {
            String coordinator = dynamoRing.getCoordinator(genHash(selectionItems[0]));
            String selectionItem = SimpleDynamoContract.MessageEntry.COLUMN_KEY + " = ?";
            String[] selectionArg = new String[]{selectionItems[0]};
            if (MY_PORT.equals(coordinator)) {
                cursor = dbReader.query(SimpleDynamoContract.MessageEntry.TABLE_NAME, null,
                        selectionItem, selectionArg, null, null, null);
            } else {
                cursor = contactCoordinatorForQuery(selectionItems[0], coordinator);
                if (cursor == null) {
                    cursor = contactReplicasForQuery(selectionItems[0], coordinator);
                }
            }
            return cursor;
        }
    }

    private Cursor contactReplicasForQuery(String selectionItem, String coordinator) {
        String port = dynamoRing.getSuccessor(coordinator);
        MatrixCursor cursor = new MatrixCursor(new String[]{KEY, VALUE});
        ConcurrentHashMap<String, String> content = new ConcurrentHashMap<String, String>();
        try {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(port));
            socket.setSoTimeout(1500);
            PrintStream printStream = new PrintStream(socket.getOutputStream());
            String messageToSend = QUERY_REPLICA + ":" + selectionItem;
            Log.e("contactCoorForQuery", "message to send " + messageToSend);
            printStream.println(messageToSend);

            InputStreamReader inputStreamReader = new InputStreamReader(socket.getInputStream());
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
            String messageReceived = bufferedReader.readLine();
            Log.e("contactCoorForQuery", "message received " + messageReceived);
            String[] msg = messageReceived.split(":");
            if (msg[0].contains(QUERY_REPLICA_ACK)) {
                String[] value = msg[1].split(";");
                content.put(value[0], value[1]);
            }
            for (String key : content.keySet()) {
                cursor.newRow().add(KEY, key)
                        .add(VALUE, content.get(key));
            }
            socket.close();
            return cursor;
        } catch (NullPointerException ne) {
            ne.printStackTrace();
        } catch (SocketException se) {
            se.printStackTrace();
        } catch (SocketTimeoutException set) {
            set.printStackTrace();
        } catch (IOException io) {
            io.printStackTrace();
        }
        return null;
    }

    private Cursor contactCoordinatorForQuery(String selectionItem, String coordinator) {
        MatrixCursor cursor = null;
        try {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(coordinator));
            socket.setSoTimeout(1500);
            PrintStream printStream = new PrintStream(socket.getOutputStream());
            String messageToSend = COORDINATOR_QUERY + ":" + selectionItem;
            Log.e("contactCoorForQuery", "message to send " + messageToSend);
            printStream.println(messageToSend);

            InputStreamReader inputStreamReader = new InputStreamReader(socket.getInputStream());
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
            String messageReceived = bufferedReader.readLine();
            Log.e("contactCoorForQuery", "message received " + messageReceived);
            String[] msg = messageReceived.split(":");
            if (msg[0].contains(COORDINATOR_QUERY_ACK)) {
                cursor = new MatrixCursor(new String[]{KEY, VALUE});
                String[] content = msg[1].split("==");
                for (int j = 0; j < content.length; j++) {
                    cursor.newRow().add(KEY, content[j].split(";")[0])
                            .add(VALUE, content[j].split(";")[1]);
                }
            }
            socket.close();
            return cursor;
        } catch (NullPointerException ne) {
            ne.printStackTrace();
            return null;
        } catch (SocketException se) {
            se.printStackTrace();
            return null;
        } catch (SocketTimeoutException set) {
            set.printStackTrace();
            return null;
        } catch (IOException io) {
            io.printStackTrace();
            return null;
        }
    }

    @Override
    public boolean onCreate() {
        dbHelper = new DatabaseHelper(getContext());
        mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo");
        dbWriter = dbHelper.getWritableDatabase();
        dbReader = dbHelper.getReadableDatabase();
        clearDatabase();
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        MY_PORT = String.valueOf((Integer.parseInt(portStr) * 2));
        dynamoRing = new DynamoRing();
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException io) {
            io.printStackTrace();
        }
        recoverData();
        return true;
    }

    private void clearDatabase() {
        String clearDatabase = "DELETE FROM " + SimpleDynamoContract.MessageEntry.TABLE_NAME;
        dbWriter.execSQL(clearDatabase);
    }

    private void recoverData() {
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "null", RECOVERY);
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            try {
                while (true) {
                    Socket socket = serverSocket.accept();
                    InputStreamReader inputStreamReader = new InputStreamReader(socket.getInputStream());
                    BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
                    String message = bufferedReader.readLine();

                    String[] messageParts = message.split(":");

                    if (messageParts[0].equals(COORDINATOR_REPLICATION)) {
                        ContentValues values = new ContentValues();
                        String coordinator = messageParts[1];
                        values.put(KEY, messageParts[2]);
                        values.put(VALUE, messageParts[3]);
                        myData.put(messageParts[2], messageParts[3]);

                        Log.e("ServerTask", "Replication data " + messageParts[1] + " " + messageParts[2]);
                        dbWriter.insertWithOnConflict(SimpleDynamoContract.MessageEntry.TABLE_NAME, null,
                                values, SQLiteDatabase.CONFLICT_REPLACE);

                        boolean success = replicateFromCoordinator(messageParts[2], messageParts[3], coordinator, dynamoRing.getSuccessor(MY_PORT));
                        if (!success) {
                            replicateToNext(messageParts[2], messageParts[3], coordinator, dynamoRing.getSuccessor(dynamoRing.getSuccessor(MY_PORT)));
                        }
                    } else if (messageParts[0].equals(COORDINATOR_QUERY)) {
                        Log.e("ServerTask", "COORDINATOR_QUERY");
                        String selectionItem = SimpleDynamoContract.MessageEntry.COLUMN_KEY + " = ?";
                        String[] selectionArgs = new String[]{messageParts[1]};
                        Cursor cursor = dbReader.query(SimpleDynamoContract.MessageEntry.TABLE_NAME, null, selectionItem, selectionArgs,
                                null, null, null);
                        String messageToSend = "";
                        if (cursor != null && cursor.getCount() > 0) {
                            cursor.moveToFirst();
                            while (!cursor.isAfterLast()) {
                                String key = cursor.getString(cursor.getColumnIndex(KEY));
                                String value = cursor.getString(cursor.getColumnIndex(VALUE));
                                if (messageToSend.isEmpty()) {
                                    messageToSend += COORDINATOR_QUERY_ACK + ":" + key + ";" + value;

                                } else
                                    messageToSend += "==" + key + ";" + value;
                                cursor.moveToNext();
                            }
                        } else {
                            messageToSend = COORDINATOR_QUERY_ACK + ":" + messageParts[1];
                        }
                        PrintStream printStream = new PrintStream(socket.getOutputStream());
                        printStream.println(messageToSend);
                        Log.e("ServerTask", "COORDINATOR_QUERY_ACK messageToSend " + messageToSend);
                        printStream.flush();
                        if (cursor != null)
                            cursor.close();
                        socket.close();
                        continue;
                    } else if (messageParts[0].equals(COORDINATOR_ALL_QUERY)) {
                        Cursor myCursor = null;
                        myCursor = dbReader.query(SimpleDynamoContract.MessageEntry.TABLE_NAME, null, null, null,
                                null, null, null);
                        String messageToSend = "";
                        if (myCursor != null && myCursor.getCount() > 0) {
                            myCursor.moveToFirst();
                            while (!myCursor.isAfterLast()) {
                                String key = myCursor.getString(myCursor.getColumnIndex(KEY));
                                String value = myCursor.getString(myCursor.getColumnIndex(VALUE));
                                if (messageToSend.isEmpty()) {
                                    messageToSend += COORDINATOR_ALL_QUERY_ACK + ":" + key + ";" + value;
                                } else
                                    messageToSend += "==" + key + ";" + value;
                                myCursor.moveToNext();
                            }
                        }
                        PrintStream printStream = new PrintStream(socket.getOutputStream());
                        printStream.println(messageToSend);
                        Log.e("ServerTask", "COORDINATOR_ALL_QUERY_ACK messageToSend " + messageToSend);
                        printStream.flush();
                        if (myCursor != null)
                            myCursor.close();
                        socket.close();
                        continue;
                    } else if (messageParts[0].equals(DELETE_ALL_DATA)) {
                        dbWriter.delete(SimpleDynamoContract.MessageEntry.TABLE_NAME, null, null);
                        String messageToSend = DELETE_ALL_DATA_ACK + ":";

                        PrintStream printStream = new PrintStream(socket.getOutputStream());
                        printStream.println(messageToSend);
                        printStream.flush();
                        socket.close();
                        continue;
                    } else if (messageParts[0].equals(COORDINATOR_DELETE)) {
                        String selection = messageParts[1];
                        delete(null, selection, null);
                        String messageToSend = COORDINATOR_DELETE_ACK + ":";

                        PrintStream printStream = new PrintStream(socket.getOutputStream());
                        printStream.println(messageToSend);
                        printStream.flush();
                        socket.close();
                        continue;
                    } else if (messageParts[0].equals(REPLICATION_DELETE)) {
                        String selection = SimpleDynamoContract.MessageEntry.COLUMN_KEY + " = ?";
                        String selectionArgs[] = {messageParts[1]};
                        dbWriter.delete(SimpleDynamoContract.MessageEntry.TABLE_NAME, selection, selectionArgs);
                        String messageToSend = REPLICATION_DELETE_ACK + ":";

                        PrintStream printStream = new PrintStream(socket.getOutputStream());
                        printStream.println(messageToSend);
                        printStream.flush();
                        socket.close();
                        continue;
                    } else if (messageParts[0].equals(RECOVERY)) {
                        String messageToSend = "";
                        String messagePart1 = "";
                        String messagePart2 = "";
                        String fromPort = messageParts[1];
                        if (myData.isEmpty() && predecessor1Data.isEmpty())
                            messageToSend = RECOVERY_ACK + ":" + "";
                        if (fromPort.equals(dynamoRing.getSuccessor(MY_PORT))) {
                            Log.e("server rec", "sending pred12 data");
                            if (!myData.isEmpty()) {
                                for (Map.Entry<String, String> entry : myData.entrySet()) {
                                    if (messagePart1.isEmpty()) {
                                        messagePart1 += entry.getKey() + ";" + entry.getValue();
                                    } else
                                        messagePart1 += "==" + entry.getKey() + ";" + entry.getValue();
                                }
                            } else
                                messagePart1 = "null";
                            if (!predecessor1Data.isEmpty()) {
                                for (Map.Entry<String, String> entry : predecessor1Data.entrySet()) {
                                    if (messagePart2.isEmpty()) {
                                        messagePart2 += entry.getKey() + ";" + entry.getValue();
                                    } else
                                        messagePart2 += "==" + entry.getKey() + ";" + entry.getValue();
                                }
                            } else
                                messagePart2 = "null";

                            messageToSend = RECOVERY_ACK + ":" + MY_PORT + ":" + messagePart1 + "<>" + messagePart2;

                        } else if (fromPort.equals(dynamoRing.getPredecessor(MY_PORT))) {
                            Log.e("server rec", "sending pred1 data");
                            if (!predecessor1Data.isEmpty()) {
                                Log.e("RECOVERY", "sending pred1 data");
                                for (Map.Entry<String, String> entry : predecessor1Data.entrySet()) {
                                    if (messageToSend.isEmpty()) {
                                        messageToSend += RECOVERY_ACK + ":" + MY_PORT + ":"
                                                + entry.getKey() + ";" + entry.getValue();
                                    } else
                                        messageToSend += "==" + entry.getKey() + ";" + entry.getValue();
                                }
                            } else
                                messageToSend = RECOVERY_ACK + ":" + "";
                        }
                        PrintStream printStream = new PrintStream(socket.getOutputStream());
                        Log.e("server rec", "message to send: " + messageToSend);
                        printStream.println(messageToSend);
                        printStream.flush();
                        socket.close();
                        continue;
                    } else if (messageParts[0].equals(QUERY_REPLICA)) {
                        Cursor cursor;
                        String selectionItem = SimpleDynamoContract.MessageEntry.COLUMN_KEY + " = ?";
                        String[] selectionArgs = new String[]{messageParts[1]};
                        cursor = dbReader.query(SimpleDynamoContract.MessageEntry.TABLE_NAME, null, selectionItem, selectionArgs,
                                null, null, null);
                        String messageToSend = "";
                        if (cursor != null && cursor.getCount() > 0) {
                            cursor.moveToFirst();
                            while (!cursor.isAfterLast()) {
                                String key = cursor.getString(cursor.getColumnIndex(KEY));
                                String value = cursor.getString(cursor.getColumnIndex(VALUE));
                                if (messageToSend.isEmpty()) {
                                    messageToSend += QUERY_REPLICA_ACK + ":" + key + ";" + value;
                                }
                                cursor.moveToNext();
                            }
                        }
                        PrintStream printStream = new PrintStream(socket.getOutputStream());
                        printStream.println(messageToSend);
                        printStream.flush();
                        socket.close();
                        continue;
                    } else if (messageParts[0].equals(REPLICATE_TO_SUCCESSOR)) {
                        ContentValues values = new ContentValues();
                        String coordinator = messageParts[1];
                        values.put(KEY, messageParts[2]);
                        values.put(VALUE, messageParts[3]);
                        if (coordinator.equals(dynamoRing.getPredecessor(MY_PORT))) {
                            predecessor1Data.put(messageParts[2], messageParts[3]);
                        } else if (coordinator.equals(dynamoRing.getPredecessor(dynamoRing.getPredecessor(MY_PORT)))) {
                            predecessor2Data.put(messageParts[2], messageParts[3]);
                        }
                        Log.e("ServerTask", "Replication data " + messageParts[1] + " " + messageParts[2]);
                        dbWriter.insertWithOnConflict(SimpleDynamoContract.MessageEntry.TABLE_NAME, null,
                                values, SQLiteDatabase.CONFLICT_REPLACE);
                        String[] replicationList = dynamoRing.getReplicationList(coordinator);
                        if (!MY_PORT.equals(replicationList[1]))
                            replicateToNext(messageParts[2], messageParts[3], coordinator, dynamoRing.getSuccessor(MY_PORT));
                    }
                    PrintStream printStream = new PrintStream(socket.getOutputStream());
                    printStream.println(ACK_MESSAGE);
                    printStream.flush();
                    socket.close();
                }
            } catch (NullPointerException ne) {
                ne.printStackTrace();
            } catch (SocketException se) {
                se.printStackTrace();
            } catch (SocketTimeoutException set) {
                set.printStackTrace();
            } catch (IOException io) {
                io.printStackTrace();
            }
            return null;
        }
    }

    private void replicateToNext(String key, String value, String coordinator, String toPort) {
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, key + ":" + value,
                REPLICATE_TO_SUCCESSOR, coordinator, toPort);
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... msgs) {
            Socket socket;
            String[] message = msgs[0].split(":");
            String messageType = msgs[1];
            if (messageType.equals(RECOVERY)) {
                String[] recoveryList = dynamoRing.getRecoveryContactList(MY_PORT);
                for (String port : recoveryList) {
                    try {
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(port));
                        socket.setSoTimeout(1500);
                        PrintStream printStream = new PrintStream(socket.getOutputStream());
                        String messageToSend = RECOVERY + ":" + MY_PORT;
                        Log.e("recoveryData", "message to send " + messageToSend);
                        printStream.println(messageToSend);

                        InputStreamReader inputStreamReader = new InputStreamReader(socket.getInputStream());
                        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
                        String messageReceived = bufferedReader.readLine();
                        Log.e("recoveryData", "message received " + messageReceived);
                        if (messageReceived.contains(RECOVERY_ACK)) {
                            String[] msg = messageReceived.split(":");
                            if (msg.length > 1) {
                                if (msg[1].equals(dynamoRing.getPredecessor(MY_PORT))) {
                                    ContentValues values = new ContentValues();
                                    String[] data = msg[2].split("<>");
                                    if (!data[0].equals("null")) {
                                        String[] content = data[0].split("==");
                                        for (int i = 0; i < content.length; i++) {
                                            predecessor1Data.put(content[i].split(";")[0],
                                                    content[i].split(";")[1]);
                                            values.put(KEY, content[i].split(";")[0]);
                                            values.put(VALUE, content[i].split(";")[1]);
                                        }
                                        dbWriter.insertWithOnConflict(SimpleDynamoContract.MessageEntry.TABLE_NAME, null,
                                                values, SQLiteDatabase.CONFLICT_REPLACE);
                                    }
                                    if (!data[1].equals("null")) {
                                        String[] content = data[1].split("==");
                                        for (int j = 0; j < content.length; j++) {
                                            predecessor2Data.put(content[j].split(";")[0],
                                                    content[j].split(";")[1]);
                                            values.put(KEY, content[j].split(";")[0]);
                                            values.put(VALUE, content[j].split(";")[1]);
                                        }
                                        dbWriter.insertWithOnConflict(SimpleDynamoContract.MessageEntry.TABLE_NAME, null,
                                                values, SQLiteDatabase.CONFLICT_REPLACE);
                                    }
                                } else if (msg[1].equals(dynamoRing.getSuccessor(MY_PORT))) {
                                    ContentValues values = new ContentValues();
                                    String[] content = msg[2].split("==");
                                    for (int j = 0; j < content.length; j++) {
                                        myData.put(content[j].split(";")[0],
                                                content[j].split(";")[1]);
                                        values.put(KEY, content[j].split(";")[0]);
                                        values.put(VALUE, content[j].split(";")[1]);
                                    }
                                    dbWriter.insertWithOnConflict(SimpleDynamoContract.MessageEntry.TABLE_NAME, null,
                                            values, SQLiteDatabase.CONFLICT_REPLACE);
                                }
                                Log.e("OnCreate", "Data Recovered");
                                for (String key : myData.keySet()) {
                                    Log.e("RecoveredData", "SuccessorMap " + key + "," + myData.get(key));
                                }
                                for (String key : predecessor1Data.keySet()) {
                                    Log.e("RecoveredData", "predecessor1 " + key + "," + predecessor1Data.get(key));
                                }
                                for (String key : predecessor2Data.keySet()) {
                                    Log.e("RecoveredData", "predecessor2 " + key + "," + predecessor2Data.get(key));
                                }
                            }
                            printStream.close();
                            socket.close();
                        }
                    } catch (NullPointerException ne) {
                        ne.printStackTrace();
                    } catch (SocketException se) {
                        se.printStackTrace();
                    } catch (SocketTimeoutException set) {
                        set.printStackTrace();
                    } catch (IOException io) {
                        io.printStackTrace();
                    }
                }
            } else if (messageType.equals(REPLICATE_TO_SUCCESSOR)) {
                try {
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(msgs[3]));
                    socket.setSoTimeout(1500);
                    PrintStream printStream = new PrintStream(socket.getOutputStream());
                    String messageToSend = REPLICATE_TO_SUCCESSOR + ":" + msgs[2] + ":" + message[0] + ":" + message[1];
                    Log.e("recoveryData", "message to send " + messageToSend);
                    printStream.println(messageToSend);

                    InputStreamReader inputStreamReader = new InputStreamReader(socket.getInputStream());
                    BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
                    String messageReceived = bufferedReader.readLine();
                    if (messageReceived != null) {
                        printStream.close();
                        socket.close();
                    }
                } catch (NullPointerException ne) {
                    ne.printStackTrace();
                } catch (SocketException se) {
                    se.printStackTrace();
                } catch (SocketTimeoutException set) {
                    set.printStackTrace();
                } catch (IOException io) {
                    io.printStackTrace();
                }
            }
            return null;
        }
    }

    private String genHash(String input) {
        MessageDigest sha1 = null;
        try {
            sha1 = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }
}
