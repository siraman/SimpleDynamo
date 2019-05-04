package edu.buffalo.cse.cse486586.simpledynamo;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;

public class DynamoRing {
    private String NODE_5554 = genHash("5554");
    private String NODE_5556 = genHash("5556");
    private String NODE_5558 = genHash("5558");
    private String NODE_5560 = genHash("5560");
    private String NODE_5562 = genHash("5562");

    private String NODE_5554_PORT = "11108";
    private String NODE_5556_PORT = "11112";
    private String NODE_5558_PORT = "11116";
    private String NODE_5560_PORT = "11120";
    private String NODE_5562_PORT = "11124";

    private String[] remotePorts = {NODE_5562_PORT, NODE_5556_PORT, NODE_5554_PORT,NODE_5558_PORT,
                                    NODE_5560_PORT};

    private String[] nodesHash = {NODE_5562,NODE_5556,NODE_5554,NODE_5558,NODE_5560};

    private String[] replicaNode5554 = {NODE_5558_PORT,NODE_5560_PORT};
    private String[] replicaNode5556 = {NODE_5554_PORT,NODE_5558_PORT};
    private String[] replicaNode5558 = {NODE_5560_PORT,NODE_5562_PORT};
    private String[] replicaNode5560 = {NODE_5562_PORT,NODE_5556_PORT};
    private String[] replicaNode5562 = {NODE_5556_PORT,NODE_5554_PORT};

    private String[] recoveryNode5554 = {NODE_5558_PORT,NODE_5556_PORT,NODE_5562_PORT};
    private String[] recoveryNode5556 = {NODE_5554_PORT,NODE_5562_PORT,NODE_5560_PORT};
    private String[] recoveryNode5558 = {NODE_5560_PORT,NODE_5554_PORT,NODE_5556_PORT};
    private String[] recoveryNode5560 = {NODE_5562_PORT,NODE_5558_PORT,NODE_5554_PORT};
    private String[] recoveryNode5562 = {NODE_5556_PORT,NODE_5560_PORT,NODE_5558_PORT};

    private String[] recoveryContact5554 = {NODE_5558_PORT,NODE_5562_PORT,NODE_5556_PORT};
    private String[] recoveryContact5556 = {NODE_5554_PORT,NODE_5560_PORT,NODE_5562_PORT};
    private String[] recoveryContact5558 = {NODE_5560_PORT,NODE_5556_PORT,NODE_5554_PORT};
    private String[] recoveryContact5560 = {NODE_5562_PORT,NODE_5554_PORT,NODE_5558_PORT};
    private String[] recoveryContact5562 = {NODE_5556_PORT,NODE_5558_PORT,NODE_5560_PORT};

    public String[] getReplicationList(String port){
        switch(Integer.parseInt(port)) {
            case 11108:
                return replicaNode5554;
            case 11112:
                return replicaNode5556;
            case 11116:
                return replicaNode5558;
            case 11120:
                return replicaNode5560;
            case 11124:
                return replicaNode5562;
            default:
                return null;
        }
    }

    public String[] getRecoveryList(String port) {
        switch(Integer.parseInt(port)) {
            case 11108:
                return recoveryNode5554;
            case 11112:
                return recoveryNode5556;
            case 11116:
                return recoveryNode5558;
            case 11120:
                return recoveryNode5560;
            case 11124:
                return recoveryNode5562;
            default:
                return null;
        }
    }

    public String[] getRecoveryContactList(String port) {
        switch(Integer.parseInt(port)) {
            case 11108:
                return recoveryContact5554;
            case 11112:
                return recoveryContact5556;
            case 11116:
                return recoveryContact5558;
            case 11120:
                return recoveryContact5560;
            case 11124:
                return recoveryContact5562;
            default:
                return null;
        }
    }

    public String getSuccessor(String port) {
        switch(Integer.parseInt(port)) {
            case 11108:
                return "11116";
            case 11112:
                return "11108";
            case 11116:
                return "11120";
            case 11120:
                return "11124";
            case 11124:
                return "11112";
            default:
                return null;
        }
    }

    public String getPredecessor(String port) {
        switch(Integer.parseInt(port)) {
            case 11108:
                return "11112";
            case 11112:
                return "11124";
            case 11116:
                return "11108";
            case 11120:
                return "11116";
            case 11124:
                return "11120";
            default:
                return null;
        }
    }

    public String getCoordinator(String key) {
        for(int i = 0; i < nodesHash.length; i++) {
            if (key.compareTo(nodesHash[i]) < 0)
                return remotePorts[0];
            else if (i == nodesHash.length - 1 && key.compareTo(nodesHash[i]) > 0)
                return remotePorts[0];
            else if(key.compareTo(nodesHash[i]) > 0 && key.compareTo(nodesHash[i+1]) < 0) {
                return remotePorts[i+1];
            }
        }
        return null;
    }

    public String[] getAllNodesPort() {
        return remotePorts;
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
}
