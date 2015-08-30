package edu.buffalo.cse.cse486586.simpledynamo;

import android.util.Log;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashSet;
import java.util.TreeMap;

/**
 * Created by keno on 4/21/15.
 */
public class Router {
    private static Router routerInstance;
    private TreeMap<String, String> nodeMap;
    private ArrayList<String> nodeList;
    public static final int NUM_REPLICAS = 3;
    public String myPort;
    private static final String TAG = "DYRouter";

    public static Router getInstance(){
        /*
            Returns a singleton router instance
        */
        if (routerInstance == null){
            routerInstance = new Router();
        }
        return routerInstance;
    }

    public void initMyPort(String node){
        /*
            Sets the port number of this node for
            route calculations
        */
        try {
            myPort = genHash(node);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }

    public void initNodes(String[] nodeArray){
        /*
            Initializes all known nodes (ports) to be used in the routing
            table. All nodes need to be known at the beginning,
            whether or not they are alive
        */
        nodeMap = new TreeMap<>();
        nodeList = new ArrayList<>();
        for (String nodeName : nodeArray){
            try {
                String hash = genHash(nodeName);
                nodeMap.put(hash, nodeName);
                nodeList.add(hash);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }
        Collections.sort(nodeList);
    }

    public String getCoordinator(String key, boolean hashed){
        /* 
            Returns the coordinator port for a given key either in the hashed or unhashed form
        */
        String hashedKey;
        try {
            hashedKey = genHash(key);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return null;
        }

        for (String node : nodeList){
            if (hashedKey.compareTo(node) <= 0){
                if (hashed) {
                    return node;
                }
                else {
                    return nodeMap.get(node);
                }
            }
        }

        //wrap around
        if (hashed) {
            return nodeList.get(0);
        }
        else {
            return nodeMap.get(nodeList.get(0));
        }
    }

    public ArrayList<String> getSuccessors(String node){
        /* 
            Returns the replica server addresses
            for a given node (port)
        */
        int position = 0;
        try {
            position = nodeList.indexOf(genHash(node));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }        ArrayList<String> result = new ArrayList<String>(NUM_REPLICAS - 1);

        while (result.size() < NUM_REPLICAS - 1){
            position = getNextIndex(position);
            result.add(nodeMap.get(nodeList.get(position)));
        }

        return result;
    }

    public ArrayList<String> getKeySuccessors(String key){
        /* 
            Returns the replica server addresses
            for a given key
        */
        String node = getCoordinator(key,true);
        int position = nodeList.indexOf(node);
        ArrayList<String> result = new ArrayList<String>(NUM_REPLICAS - 1);

        while (result.size() < NUM_REPLICAS - 1){
            position = getNextIndex(position);
            result.add(nodeMap.get(nodeList.get(position)));
        }
        return result;
    }

    public ArrayList<String> getGroup(String key){
        /* 
            Returns all the members of the replica group for a given key
        */
        String node = getCoordinator(key, false);

        int position = 0;
        try {
            position = nodeList.indexOf(genHash(node));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        ArrayList<String> result = new ArrayList<String>(NUM_REPLICAS);
        result.add(node);

        while (result.size() < NUM_REPLICAS){
            position = getNextIndex(position);
            result.add(nodeMap.get(nodeList.get(position)));
        }
        return result;
    }

    public HashSet<String> getNeighbors(String node){
        /* 
            Return all nodes that could possibly possess
            i) Keys for which this node is the coordinator
            ii) Keys for which this node is a replica server
        */
        int pBackward = 0;
        try {
            pBackward = nodeList.indexOf(genHash(node));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        int pForward = pBackward;
        HashSet<String> result = new HashSet<>();
        int count = 0;

        while (count < NUM_REPLICAS - 1){
            pForward = getNextIndex(pForward);
            pBackward = getPrevIndex(pBackward);
            result.add(nodeMap.get(nodeList.get(pForward)));
            result.add(nodeMap.get(nodeList.get(pBackward)));
            count += 1;
        }
        return result;
    }

    private int getNextIndex(int current){
        /* 
            Performs the wrap around function while attempting to get
            indexes of replica servers
         */
        return  (current + 1) % nodeList.size();
    }

    private int getPrevIndex(int current){
        /* 
            Performs the reverse wrap around function while attempting to get
           indexes of replica servers
         */
        int ret = current - 1;
        if (ret < 0){
            ret = nodeList.size() + ret;
        }
        return ret;
    }

    public int nodesFailed(String key){
        /* 
            Given a key, this method estimates the number of nodes that failed before
           this key was finally sent to me (i.e my position in the preference list)
         */
        int ret = 0;
        try {
            key = genHash(key);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return 0;
        }
        String coord = getCoordinator(key, true);
        int pos = nodeList.indexOf(coord);
        while (!coord.equals(myPort)){
            pos = getNextIndex(pos);
            coord = nodeList.get(pos);
            ret += 1;
        }
        return ret;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        /*
            Hash generation utility. The NoSuchAlgorighmException should never 
            actually be thrown, and so we shall take it lightly
        */
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}
