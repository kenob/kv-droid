package edu.buffalo.cse.cse486586.simpledynamo;

/**
 * Created by kenob on 4/25/15.
 */
public class QuorumException extends Exception {

    public QuorumException(String detailMessage) {
        super(detailMessage);
    }
}
