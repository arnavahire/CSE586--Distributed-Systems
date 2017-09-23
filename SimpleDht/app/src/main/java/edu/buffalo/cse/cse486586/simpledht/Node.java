package edu.buffalo.cse.cse486586.simpledht;

import java.io.Serializable;

/**
 * Created by arnav on 4/7/17.
 */

public class Node {
    String pNumber;
    String node_id;
    String successor;
    String predecesssor;
    String successor_pno;
    String predeccessor_pno;

    public String getpNumber() {
        return pNumber;
    }

    public void setpNumber(String pNumber) {
        this.pNumber = pNumber;
    }

    public String getNode_id() {
        return node_id;
    }

    public void setNode_id(String node_id) {
        this.node_id = node_id;
    }

    public String getSuccessor() {
        return successor;
    }

    public void setSuccessor(String successor) {
        this.successor = successor;
    }

    public String getPredecesssor() {
        return predecesssor;
    }

    public void setPredecesssor(String predecesssor) {
        this.predecesssor = predecesssor;
    }

    public String getPredeccessor_pno() {
        return predeccessor_pno;
    }

    public void setPredeccessor_pno(String predeccessor_pno) {
        this.predeccessor_pno = predeccessor_pno;
    }

    public String getSuccessor_pno() {
        return successor_pno;
    }

    public void setSuccessor_pno(String successor_pno) {
        this.successor_pno = successor_pno;
    }


}
