package edu.buffalo.cse.cse486586.simpledynamo;

/**
 * Created by arnav on 5/1/17.
 */

public class ReplicaStorage {
    String originatorPort;
    String keyToPut;
    String valueToPut;

    public String getOriginatorPort() {
        return originatorPort;
    }
    public void setOriginatorPort(String originatorPort) {
        this.originatorPort = originatorPort;
    }
    public String getKeyToPut() {
        return keyToPut;
    }
    public void setKeyToPut(String keyToPut) {
        this.keyToPut = keyToPut;
    }
    public String getValueToPut() {
        return valueToPut;
    }
    public void setValueToPut(String valueToPut) {
        this.valueToPut = valueToPut;
    }
}
