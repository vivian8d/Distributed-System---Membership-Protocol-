package cs425.mp3.Messages;

import cs425.mp3.MembershipList.MemberListEntry;
import cs425.mp3.Model.DataBatch;
import cs425.mp3.Model.ModelInfo;

import javax.xml.crypto.Data;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Communication message. Contains message type and the member details
 */
public class Message implements Serializable {

    private MessageType messageType;
    private MemberListEntry subjectEntry;
    private String srcFileName;
    private String desFileName;
    private ArrayList<MemberListEntry> replicas;
    private byte[] content;
    private int n;
    private DataBatch dataBatch;
    private String model;
    private HashMap<String, ModelInfo> models;

    public Message(MessageType messageType, MemberListEntry subjectEntry) {
        this.messageType = messageType;
        this.subjectEntry = subjectEntry;
    }

    public Message(MessageType messageType, int batchSize) {
        this.messageType = messageType;
        this.n = batchSize;
    }

    public Message(MessageType messageType, String srcFileName, String desFileName, ArrayList<MemberListEntry> replicas) {
        this.messageType = messageType;
        this.srcFileName = srcFileName;
        this.desFileName = desFileName;
        this.replicas = replicas;
    }

    public Message(MessageType messageType, int n, String srcFileName, byte[] content, String desFileName) {
        this.srcFileName = srcFileName;
        this.n = n;
        this.messageType = messageType;
        this.content = content;
        this.desFileName = desFileName;
    }

    public Message(MessageType messageType, String desFileName, MemberListEntry subjectEntry) {
        this.messageType = messageType;
        this.desFileName = desFileName;
        this.subjectEntry = subjectEntry;
    }

    public Message(MessageType messageType, DataBatch dataBatch, String model) {
        this.messageType = messageType;
        this.dataBatch = dataBatch;
        this.model = model;
    }

    public Message(MessageType messageType, HashMap<String, ModelInfo> models) {
        this.messageType = messageType;
        this.models = models;
    }

    public HashMap<String, ModelInfo> getModels() {
        return models;
    }

    public int getN() {
        return n;
    }

    public byte[] getContent() {
        return content;
    }

    public String getSrcFileName() {
        return srcFileName;
    }

    public String getDesFileName() {
        return desFileName;
    }

    public DataBatch getDataBatch() {
        return dataBatch;
    }

    public ArrayList<MemberListEntry> getReplicas() {
        return replicas;
    }

    public void setReplicas(ArrayList<MemberListEntry> replicas) {
        this.replicas = replicas;
    }

    public void setMessageType(MessageType messageType) {
        this.messageType = messageType;
    }

    public MessageType getMessageType() {
        return messageType;
    }

    public MemberListEntry getSubjectEntry() {
        return subjectEntry;
    }

    public String getModel() {
        return model;
    }
}
