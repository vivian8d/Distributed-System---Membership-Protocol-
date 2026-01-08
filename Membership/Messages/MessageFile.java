package cs425.mp3.Messages;

import cs425.mp3.MembershipList.MemberListEntry;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;

public class MessageFile implements Serializable {
    private MessageType messageType;
    private String srcFileName;
    private String desFileName;
    private ArrayList<MemberListEntry> replicas = new ArrayList<>();

    public MessageFile(MessageType messageType, String srcFileName, String desFileName, ArrayList<MemberListEntry> replicas) {
        this.messageType = messageType;
        this.srcFileName = srcFileName;
        this.desFileName = desFileName;
        this.replicas = replicas;
    }

    public MessageType getMessageType() {
        return messageType;
    }

    public String getSrcFileName() {
        return srcFileName;
    }

    public String getDesFileName() {
        return desFileName;
    }

    public ArrayList<MemberListEntry> getReplicas() {
        return replicas;
    }
}
