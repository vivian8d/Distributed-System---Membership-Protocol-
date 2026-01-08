package cs425.mp3.FileOp;

import cs425.mp3.MembershipList.MemberListEntry;

import java.util.ArrayList;

public class FileOp {

    public enum Op {
        PUT,
        GET,
        GET_VERSION
    }

    private Op operation;
    private String srcFileName;
    private String desFileName;
    private ArrayList<MemberListEntry> entries;
    private int n;

    public FileOp(Op operation, String srcFileName, String desFileName,
                  ArrayList<MemberListEntry> entries, int n) {
        this.operation = operation;
        this.srcFileName = srcFileName;
        this.desFileName = desFileName;
        this.entries = entries;
        this.n = n;
    }

    public int getN() {
        return n;
    }

    public Op getOperation() {
        return operation;
    }

    public String getSrcFileName() {
        return srcFileName;
    }

    public String getDesFileName() {
        return desFileName;
    }

    public ArrayList<MemberListEntry> getEntries() {
        return entries;
    }
}
