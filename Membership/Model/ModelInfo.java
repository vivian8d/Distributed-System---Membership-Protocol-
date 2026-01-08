package cs425.mp3.Model;

import cs425.mp3.MembershipList.MemberListEntry;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ModelInfo implements Serializable {
    private List<MemberListEntry> entries;
    private double queryRate = 0;
    private int queryCount = 0;
    private List<Double> processTime = new ArrayList<>();
    private List<String> outputFiles = new ArrayList<>();
    private String outputFile;

    public ModelInfo() {
    }

    public ModelInfo(List<MemberListEntry> entries) {
        this.entries = entries;
    }

    public List<MemberListEntry> getEntries() {
        return entries;
    }

    public double getQueryRate() {
        return queryRate;
    }

    public int getQueryCount() {
        return queryCount;
    }

    public void setEntries(List<MemberListEntry> entries) {
        this.entries = entries;
    }

    public void setQueryRate(double queryRate) {
        this.queryRate = queryRate;
    }

    public void calculateQueryRate(int batchSize) {
        this.queryRate = (double) queryCount/(batchSize * processTime.get(processTime.size()-1));
    }

    public void setQueryCount(int queryCount) {
        this.queryCount = queryCount;
    }

    public void increaseQueryCount() {
        this.queryCount++;
    }

    public void addProcessingTime(double time) {
        this.processTime.add(time);
    }

    public List<Double> getProcessTime() {
        return processTime;
    }

    public void addOutput(String output) {
        this.outputFiles.add(output);
    }

    public List<String> getOutputFiles() {
        return outputFiles;
    }

    public String getOutputFile() {
        return outputFile;
    }

    public void setOutputFile(String outputFile) {
        this.outputFile = outputFile;
    }

    @Override
    public String toString() {
        return this.queryCount + "\t" + this.queryRate + "\t" + this.outputFiles + "\t" +
                entries.stream().map(Object::toString).collect(Collectors.joining(", ")) + "\t" +
                processTime.stream().map(Object::toString).collect(Collectors.joining(", "));
    }
}
