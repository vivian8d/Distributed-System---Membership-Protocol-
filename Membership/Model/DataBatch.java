package cs425.mp3.Model;

import java.io.Serializable;
import java.util.Arrays;

public class DataBatch implements Serializable {
    private int startInd;
    private int endInd;
    private String[] input;

    public DataBatch(int startInd, int endInd, String[] input) {
        this.startInd = startInd;
        this.endInd = endInd;
        this.input = input;
    }

    public int getStartInd() {
        return startInd;
    }

    public int getEndInd() {
        return endInd;
    }

    public String[] getInput() {
        return input;
    }

    @Override
    public String toString() {
        return this.startInd + "\t" + this.endInd + "\t" + Arrays.toString(this.input);
    }
}
