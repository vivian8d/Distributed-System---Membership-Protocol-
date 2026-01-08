package cs425.mp3.FileOp;

import java.io.File;

public class FilePut {
    private File source;
    private String target;

    public FilePut(File source, String target) {
        this.source = source;
        this.target = target;
    }

    public File getSource() {
        return source;
    }

    public String getTarget() {
        return target;
    }
}
