package cs425.mp3.test;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class Utils {
    public static void main(String[] args) throws IOException {
        BufferedWriter br = new BufferedWriter(new FileWriter("testset.txt"));
        for (int i = 0; i < 10000; i++) {
            br.write("1.jpg" + "\n");
        }

        br.close();
    }
}
