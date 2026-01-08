package cs425.mp3;

import java.io.*;
import java.util.ArrayList;
import java.util.Scanner;

public class Utils {
    public static final long PROTOCOL_TIME = 1500;
    public static final int NUM_MONITORS = 1;
    public static final int NUM_REPLICAS = 4;
    public static final String versionFileName = "version_list.txt";
    public static String Model1 = "model_1";
    public static String Model2 = "model_2";

    public static String readFile(String filename) {
        String res = "";

        try {
            BufferedReader br = new BufferedReader(new FileReader(filename));
            String line = "";

            while ((line = br.readLine()) != null) {
                res += line + ";\n";
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return res;
    }

    public static void getVersion(int n, String sdfsPrefix, String sdfsFileName, File localFileName) {
        String get_version = "";
        ArrayList<String> list_filename = new ArrayList<>();
        if (!sdfsFileName.contains(sdfsPrefix)) {
            sdfsFileName = sdfsPrefix + sdfsFileName;
        }
        String list_version_file = sdfsFileName + "/" + versionFileName;

        try {
            File file = new File(list_version_file);
            Scanner sc = new Scanner(file);

            while (sc.hasNext()) {
                list_filename.add(sc.nextLine());
            }

            for (int i=0; i<n; i++){
                String filename = list_filename.get(list_filename.size() - i - 1);
                get_version += filename + ":\n" + Utils.readFile(filename);
            }

            BufferedWriter writer = new BufferedWriter(
                    new FileWriter(localFileName, false));
            writer.write(get_version);
            writer.close();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

//    public static void setBatchSize(int batchSize) {
//        Utils.batchSize = batchSize;
//    }
}
