package cs425.mp3.Model;

import org.nd4j.common.util.ArrayUtil;

import java.io.*;

public class ImageData {
    public int num_row;
    public int num_col;
    public int [][]data;

    public ImageData() {}

    public ImageData(int num_row, int num_col) {
        this.data = new int[num_row][num_col];
    }



    public void setData(int num_row, int num_col, int value) {
        this.data[num_row][num_col] = value;
    }

    public void setNRow(int num_row) {
        this.num_row = num_row;
    }

    public void setNCol(int num_col) {
        this.num_col = num_col;
    }

    public ImageData getData(String filePath) {
        ImageData image;
        try {
            BufferedReader br = new BufferedReader(new FileReader(filePath));
            String[] all_row = br.readLine().split("@");
            image = new ImageData(all_row.length, all_row.length);


            for (int i=0; i < all_row.length; i++) {
                int temp[] = new int[all_row.length];
                int ind = 0;

                for (String s: all_row[i].split("!")) {
                    temp[ind] = Integer.parseInt(s);
                    ind ++ ;
                }
                image.data[i] = temp;
            }
//            System.out.println(image.data[0].length);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return image;
    }

    public ImageData[] preprocess_data(String filePath) throws IOException {
        DataInputStream dataReader = new DataInputStream(new BufferedInputStream(new FileInputStream(filePath)));
        int magic_num = dataReader.readInt();
        int num_image = dataReader.readInt();
        int num_row = dataReader.readInt();
        int num_col = dataReader.readInt();


        ImageData[] input = new ImageData[num_image];
        for (int i=0; i < num_image; i++  ) {
            ImageData image = new ImageData(num_row, num_col);
            String image_to_file = "";

            // MNIST dataset is row-wise
            for (int j=0; j < num_row; j ++) {
                for (int k=0; k < num_col; k ++) {
                    image_to_file  = dataReader.readUnsignedByte()  + "!";
//                    image.setData(j, k, dataReader.readUnsignedByte());
                }
                image_to_file = image_to_file.substring(0, image_to_file.length() - 1)  + '@';
            }
            writeToFile(image_to_file.substring(0, image_to_file.length() - 1), i + 1);
            input[i] = image;
        }

        dataReader.close();

        return input;
    }

    public void writeToFile (String image_data, int index) throws IOException {
        String fileName = "Dataset/MNIST/image_" +  index +  ".txt";
        BufferedWriter writer = new BufferedWriter(new FileWriter(fileName, false));
        writer.append(image_data);

        writer.close();
    }

}