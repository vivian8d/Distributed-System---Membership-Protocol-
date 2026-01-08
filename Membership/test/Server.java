package cs425.mp3.test;

import cs425.mp3.Messages.Message;
import cs425.mp3.Messages.MessageFile;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.SerializationUtils;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;

public class Server {
    private static ObjectOutputStream dataOutputStream = null;
    private static ObjectInputStream dataInputStream = null;

    public static void main(String[] args) {
        try(ServerSocket serverSocket = new ServerSocket(5000)){
            System.out.println("listening to port:5000");
            Socket clientSocket = serverSocket.accept();
            System.out.println(clientSocket+" connected.");
            dataInputStream = new ObjectInputStream(clientSocket.getInputStream());
            dataOutputStream = new ObjectOutputStream(clientSocket.getOutputStream());

            Message message = (Message) dataInputStream.readObject();
            System.out.println(message.getMessageType());
            System.out.println(message.getSrcFileName());
            System.out.println(message.getReplicas().get(0).getHostname());

            dataInputStream.close();
            dataOutputStream.close();
            clientSocket.close();
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    private static void receiveFile(String fileName) throws Exception{
        int bytes = 0;
        FileOutputStream fileOutputStream = new FileOutputStream(fileName);

        long size = dataInputStream.readLong();     // read file size
        byte[] buffer = new byte[4*1024];
        while (size > 0 && (bytes = dataInputStream.read(buffer, 0, (int)Math.min(buffer.length, size))) != -1) {
            fileOutputStream.write(buffer,0,bytes);
            size -= bytes;      // read upto file size
        }
        fileOutputStream.close();
    }

//    private static DataOutputStream dataOutputStream = null;
//    private static DataInputStream dataInputStream = null;
//
//    public static void main(String[] args) {
//        try(ServerSocket serverSocket = new ServerSocket(5000)){
//            System.out.println("listening to port:5000");
//            Socket clientSocket = serverSocket.accept();
//            System.out.println(clientSocket+" connected.");
//            dataInputStream = new DataInputStream(clientSocket.getInputStream());
//            dataOutputStream = new DataOutputStream(clientSocket.getOutputStream());
//
//            receiveFile("NewFile1.txt");
////            receiveFile("NewFile2.pdf");
//
//            dataInputStream.close();
//            dataOutputStream.close();
//            clientSocket.close();
//        } catch (Exception e){
//            e.printStackTrace();
//        }
//    }
//
//    private static void receiveFile(String fileName) throws Exception{
//        int bytes = 0;
//        FileOutputStream fileOutputStream = new FileOutputStream(fileName);
//
//        long size = dataInputStream.readLong();     // read file size
//        byte[] buffer = new byte[4*1024];
//        while (size > 0 && (bytes = dataInputStream.read(buffer, 0, (int)Math.min(buffer.length, size))) != -1) {
//            fileOutputStream.write(buffer,0,bytes);
//            size -= bytes;      // read upto file size
//        }
//        fileOutputStream.close();
//    }

//    private static ObjectOutputStream outputStream = null;
//    private static ObjectInputStream inputStream = null;
//
//    public static void main(String[] args) {
//        try(ServerSocket serverSocket = new ServerSocket(5000)){
//            System.out.println("listening to port:5000");
//            Socket clientSocket = serverSocket.accept();
//            System.out.println(clientSocket+" connected.");
//            inputStream = new ObjectInputStream(clientSocket.getInputStream());
//
////            ByteArrayOutputStream baos = new ByteArrayOutputStream();
////            byte buffer[] = new byte[1024];
////            baos.write(buffer, 0 , inputStream.read(buffer));
////
////            byte result[] = baos.toByteArray();
//
//            byte[] data = new byte[1024];
//            int count = inputStream.read(data, 0, data.length);
//            int current = count;
//            do {
//                count = inputStream.read(data, current, (data.length-current));
//                if(count >= 0) current += count;
//            } while(count > -1);
//
//            MessageFile message = (MessageFile) SerializationUtils.deserialize(data);
//            System.out.println(message.getMessageType());
//            System.out.println(message.getFile().getTotalSpace());
//
//            System.out.println(data);
//            System.out.println(FileUtils.readLines(message.getFile()));
//
////            FileOutputStream bos = new FileOutputStream(message.getFile());
////            BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(bos);
////            bufferedOutputStream.write(data);
////            bufferedOutputStream.flush();
//
////            receiveFile("NewFile1.txt");
//
//            inputStream.close();
//            clientSocket.close();
//        } catch (Exception e){
//            e.printStackTrace();
//        }
//    }
}
