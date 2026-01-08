package cs425.mp3.test;

import cs425.mp3.MembershipList.MemberListEntry;
import cs425.mp3.Messages.Message;
import cs425.mp3.Messages.MessageFile;
import cs425.mp3.Messages.MessageType;
import org.apache.commons.lang3.SerializationUtils;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Date;

public class Client {
    private static ObjectOutputStream dataOutputStream = null;
    private static ObjectInputStream dataInputStream = null;

    public static void main(String[] args) {
        try(Socket socket = new Socket("localhost",5000)) {
//            dataInputStream = new ObjectInputStream(socket.getInputStream());
            dataOutputStream = new ObjectOutputStream(socket.getOutputStream());

//            sendFile("/home/maipham/uiuc/cs425/sdfs/sdfs/src/main/java/cs425/mp3/test/test.txt");
            System.out.println("34");
            ArrayList<MemberListEntry> replicas = new ArrayList<>();
            replicas.add(new MemberListEntry("localhost", 5001, new Date()));
            Message message = new Message(MessageType.FileOpPut, "test.txt", "nweFile2.txt",
                    replicas);
            System.out.println("34");
            dataOutputStream.writeObject(message);
            dataOutputStream.flush();

            dataInputStream.close();
            dataInputStream.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private static void sendFile(String path) throws Exception {
        int bytes = 0;
        File file = new File(path);
        FileInputStream fileInputStream = new FileInputStream(file);

        // send file size
        dataOutputStream.writeLong(file.length());
        // break file into chunks
        byte[] buffer = new byte[4*1024];
        while ((bytes=fileInputStream.read(buffer))!=-1){
            dataOutputStream.write(buffer,0,bytes);
            dataOutputStream.flush();
        }
        fileInputStream.close();
    }
//    private static DataOutputStream dataOutputStream = null;
//    private static DataInputStream dataInputStream = null;
//
//    public static void main(String[] args) {
//        try(Socket socket = new Socket("localhost",5000)) {
//            dataInputStream = new DataInputStream(socket.getInputStream());
//            dataOutputStream = new DataOutputStream(socket.getOutputStream());
//
//            sendFile("/home/maipham/uiuc/cs425/sdfs/sdfs/src/main/java/cs425/mp3/test/test.txt");
//
////            Message message = new Message(MessageType.FileOpPut, "test.txt", "nweFile2.txt", new ArrayList<>());
////            dataOutputStream.write(SerializationUtils.serialize(message));
//
//            dataInputStream.close();
//            dataInputStream.close();
//        }catch (Exception e){
//            e.printStackTrace();
//        }
//    }
//
//    private static void sendFile(String path) throws Exception{
//        int bytes = 0;
//        File file = new File(path);
//        FileInputStream fileInputStream = new FileInputStream(file);
//
//        // send file size
//        dataOutputStream.writeLong(file.length());
//        // break file into chunks
//        byte[] buffer = new byte[4*1024];
//        while ((bytes=fileInputStream.read(buffer))!=-1){
//            dataOutputStream.write(buffer,0,bytes);
//            dataOutputStream.flush();
//        }
//        fileInputStream.close();
//    }

//    private static DataOutputStream dataOutputStream = null;
//    private static DataInputStream dataInputStream = null;

//    public static void main(String[] args) {
//        try(Socket socket = new Socket("localhost",5000)) {
//            dataInputStream = new DataInputStream(socket.getInputStream());
//            dataOutputStream = new DataOutputStream(socket.getOutputStream());
//
////            sendFile("/home/maipham/uiuc/cs425/sdfs/sdfs/src/main/java/cs425/mp3/test/test.txt");
//            int bytes = 0;
//            File file = new File("/home/maipham/uiuc/cs425/sdfs/sdfs/src/main/java/cs425/mp3/test/test.txt");
//            MessageFile message = new MessageFile(MessageType.FileOpPut, file);
//            ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
//
//            byte byteArray[] = SerializationUtils.serialize(message);
////            MessageFile message1 = (MessageFile) SerializationUtils.deserialize(byteArray);
//            System.out.println(byteArray);
////            System.out.println(message1.getFile().getName());
//            System.out.println(file.getTotalSpace());
//            outputStream.write(byteArray, 0, byteArray.length);
//            outputStream.flush();
//
//            dataInputStream.close();
//            dataInputStream.close();
//        }catch (Exception e){
//            e.printStackTrace();
//        }
//    }

}
