package cs425.mp3.BullyElection;

import cs425.mp3.MembershipList.MemberListEntry;
import cs425.mp3.Messages.Message;
import cs425.mp3.Messages.MessageType;
import cs425.mp3.membership.Member;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Iterator;

public class Election extends Thread{
    private Member sender;


    //Constructor
    public Election(Member sender) {
        this.sender = sender;
    }

    // If FD.detect_ID.isIntroducer => Call ElectionProcess
    public void sendMessage(Message message, MemberListEntry receiver) {
        try {
            Socket socket = new Socket(receiver.getHostname(), receiver.getPort());
            ObjectOutputStream sendData = new ObjectOutputStream(socket.getOutputStream());
            sendData.writeObject(message);
            sendData.flush();
            System.out.println(message.getMessageType() + ": Send message to " + receiver.getHostname());
            socket.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public void sendVictoryMessage() {
        Iterator<MemberListEntry> memberList = sender.getMemberList().iterator();
        sender.setNewMaster(sender.getHost(), sender.getPort());

        while(memberList.hasNext()) {
            Message message = new Message(MessageType.VICTORY, sender.selfEntry);
            sendMessage(message, memberList.next());
        }
    }


    public Message receiveMessage(int timeout) {
        Message response = null;
        try {
            Socket socket = new Socket(this.sender.getHost(), this.sender.getPort());
            socket.setSoTimeout(timeout);
            ObjectInputStream receiveData = new ObjectInputStream(socket.getInputStream());
            response = (Message) receiveData.readObject();
        } catch(IOException e){
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        return response;
    }


    // return 1 = restart election process, return 0 = Done!
    public int ElectionProcess () {
        int timeout = 1000;
        boolean wait_ElectionResponse = false;
        boolean wait_Victory = false;

        // Send Election message
        ArrayList<MemberListEntry> predecessors = sender.getMemberList().getAllPredecessor();

        // No higher ID than sender
        if (predecessors.isEmpty()) {
            sendVictoryMessage();
        }
        else {
            for (MemberListEntry preNode : predecessors) {
                Message message = new Message(MessageType.ELECTION, sender.selfEntry);
                sendMessage(message, preNode);
            }
            wait_ElectionResponse = true;

            //Wait for Election response
            while(true) {
                Message receive = receiveMessage(timeout);
                if (receive == null) {
                    if (wait_Victory) {
                        return 1;
                    }
                    sendVictoryMessage();
                    return 0;
                }
                else if (receive.getMessageType().equals(MessageType.ELECTION)) {
                    if (receive.getSubjectEntry().compareTo(sender.selfEntry) < 0) {
                        Message message = new Message(MessageType.ELECTION_RESPONSE, (MemberListEntry) null);
                        sendMessage(message, receive.getSubjectEntry());
                        return 1;
                    }
                }
                else if (wait_ElectionResponse && receive.getMessageType().equals(MessageType.ELECTION_RESPONSE)) {
                    wait_ElectionResponse = false;
                    wait_Victory = true;
                }
                else if (receive.getMessageType().equals(MessageType.VICTORY)) {
                    MemberListEntry new_master = receive.getSubjectEntry();
                    sender.setNewMaster(new_master.getHostname(), new_master.getPort());
                    return 0;
                }
            }
        }
        return 0;
    }

}
