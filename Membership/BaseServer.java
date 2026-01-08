package cs425.mp3;

import cs425.mp3.membership.Introducer;
import cs425.mp3.membership.Member;

import java.net.InetAddress;

/**
 * Starting point of the program
 */
public class BaseServer {

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            throw new IllegalArgumentException("Not enough arguments");
        }

        // Starting introducer only needs port number
        if (args.length == 1) {
            new Introducer(Integer.parseInt(args[0])).start();
        }

        // Starting member needs port number and introducer details
        else {
            int port = Integer.parseInt(args[0]);
            //introducer
            String introducerHost = args[1];
            int introducerPort = Integer.parseInt(args[2]);
            //master
            String masterHost = args[3];
            int masterPort = Integer.parseInt(args[4]);
            new Member(InetAddress.getLocalHost().getHostName(), port,
                    introducerHost, introducerPort,
                    masterHost, masterPort
            ).start();
        }
    }
}
