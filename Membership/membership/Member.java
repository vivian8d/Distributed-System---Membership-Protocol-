package cs425.mp3.membership;

import cs425.mp3.FileOp.FileOp;
import cs425.mp3.MembershipList.MemberList;
import cs425.mp3.MembershipList.MemberListEntry;
import cs425.mp3.Messages.Message;
import cs425.mp3.Messages.MessageType;
import cs425.mp3.Model.DataBatch;
import cs425.mp3.Model.Inference;
import cs425.mp3.Model.ModelInfo;
import cs425.mp3.Utils;
import org.apache.commons.io.FileUtils;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;

import java.io.*;
import java.net.*;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.stream.Collectors;

public class Member {

    // Member & introducer info
    private String host;
    private int port;
    private Date timestamp;
    private String introducerHost;
    private int introducerPort;

    private String masterHost;
    private int masterPort;

    // Protocol settings
    private static String sdfsPrefix;

    // Sockets
    private ServerSocket server;
    private DatagramSocket socket;

    // Membership list and owner entry
    private volatile MemberList memberList;
    public MemberListEntry selfEntry;

    // operations
//    private volatile FileOp operation = null;


    // Threading resources
    private Thread mainProtocolThread;
    private Thread TCPListenerThread;

    private AtomicBoolean joined;
    private AtomicBoolean end;

    private File dfs;
    private volatile HashMap<String, List<MemberListEntry>> metadata = new HashMap<>();
    private HashMap<String, ModelInfo> metadataModel = new HashMap<>(); // model_name: list of members
    private HashMap<MemberListEntry, ArrayList<DataBatch>> entryDataBatch = new HashMap<>(); // node: batches to infer
    private HashMap<DataBatch, Boolean> isInferred = new HashMap<>(); // batch: is inferred successfully
//    private HashMap<String, Double> queryRate = new HashMap<>();
//    private HashMap<String>

    private int batchSize; // all nodes have batchsize
//    private int numQuery;

    // Logger
    public static Logger logger = Logger.getLogger("MemberLogger");


    // File
//    private String list_version_file;


    // Constructor and beginning of functionality
    public Member(String host, int port, String introducerHost, int introducerPort,
                  String masterHost, int masterPort) throws SecurityException, IOException {
        assert(host != null);
        assert(timestamp != null);

        this.host = host;
        this.port = port;

        this.introducerHost = introducerHost;
        this.introducerPort = introducerPort;

        this.masterHost = masterHost;
        this.masterPort = masterPort;

        joined = new AtomicBoolean();
        end = new AtomicBoolean();

        Handler fh = new FileHandler("./mp3_logs/member_" +port+ ".log");
        sdfsPrefix = "./sdfs_files_" + port+"/";
        dfs = new File(sdfsPrefix);
        fh.setFormatter(new SimpleFormatter());
        logger.setUseParentHandlers(false);
        logger.addHandler(fh);
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public MemberList getMemberList() {
        return memberList;
    }

    public void setNewMaster(String masterHost, int masterPort) {
        this.masterHost = masterHost;
        this.masterPort = masterPort;
    }

    // Process command line inputs
    public void start() throws ClassNotFoundException, InterruptedException {
        logger.info("Member process started");
        BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
        while(true) {
            try {
                System.out.print("MemberProcess$ ");
                String command = stdin.readLine();
                System.out.println();

                command = command.toLowerCase().trim();
                String[] commandInfo = command.split(" ");

                if (commandInfo.length > 1) {
                    String fileOp = commandInfo[0];
                    String sdfsFileName;
                    String localFileName;

                    switch (fileOp) {
                        case "put":
                            if (commandInfo.length == 3) {
                                localFileName = commandInfo[1];
                                sdfsFileName = commandInfo[2];
                                put(localFileName, sdfsFileName);
                            } else if (commandInfo.length == 4 && Objects.equals(commandInfo[1], "-r")) {
                                // put -r <dir>
                                String dir = commandInfo[2];
                                for (int i = 1; i <= 10000; i++) {
                                    String fileName = dir + "/" + i +".jpg";
                                    put(fileName, i +".jpg");
                                }
//                                File dir = new File(commandInfo[2]);
//                                File[] directoryListing = dir.listFiles();
//                                if (directoryListing != null) {
//                                    Arrays.sort(directoryListing, (a, b) -> a.getName().compareTo(b.getName()));
//                                    int i = 0;
//                                    for (File child : directoryListing) {
//                                        i ++;
//                                        if (i >= 1000) break;
//                                        System.out.println(child.getPath());
//                                        put(child.getPath(), child.getName());
//                                    }
//                                } else {
//                                    System.out.println("This is not directory");
//                                }
                            } else break;

                            break;
                        case "get":
                            if (commandInfo.length != 3) break;
                            sdfsFileName = commandInfo[1];
                            localFileName = commandInfo[2];
                            get(sdfsFileName, localFileName, FileOp.Op.GET, 0);
                            break;
                        case "delete":
                            if (commandInfo.length != 2) break;
                            sdfsFileName = commandInfo[1];
                            delete(sdfsFileName);
                            break;
                        case "ls":
                            if (commandInfo.length != 2) break;
                            sdfsFileName = commandInfo[1];
                            ls(sdfsFileName);
                            break;
                        case "get-versions":
                            if (commandInfo.length != 4) break;
                            sdfsFileName = commandInfo[1];
                            int numVer = Integer.parseInt(commandInfo[2]);
                            localFileName = commandInfo[3];
                            System.out.println(new Date().getTime());
                            get(sdfsFileName, localFileName, FileOp.Op.GET_VERSION, numVer);
                            System.out.println(new Date().getTime());
                            break;
                        case "C1":
                            getMetadataModel();
                            System.out.println("Number of jobs: " + metadataModel.size());
                            for (String modelName: metadataModel.keySet()) {
                                System.out.println(modelName + ": ");
                                System.out.println("\t Query count - " + metadataModel.get(modelName).getQueryCount());
                                System.out.println("\t Query rate - " + metadataModel.get(modelName).getQueryRate());
                            }
                            break;
                        case "C2":
                            getMetadataModel();
                            System.out.println("Number of jobs: " + metadataModel.size());
                            for (String modelName: metadataModel.keySet()) {
                                List<Double> time = metadataModel.get(modelName).getProcessTime();
                                System.out.println(modelName + " - processing time: ");
                                DoubleSummaryStatistics dstats = time.stream().
                                        collect(DoubleSummaryStatistics::new, DoubleSummaryStatistics::accept,
                                                DoubleSummaryStatistics::combine);
                                System.out.println("Max:"+dstats.getMax()+", Min:"+dstats.getMin());
                                System.out.println("Count:"+dstats.getCount()+", Sum:"+dstats.getSum());
                                System.out.println("Average:"+dstats.getAverage());
                            }
                            break;
                        case "C3":
                            if (commandInfo.length != 2) break;
                            int batchSize = Integer.parseInt(commandInfo[1].split("=")[1]);
                            this.batchSize = batchSize;
                            disseminateMessage(new Message(MessageType.BatchSize, batchSize));
                            break;
                        case "C4":
                            getMetadataModel();
                            System.out.println("Number of jobs: " + metadataModel.size());
                            for (String modelName: metadataModel.keySet()) {
                                String outFile = metadataModel.get(modelName).getOutputFile();
                                System.out.println(modelName + " - sdfs outputFile: " + outFile);
                            }
                            break;
                        case "C5":
                            getMetadataModel();
                            System.out.println("Number of jobs: " + metadataModel.size());
                            for (String modelName: metadataModel.keySet()) {
                                List<MemberListEntry> entries = metadataModel.get(modelName).getEntries();
                                System.out.println(modelName + " - current VMs running: " +
                                        entries.stream().map(MemberListEntry::toString).collect(Collectors.joining(", ")));

                            }
                            break;
                        case "infer":
                            // infer input output batchSize=20
                            if (commandInfo.length != 4) break;
                            String inputFile = commandInfo[1];
                            String resultFie = commandInfo[2];
                            batchSize = Integer.parseInt(commandInfo[3].split("=")[1]);
                            this.batchSize = batchSize;
                            inference(inputFile, batchSize, resultFie);
                            break;
                        default:
                            System.out.println("Unrecognized command, type 'join', 'leave', 'list_mem', 'list_self'" +
                                    " or file op command: 'put', 'get', 'delete', 'ls', or 'get-versions'");
                            break;
                    }

                } else {
                    switch (command) {
                        case "join":
                            joinGroup();
                            break;

                        case "leave":
                            leaveGroup(true);
                            break;

                        case "list_mem":
                            if (joined.get()) {
                                System.out.println(memberList);
                            } else {
                                System.out.println("Not joined");
                            }
                            break;

                        case "list_self":
                            if (joined.get()) {
                                System.out.println(selfEntry);
                            } else {
                                System.out.println("Not joined");
                            }
                            break;
                        case "store":
                            if (joined.get()) {
                                /*
                                List files name in directory
                                 */
                                String[] filenames = dfs.list();
                                System.out.println("List files are currently stored at this machine: ");
                                if (filenames == null || filenames.length == 0) {
                                    System.out.println("No files!");
                                } else {
                                    for (String filename: filenames) {
                                        System.out.println(filename);
                                    }
                                }
                            }
                            break;

                        default:
                            System.out.println("Unrecognized command, type 'join', 'leave', 'list_mem', 'list_self'" +
                                    " or file op command: 'put', 'get', 'delete', 'ls', or 'get-versions'");
                            break;
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println();
        }
    }

    private void getMetadataModel() throws IOException, ClassNotFoundException {
        Socket socket1 = new Socket(masterHost, masterPort);
        ObjectOutputStream outputStream = new ObjectOutputStream(socket1.getOutputStream());
        ObjectInputStream inputStream = new ObjectInputStream(socket1.getInputStream());

        Message message = new Message(MessageType.MetadataModel, new HashMap<>());
        outputStream.writeObject(message);

        Message reply = (Message) inputStream.readObject();
        metadataModel = reply.getModels();
        outputStream.close();
        inputStream.close();
        socket1.close();
    }

    /*
    Put localFileName to sdfsFileName
     */
    
    private void inference(String inputFile, int batchSize, String outputFile) {
        try {
            Socket inferSocket = new Socket(masterHost, masterPort);
            ObjectOutputStream outputStream = new ObjectOutputStream(inferSocket.getOutputStream());
            Message message = new Message(MessageType.InferInfo, batchSize, inputFile, null, outputFile);

            outputStream.writeObject(message);
            outputStream.flush();
            logger.info("INFER_INFO: Send infer info" + inputFile + " with " + batchSize + " from client to master");
            
            inferSocket.close();
            outputStream.close();
            
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    private void put(String localFileName, String sdfsFileName) {
        try {
            Socket sendFileSocket = new Socket(masterHost, masterPort);
            ObjectOutputStream outputStream = new ObjectOutputStream(sendFileSocket.getOutputStream());
            ObjectInputStream inputStream = new ObjectInputStream(sendFileSocket.getInputStream());
            /*
            Send FileOpPutRequest to master to get list nodes
             */
            Message message = new Message(MessageType.GetReplicasToPut, localFileName, sdfsFileName, null);
            outputStream.writeObject(message);
            outputStream.flush();

            logger.info("GET_REPLICAS_TO_PUT: Sent request put " + localFileName +" to master " + masterHost + " " + masterPort);

            Message returnMessage = (Message) inputStream.readObject();

            if (returnMessage.getMessageType() == MessageType.ReturnPutReplicas) {
                logger.info("RETURN_PUT_REPLICAS: Get " +returnMessage.getReplicas().size() +
                        " replica nodes from master successfully. Prepare to sent file to nodes");
                FileOp operation = new FileOp(FileOp.Op.PUT, returnMessage.getSrcFileName(),
                        returnMessage.getDesFileName(), returnMessage.getReplicas(), 0);
                operateFile(operation);
            }

            sendFileSocket.close();
            outputStream.close();
            inputStream.close();

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

    }

    private void get(String sdfsFileName, String localFileName, FileOp.Op op, int numVer) {
        try {
            Socket getFileSocket = new Socket(masterHost, masterPort);
            ObjectOutputStream outputStream = new ObjectOutputStream(getFileSocket.getOutputStream());
            ObjectInputStream inputStream = new ObjectInputStream(getFileSocket.getInputStream());

            // get list nodes saved sdfsFileName

            Message message = new Message(MessageType.GetReplicasToGet, sdfsFileName, localFileName, null);
            outputStream.writeObject(message);
            outputStream.flush();

            logger.info("GET_REPLICAS_TO_GET: Sent request get " + sdfsFileName +" to master " + masterHost + " " + masterPort);

            Message returnMessage = (Message) inputStream.readObject();

            ArrayList<MemberListEntry> replicas = returnMessage.getReplicas();
            if (returnMessage.getMessageType() == MessageType.ReturnGetReplicas) {
                if (replicas == null) {
                    logger.info("RETURN_GET_REPLICAS: Cannot find any replicas of files");
                } else {
                    logger.info("RETURN_GET_REPLICAS: Get " + replicas.size() +
                            " replica nodes from master successfully. Prepare to get file from nodes");
                    FileOp operation = new FileOp(op, returnMessage.getSrcFileName(),
                            returnMessage.getDesFileName(), replicas, numVer);
                    operateFile(operation);
                }
            }

            getFileSocket.close();
            outputStream.close();
            inputStream.close();

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private void delete(String sdfsFileName) {

        try {
            Socket deleteSocket = new Socket(masterHost, masterPort);
            ObjectOutputStream outputStream = new ObjectOutputStream(deleteSocket.getOutputStream());
            ObjectInputStream inputStream = new ObjectInputStream(deleteSocket.getInputStream());

            // send message delete

            Message message = new Message(MessageType.RequestDelete, sdfsFileName, null);
            outputStream.writeObject(message);
            outputStream.flush();

            logger.info("REQUEST_DELETE: Sent delete request to master " + masterHost + ":" + masterPort);

            deleteSocket.close();
            outputStream.close();
            inputStream.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void ls(String sdfsFileName) {
        try {
            Socket lsSocket = new Socket(masterHost, masterPort);
            ObjectOutputStream outputStream = new ObjectOutputStream(lsSocket.getOutputStream());
            ObjectInputStream inputStream = new ObjectInputStream(lsSocket.getInputStream());

            // get list nodes saved sdfsFileName

            Message message = new Message(MessageType.GetReplicasToGet, sdfsFileName, "", null);
            outputStream.writeObject(message);
            outputStream.flush();

            logger.info("LS: Sent request ls " + sdfsFileName +" to master " + masterHost + " " + masterPort);

            Message returnMessage = (Message) inputStream.readObject();

            ArrayList<MemberListEntry> replicas = returnMessage.getReplicas();
            System.out.println("List of replicas of " + sdfsFileName);
            if (returnMessage.getMessageType() == MessageType.ReturnGetReplicas) {
                if (replicas == null) {
                    logger.info("RETURN_GET_REPLICAS: Cannot find any replicas of files");
                } else {
                    logger.info("RETURN_GET_REPLICAS: Get " + replicas.size() +
                            " replica nodes from master successfully");
                    for (MemberListEntry entry: replicas) {
                        System.out.println(entry.getHostname()+":"+entry.getPort());
                    }
                }
            }

            lsSocket.close();
            outputStream.close();
            inputStream.close();

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private void joinGroup() throws IOException, ClassNotFoundException {
        // Do nothing if already joined
        if(joined.get()) {
            return;
        }

        /*
        Create dir for storing
         */
        if (dfs.mkdir()) {
            logger.info("Directory dfs has been created successfully");
        } else {
            FileUtils.deleteDirectory(dfs);
            dfs.mkdir();
        }

        logger.info("Member process received join command");

        end.set(false);

        // Initialize incarnation identity
        this.timestamp = new Date();
        this.selfEntry = new MemberListEntry(host, port, timestamp);

        logger.info("New entry created: " + selfEntry);

        // Get member already in group
        MemberListEntry groupProcess = getGroupProcess();

        if (groupProcess != null) {
            // Get member list from group member and add self
            memberList = requestMemberList(groupProcess);
            memberList.addNewOwner(selfEntry);
            logger.info("Retrieved membership list");
        } else {
            // This is the first member of the group
            logger.info("First member of group");
            memberList = new MemberList(selfEntry);
        }

        // Start sockets/listeners
        server = new ServerSocket(port);
        TCPListenerThread = new Thread(new Runnable() {
            @Override
            public void run() {
                Member.this.TCPListener();
            }
        });
        TCPListenerThread.start();
        logger.info("TCP Server started");

        // Communicate join
        disseminateMessage(new Message(MessageType.Join, selfEntry));
        logger.info("Process joined");

        // Start main protocol
        mainProtocolThread = new Thread(new Runnable() {
            @Override
            public void run() {
                Member.this.mainProtocol();
            }
        });
        mainProtocolThread.setDaemon(true);
        mainProtocolThread.start();
        logger.info("cs425.mp3.Model.Main protocol started");

        joined.set(true);
    }

    // Fetch member details of member already present in group
    private MemberListEntry getGroupProcess() throws IOException, ClassNotFoundException {
        Socket introducer = new Socket(introducerHost, introducerPort);
        logger.info("Connected to " + introducer);
        ObjectOutputStream output = new ObjectOutputStream(introducer.getOutputStream());
        ObjectInputStream input = new ObjectInputStream(introducer.getInputStream());

        // Send self entry to introducer
        output.writeObject(selfEntry);
        output.flush();

        logger.info("JOIN: Sent " + ObjectSize.sizeInBytes(selfEntry) + " bytes over TCP to introducer");

        // receive running process
        MemberListEntry runningProcess = (MemberListEntry) input.readObject();

        // Close resources
        input.close();
        output.close();
        introducer.close();

        logger.info("Connection to introducer closed");

        return runningProcess;
    }

    // Fetch membership details from a member already in group
    private MemberList requestMemberList(MemberListEntry groupProcess) throws IOException, ClassNotFoundException {
        Socket client = new Socket(groupProcess.getHostname(), groupProcess.getPort());
        ObjectOutputStream output = new ObjectOutputStream(client.getOutputStream());
        ObjectInputStream input = new ObjectInputStream(client.getInputStream());


        // Request membership list
        Message message = new Message(MessageType.MemberListRequest, selfEntry);

        output.writeObject(message);
        output.flush();
        logger.info("JOIN: Sent " + ObjectSize.sizeInBytes(message) + " bytes over TCP for membership list request");

        MemberList retrievedList = (MemberList) input.readObject();

        // Close resources
        input.close();
        output.close();
        client.close();

        return retrievedList;
    }

    private void leaveGroup(boolean sendMessage) throws IOException, InterruptedException {
        // Do nothing if not joined
        if (!joined.get()) {
            return;
        }

        FileUtils.deleteDirectory(dfs);

        logger.info("Leave command received");

        // Disseminate leave if necessary
        if (sendMessage) {
            disseminateMessage(new Message(MessageType.Leave, selfEntry));
            logger.info("Request to leave disseminated");
        }

        // Close resources
        end.set(true);

        mainProtocolThread.join();
        logger.info("cs425.mp3.Model.Main Protocol stopped");

        server.close();
        TCPListenerThread.join();
        logger.info("TCP server closed");

        memberList = null;
        selfEntry = null;
        
        logger.info("Process left");
        joined.set(false);
    }

    // Uses fire-and-forget paradigm
    public void disseminateMessage(Message message) {
        synchronized (memberList) {
            for (MemberListEntry entry: memberList) {
                // Don't send a message to ourself
                if (entry.equals(selfEntry)) {
                    continue;
                }

                try {
                    // Open resources
                    Socket groupMember = new Socket(entry.getHostname(), entry.getPort());
                    ObjectOutputStream output = new ObjectOutputStream(groupMember.getOutputStream());
                    ObjectInputStream input = new ObjectInputStream(groupMember.getInputStream());

                    // Send message
                    output.writeObject(message);
                    output.flush();

                    switch (message.getMessageType()) {
                        case Join:
                            logger.info("JOIN: Sent " + ObjectSize.sizeInBytes(message) + " bytes over TCP for dissemination");
                            break;
                        case Leave:
                            logger.info("LEAVE: Sent " + ObjectSize.sizeInBytes(message) + " bytes over TCP for dissemination");
                            break;
                        case Crash:
                            logger.info("CRASH: Sent " + ObjectSize.sizeInBytes(message) + " bytes over TCP for dissemination");
                            break;
                        case VICTORY:
                            logger.info("VICTORY: Sent " + ObjectSize.sizeInBytes(message) + " bytes over TCP for dissemination");
                            break;
                        case BatchSize:
                            logger.info("BATCH: Sent " + ObjectSize.sizeInBytes(message) + " bytes over TCP for dissemination");
                            break;
                        default:
                            assert(false);
                    }

                    // Close resources
                    input.close();
                    output.close();
                    groupMember.close();
                } catch (IOException e) {
                    continue;
                }
            }
        }
    }


    public void operateFile(FileOp operation) throws IOException {

        Message message;
        File file;

        switch (operation.getOperation()) {
            case PUT:
                logger.info("PUT: Starting to send file " + operation.getSrcFileName() +
                        " to " + operation.getEntries().size() +" replicas: ");

                file = new File(operation.getSrcFileName());
                message = new Message(MessageType.File, 0, operation.getSrcFileName(),
                        Files.readAllBytes(file.toPath()), operation.getDesFileName());
                for (MemberListEntry entry: operation.getEntries()) {
                    try {
                        // Open resources
                        Socket sendSocket = new Socket(entry.getHostname(), entry.getPort());
                        ObjectOutputStream output = new ObjectOutputStream(sendSocket.getOutputStream());
                        ObjectInputStream input = new ObjectInputStream(sendSocket.getInputStream());

                        output.writeObject(message);
                        output.flush();
                        logger.info("PUT_FILE: Sent file" + operation.getSrcFileName() +
                                        " to " + entry.getHostname() + " " + entry.getPort());

                        // Close resources
                        input.close();
                        output.close();
                        sendSocket.close();
                    } catch (IOException e) {
                        continue;
                    }
                }
                break;

            case GET:
                logger.info("GET: Starting to get file " + operation.getSrcFileName() +
                        " from one of " + operation.getEntries().size() +" replicas to " + operation.getDesFileName());

                message = new Message(MessageType.RequestFile, 0, operation.getSrcFileName(),
                        null, operation.getDesFileName());
                file = new File(operation.getDesFileName());
                for (MemberListEntry entry: operation.getEntries()) {
                    if (file.exists() && file.length() != 0) break;
                    try {
                        // Open resources
                        logger.info("DEBUG: Prepare to send request file " + operation.getSrcFileName() +
                                " to " + entry.getHostname()+":"+entry.getPort()+ " file " + operation.getDesFileName());
                        Socket sendSocket = new Socket(entry.getHostname(), entry.getPort());
                        ObjectOutputStream output = new ObjectOutputStream(sendSocket.getOutputStream());
                        ObjectInputStream input = new ObjectInputStream(sendSocket.getInputStream());

                        output.writeObject(message);
                        output.flush();
                        logger.info("REQUEST_GET: Sent request file " + operation.getSrcFileName() +
                                " to " + entry.getHostname() + " " + entry.getPort());

                        Message fileMess = (Message) input.readObject();
                        logger.info("FILE: received file. Write file " + fileMess.getDesFileName() + " to local ");
                        Files.write(file.toPath(), fileMess.getContent());

                        input.close();
                        output.close();
                        sendSocket.close();
                    } catch (IOException e) {
                        continue;
                    } catch (ClassNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                }
                break;
            case GET_VERSION:
                logger.info("GET_VERSION: Starting to get file version " + operation.getSrcFileName() +
                        " from one of " + operation.getEntries().size() +" replicas");
                message = new Message(MessageType.RequestVersion, operation.getN(), operation.getSrcFileName(),
                        null, operation.getDesFileName());
                file = new File(operation.getDesFileName());
                for (MemberListEntry entry: operation.getEntries()) {
                    if (file.exists()) break;
                    try {
                        // Open resources
                        logger.info("DEBUG: Prepare to send request file version to " + entry.getHostname()+":"+entry.getPort());
                        Socket sendSocket = new Socket(entry.getHostname(), entry.getPort());
                        ObjectOutputStream output = new ObjectOutputStream(sendSocket.getOutputStream());
                        ObjectInputStream input = new ObjectInputStream(sendSocket.getInputStream());

                        output.writeObject(message);
                        output.flush();
                        logger.info("REQUEST_VERSION: Sent request file version " + operation.getSrcFileName() +
                                " to " + entry.getHostname() + " " + entry.getPort());

                        Message fileMess = (Message) input.readObject();
                        logger.info("FILE_VERSION: received file. Write file " + fileMess.getDesFileName() + " to local");
                        Files.write(file.toPath(), fileMess.getContent());

                        input.close();
                        output.close();
                        sendSocket.close();
                    } catch (IOException e) {
                        continue;
                    } catch (ClassNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                }
                break;
        }
    }

    // Thread methods
    private void TCPListener() {
        while (!end.get()) {
            try {
                Socket client = server.accept();
                logger.info("TCP connection established from " + client.toString());

                // Process message in own thread to prevent race condition on membership list
                synchronized (TCPListenerThread) {
                    Thread processMessageThread = new Thread(new Runnable() {
                        @Override
                        public void run() {
                            Member.this.processTCPMessage(client);
                        }
                    });
                    processMessageThread.start();
                }
            } catch(SocketException e) {
                break;
            } catch (Exception e) {
                continue;
            }
        }
        
    }

    // Process join leave or crash message
    private void processTCPMessage(Socket client) {
        try {
            ObjectOutputStream output = new ObjectOutputStream(client.getOutputStream());
            ObjectInputStream input = new ObjectInputStream(client.getInputStream());

            // Recieve message
            Message message = (Message) input.readObject();

            // perform appropriate action
            switch(message.getMessageType()) {
                case Join:
                    logger.info("Received message for process joining group: " + message.getSubjectEntry());
                    synchronized (memberList) {
                        if (memberList.addEntry(message.getSubjectEntry())) {
                            logger.info("Process added to membership list: " + message.getSubjectEntry());
                        }
                    }
                    break;
                case Leave:
                    logger.info("Received message for process leaving group: " + message.getSubjectEntry());
                    synchronized (memberList) {
                        if (memberList.removeEntry(message.getSubjectEntry())) {
                            logger.info("Process removed from membership list: " + message.getSubjectEntry());
                        }
                    }
                    break;
                case VICTORY:
                    logger.info("Received message for process becoming new master group: " + message.getSubjectEntry());
                    setNewMaster(message.getSubjectEntry().getHostname(), message.getSubjectEntry().getPort());
                    break;
                case BatchSize:
                    logger.info("Received message for process setting batchSize: " + message.getSubjectEntry());
                    this.batchSize = message.getN();
                    break;
                case MetadataModel:
                    logger.info("Received message for get models info: " );
                    Message reply = new Message(MessageType.MetadataModel, metadataModel);
                    output.writeObject(reply);
                    break;
                case GetOutput:
                    logger.info("Received message for get models output " );
                    for (String model: metadataModel.keySet()) {
                        List<String> outputs = metadataModel.get(model).getOutputFiles();
                        File tmpInf = File.createTempFile(model, "_output");
                        BufferedWriter writer = new BufferedWriter(new FileWriter(tmpInf.getPath(), false));
                        writer.write(String.join("\n", outputs));
                        writer.close();
                        String sdfsName = "out_testset_" + model;
                        put(tmpInf.getPath(), sdfsName);
                        metadataModel.get(model).setOutputFile(sdfsName);
                    }
                    output.writeObject(new Message(MessageType.GetOutput, metadataModel));
                    break;
                case Crash:
                    logger.warning("Message received for crashed process: " + message.getSubjectEntry());
                    if (selfEntry.equals(message.getSubjectEntry())) {
                        // False crash of this node detected
                        System.out.println("\nFalse positive crash of this node detected. Stopping execution.\n");
                        logger.warning("False positive crash of this node detected. Stopping execution.");

                        // Leave group silently
                        leaveGroup(false);

                        // Command prompt
                        System.out.print("MemberProcess$ ");
                        break;
                    }
                    synchronized (memberList) {
                        if (memberList.removeEntry(message.getSubjectEntry())) {
                            logger.info("Process removed from membership list: " + message.getSubjectEntry());
                            if (message.getSubjectEntry().getHostname().equals(masterHost) &&
                                    message.getSubjectEntry().getPort() == masterPort) {
                                for (MemberListEntry member: memberList.getMemberList()) {
                                    if (member.getHostname().equals(masterHost) && member.getPort() == masterPort) {
                                        MemberListEntry newMaster = memberList.getSuccessor(member);
                                        setNewMaster(newMaster.getHostname(), newMaster.getPort());
                                        Message victoryMess = new Message(MessageType.VICTORY, newMaster);
                                        disseminateMessage(victoryMess);
                                        break;
                                    }
                                }

                            }

                            if (selfEntry.getHostname().equals(masterHost) && selfEntry.getPort() == masterPort) {
                                for (String file: metadata.keySet()) {
                                    ArrayList<MemberListEntry> values = (ArrayList<MemberListEntry>) metadata.get(file);
                                    if (values.contains(message.getSubjectEntry())) {
                                        values.remove(message.getSubjectEntry());
                                        MemberListEntry reRep = memberList.getMemberList().stream()
                                                .filter(u -> !values.contains(u))
                                                .collect(Collectors.toList()).get(0);
                                        MemberListEntry first = values.get(0);
                                        Socket reRepSock = new Socket(first.getHostname(), first.getPort());
                                        ObjectOutputStream outputStream = new ObjectOutputStream(reRepSock.getOutputStream());
                                        ObjectInputStream inputStream = new ObjectInputStream(reRepSock.getInputStream());
                                        logger.info("RE_REPLICATE: Sent re_replicate file from "+
                                                first.getHostname() + ":"+getPort() + " " + file +
                                                " to "+ reRep.getHostname()+ ":"+reRep.getPort());
                                        outputStream.writeObject(
                                                new Message(MessageType.RequestReplicate, file, reRep)
                                        );
                                        outputStream.flush();
//                                        inputStream.close();
//                                        outputStream.close();
//                                        reRepSock.close();
                                    }
                                }
                            }
                        }
                    }
                    break;
                case MemberListRequest:
                    synchronized (memberList) {
                        output.writeObject(memberList);
                        output.flush();
                        logger.info("JOIN: Sent " + ObjectSize.sizeInBytes(message) + " bytes over TCP containing membership list");
                    }
                    break;
                // Do nothing
                case IntroducerCheckAlive:
                    break;
                case GetReplicasToPut:
                    /*
                    From master, send nodes
                     */
                    logger.info("PUT_REQUEST: master - received put request");
                    synchronized (memberList) {
                        ArrayList<MemberListEntry> replicas;
                        if (metadata.get(message.getDesFileName()) == null) {
                            MemberListEntry memberToPut = memberList.getRandomMember();
                            logger.info("DEBUG: Randomly select " + memberToPut.getHostname()+ ":"+
                                    memberToPut.getPort()+" and its successors as replicas");
                            if (memberList.size() < Utils.NUM_REPLICAS) {
                                replicas = memberList.getSuccessors(memberToPut, memberList.size());
                            } else {
                                replicas = memberList.getSuccessors(memberToPut, Utils.NUM_REPLICAS);
                            }
                        } else {
                            replicas = (ArrayList<MemberListEntry>) metadata.get(message.getDesFileName());
                        }

                        Message repMess = new Message(MessageType.ReturnPutReplicas, message.getSrcFileName(),
                                message.getDesFileName(), replicas);
                        output.writeObject(repMess);
                        output.flush();
                        logger.info("RETURN_PUT_REPLICAS: Sent list replicas of " +replicas.size()+ " to client over TCP");
                    }
                    break;
                case GetReplicasToGet:
                    logger.info("GET_REQUEST: master - received get request");
                    synchronized (memberList) {
                        ArrayList<MemberListEntry> replicas = (ArrayList<MemberListEntry>) metadata.get(message.getSrcFileName());
                        Message repMess = new Message(MessageType.ReturnGetReplicas, message.getSrcFileName(),
                                message.getDesFileName(), replicas);
                        output.writeObject(repMess);
                        output.flush();
                        logger.info("RETURN_GET_REPLICAS: Sent list replicas of " + ((replicas == null) ? null: replicas.size())
                                + " of " + message.getSrcFileName() + " to client over TCP");
                    }
                    break;
                case File:
                    /*
                    Write file to sdfs and ack to master
                     */
                    String fileName = message.getDesFileName();

                    logger.info("FILE: received file. Write file " + fileName + " to local ");
                    if (!fileName.contains(sdfsPrefix)) {
                        fileName = sdfsPrefix + fileName + "/";
                    }

                    String[] fileType = fileName.split("\\.");
                    String fileFormat = fileType[fileType.length - 1];

                    File file = new File(fileName);
                    file.mkdir();

                    String timestamp_file = new Date().getTime() +"."+fileFormat;
                    Files.write(new File(fileName + timestamp_file).toPath(),
                            message.getContent());

                    // Write filename to a text for querying

                    addFileName(fileName, timestamp_file);

                    try {
                        Socket socketToMaster = new Socket(masterHost, masterPort);
                        ObjectOutputStream outputStream = new ObjectOutputStream(socketToMaster.getOutputStream());
                        outputStream.writeObject(
                                new Message(MessageType.AckFile, message.getDesFileName(), selfEntry)
                        );
                        outputStream.flush();
                        logger.info("FILE: Sent ACK write file " + message.getDesFileName()  + " successfully to master");
                        socketToMaster.close();
                        outputStream.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    break;
                case RequestFile:
                    logger.info("REQUEST_FILE: Received request file " + message.getSrcFileName());
                    File versionFile = new File(sdfsPrefix+ message.getSrcFileName()+"/" + Utils.versionFileName);

                    Scanner sc = new Scanner(versionFile);
                    String chosenFileName = "";

                    while (sc.hasNext()) {
                        chosenFileName = sc.next();
                    }

                    File chosenFile = new File(String.join("/", chosenFileName.split("/")));

                    logger.info("FILE_TO_SEND: Choose file " + chosenFile.getName() + " to send to client");

                    Message repMessage = new Message(MessageType.File, 0, message.getSrcFileName(),
                            Files.readAllBytes(chosenFile.toPath()), message.getDesFileName());
                    output.writeObject(repMessage);
                    output.flush();
                    break;

                case AckFile:
                    /*
                    Update save file to master
                     */
                    String fileSaved = message.getDesFileName();
                    MemberListEntry addr = message.getSubjectEntry();
                    logger.info("ACK_FILE: Received ACK write file " + fileSaved + " successfully");
                    logger.info("DEBUG: Adding metadata...");

                    synchronized (metadata) {
                        metadata.putIfAbsent(fileSaved, new ArrayList<MemberListEntry>());
                        MemberListEntry subj = message.getSubjectEntry();

                        if (!metadata.get(fileSaved).contains(subj)) {
                            metadata.get(fileSaved).add(subj);
                        }
                    }

                    String address = addr.getHostname() + ":" + addr.getPort();
                    logger.info("METADATA_UPDATE: Updated file "+ fileSaved + " saved to "+ address);
                    logger.info("METADATA: " + metadata);

                        // Replicate metadata
//                        List<MemberListEntry> metadataReplicas = memberList.getSuccessors(Constants.NUM_REPLICAS);
//                        for (MemberListEntry metaNode: metadataReplicas) {
//                            try {
//                                if (metaNode.equals(selfEntry)) {
//                                    continue;
//                                }
//
//                                Socket socket1 = new Socket(metaNode.getHostname(), metaNode.getPort());
//                                ObjectOutputStream outputStream = new ObjectOutputStream(socket1.getOutputStream());
//                                Message metaMessage = new Message(MessageType.Metadata, fileSaved, addr);
//                                outputStream.writeObject(metaMessage);
//                                outputStream.flush();
//                                logger.info("METADATA_SENT: Sent metadata to " + metaNode.getHostname()+":"+metaNode.getPort());
//                                socket1.close();
//                                outputStream.close();
//                            } catch (IOException e){
//                                e.printStackTrace();
//                            }
//                        }
//                    }
                    break;
                case Metadata:
                    fileSaved = message.getDesFileName();
                    addr = message.getSubjectEntry();
                    metadata.computeIfAbsent(fileSaved, k -> new ArrayList<MemberListEntry>());
                    metadata.get(fileSaved).add(message.getSubjectEntry());
                    address = addr.getHostname() + ":" + addr.getPort();
                    logger.info("METADATA_UPDATE: Updated file "+ fileSaved + " saved to "+ address);
                    logger.info("METADATA: " + metadata);
                    break;
                case RequestDelete:
                    String fileToDel = message.getDesFileName();
                    logger.info("REQUEST_DELETE: master - received DELETE request file " + fileToDel);
                    ArrayList<MemberListEntry> entries = (ArrayList<MemberListEntry>) metadata.get(fileToDel);
                    if (entries == null) {
                        break;
                    }
                    Message deleteMessage = new Message(MessageType.DeleteFile, fileToDel, null);
                    metadata.remove(fileToDel);
                    for (MemberListEntry entry: entries) {
                        try {
                            Socket socketDelete = new Socket(entry.getHostname(), entry.getPort());
                            ObjectOutputStream outputStream = new ObjectOutputStream(socketDelete.getOutputStream());
                            outputStream.writeObject(deleteMessage);
                            outputStream.flush();
                            logger.info("DELETE_FILE: Sent delete message to node " + entry.getHostname()+":" + entry.getPort());
                            socketDelete.close();
                            outputStream.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                    break;
                case DeleteFile:
                    String fileToDelete = message.getDesFileName();
                    logger.info("DELETE_FILE: Received message delete file " + fileToDelete);
                    if (!fileToDelete.contains(sdfsPrefix)) {
                        fileToDelete = sdfsPrefix + fileToDelete;
                    }
                    File deleteFile = new File(fileToDelete);
                    if (deleteFile.delete()) {
                        logger.info("DELETE_FILE: Deleted file "+ fileToDelete);
                    } else {
                        logger.info("DELETE_FILE: Delete file unsuccessfully");
                    }
                    break;
                case RequestVersion:
                    logger.info("REQUEST_VERSION: Received request file version " + message.getSrcFileName());
                    File tmp = File.createTempFile("tmp", null);
                    Utils.getVersion(message.getN(), sdfsPrefix, message.getSrcFileName(), tmp);
                    repMessage = new Message(MessageType.File, message.getN(), message.getSrcFileName(),
                            Files.readAllBytes(tmp.toPath()), message.getDesFileName());
                    output.writeObject(repMessage);
                    output.flush();
                    break;
                case RequestReplicate:
                    MemberListEntry subj = message.getSubjectEntry();
                    logger.info("RE_REPLICATE: Received re-replicate request file " + message.getDesFileName()
                            + " to " + subj.getHostname()+ ":" + subj.getPort());
                    Socket replicateSock = new Socket(subj.getHostname(), subj.getPort());
                    ObjectOutputStream outputStream = new ObjectOutputStream(replicateSock.getOutputStream());
                    versionFile = new File(sdfsPrefix+ message.getDesFileName()+"/" + Utils.versionFileName);

                    sc = new Scanner(versionFile);
                    chosenFileName = "";
                    while (sc.hasNext()) {
                        chosenFileName = sc.next();
                    }
                    chosenFile = new File(String.join("/", chosenFileName.split("/")));
                    outputStream.writeObject(new Message(
                            MessageType.File, 0, message.getDesFileName(),
                            Files.readAllBytes(chosenFile.toPath()), message.getDesFileName()
                    ));

                    logger.info("RE_REPLICATE: Sent file " + message.getDesFileName() + " to "
                            + subj.getHostname() + ":" + subj.getPort());

                    outputStream.flush();
//                    outputStream.close();
//                    replicateSock.close();
                    break;
                case InferInfo:
                    // send batch size and node having model to node having data
                    String testSet = message.getSrcFileName();
                    String testResult = message.getDesFileName();
                    int batchSize = message.getN();
                    logger.info("INFER_INFO: Received infer request for testset "+ testSet + " with output " + testResult);
                    long start = System.currentTimeMillis();
                    splitInputFile(testSet, testResult, batchSize);
                    logger.info("DEBUG: Total time for infer " + (System.currentTimeMillis() - start) + " ms");
                    break;
                case Infer:
                    DataBatch dataBatch = message.getDataBatch();
                    logger.info("INFER: Received inference request from index " + dataBatch.getStartInd() +
                                " to index "+ dataBatch.getEndInd());
                    // call inference and save file
                    // send message to coordinator that work has done
                    String modelName = message.getModel();
                    Inference infer = new Inference();
                    MultiLayerNetwork model;

                    start = System.currentTimeMillis();
                    if (modelName.equals("model_1")) {
                        model = infer.getResnetModel();
                    }
                    else {
                        model = infer.getVGGModel();
                    }
                    logger.info("DEBUG: Time to load model " + (System.currentTimeMillis() - start) + " ms");

                    start = System.currentTimeMillis();

                    String []input_file = dataBatch.getInput();
                    for (int i=0 ; i < input_file.length; i++) {
//                        File tmpInf = File.createTempFile("inputInf", input_file[i]);
                        String tmpInf = "/tmp/" + input_file[i];
                        get(input_file[i], tmpInf, FileOp.Op.GET, 0);
                        logger.info("DEBUG: Create inference input temp file in " + tmpInf);
                        String outputTmp = infer.inference(model, tmpInf, modelName);
                        String img_output = input_file[i].split("\\.")[0] + ".txt";
                        put(outputTmp, modelName + "_output_" + img_output);
                        logger.info("DEBUG: Successfully put output to sdfs with name " + modelName + "_output_" + img_output);
                    }

                    logger.info("DEBUG: batch from index " + dataBatch.getStartInd() + " to "+dataBatch.getEndInd() +
                                " completed with " + (System.currentTimeMillis() - start) +" ms");

                    logger.info("DEBUG: Infer successfully from index " + dataBatch.getStartInd() + " to index " + dataBatch.getEndInd());

                    Message ackMessage = new Message(MessageType.Infer, dataBatch, modelName + "_output_");
                    output.writeObject(ackMessage);
                    output.flush();

                    logger.info("INFER_SUCCESS: Sent infer succeed message to coordinator");

                    break;
//                case InferSuccess:
//                    dataBatch = message.getDataBatch();
//                    logger.info("INFER_SUCCESS: Received inference successful from index " + dataBatch.getStartInd() +
//                            " to index "+ dataBatch.getEndInd());
//                    isInferred.replace(dataBatch, true);
//                    break;

                default:
                    break;
            }

            // Close resources
            input.close();
            output.close();
            client.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void splitInputFile(String testSet, String testResult, int batchSize) throws IOException, ClassNotFoundException {
        File tmp = File.createTempFile("testSet", null);
        get(testSet, tmp.getPath(), FileOp.Op.GET, 0);
        logger.info("DEBUG: Get " + testSet + " from sdfs to master to split with tmp file: " + tmp.getPath());
        BufferedReader br = new BufferedReader(new FileReader(tmp.getPath()));
        String line;
        ArrayList<String> lines = new ArrayList<>();
        while ((line = br.readLine()) != null) {
            if (!line.isEmpty()) {
                lines.add(line.trim());
            }
        }
        logger.info("DEBUG: Read " + lines.size() + " from tmp files.");
        String [] lineArray = lines.toArray(new String[lines.size()]);

        List<MemberListEntry> members = new ArrayList<>(memberList.getMemberList());
        int numVmEachJob = (memberList.size() - 1)/2;
        logger.info("DEBUG: Approximately each model is assigned " + numVmEachJob);
        metadataModel.put(Utils.Model1, new ModelInfo(members.subList(1, numVmEachJob+1)) );
        metadataModel.put(Utils.Model2, new ModelInfo(members.subList( numVmEachJob+1, members.size())));

        logger.info("DEBUG: model_1 is assign with " + metadataModel.get(Utils.Model1).getEntries().size() + " VMs and " +
                    "model_2 is assigned with " + metadataModel.get(Utils.Model2).getEntries().size() + " VMs" +
                    " with total of index is " + lineArray.length);

        assignJob(lineArray, metadataModel.get(Utils.Model1).getEntries(), batchSize);
        assignJob(lineArray, metadataModel.get(Utils.Model2).getEntries(), batchSize);
        sendJob();
    }

    private void assignJob(String[] lines, List<MemberListEntry> entries, int batchSize) {

        int index = 0;
        int i = 0;
        while (index <= lines.length){

            DataBatch dataBatch;
            if (index + batchSize >= lines.length) {
                dataBatch = new DataBatch(index, lines.length, Arrays.copyOfRange(lines, index, lines.length));
            } else {
                dataBatch = new DataBatch(index, index+batchSize, Arrays.copyOfRange(lines, index, index+batchSize));
            }

            entryDataBatch.putIfAbsent(entries.get(i), new ArrayList<>());
            entryDataBatch.get(entries.get(i)).add(dataBatch);
            isInferred.putIfAbsent(dataBatch, false);

            logger.info("DEBUG: Assign index: " + dataBatch.getStartInd() + " -> " + dataBatch.getEndInd() +
                        " to " + entries.get(i).getHostname() + ":" + entries.get(i).getPort());

            index += batchSize;
            i++;
            if (i >= entries.size()) i = 0;
        }
    }

    private void sendJob() throws IOException, ClassNotFoundException {
        ExecutorService executor = Executors.newFixedThreadPool(entryDataBatch.size());
        for (MemberListEntry entry: entryDataBatch.keySet()) {

            executor.execute(new Runnable() {
                @Override
                public void run() {
                    for (DataBatch dataBatch: entryDataBatch.get(entry)) {
                        long start = System.currentTimeMillis();

                        try {
                            Socket socket1 = new Socket(entry.getHostname(), entry.getPort());
                            ObjectOutputStream outputStream = new ObjectOutputStream(socket1.getOutputStream());
                            ObjectInputStream inputStream = new ObjectInputStream(socket1.getInputStream());
                            Message message;

                            String model = Utils.Model1;
                            if (metadataModel.get(Utils.Model2).getEntries().contains(entry))
                                model = Utils.Model2;

                            metadataModel.get(model).increaseQueryCount();

                            message = new Message(MessageType.Infer, dataBatch, model);
                            logger.info("INFER_SEND: Send batch size with start index " + dataBatch.getStartInd() +
                                    " to index " + dataBatch.getEndInd() + " to host " + entry.getHostname() + ":" + entry.getPort());
                            outputStream.writeObject(message);
                            outputStream.flush();

                            Message reply = (Message) inputStream.readObject();
                            DataBatch batch = reply.getDataBatch();
                            logger.info("INFER_SUCCESS: Received inference successful from index " + batch.getStartInd() +
                                    " to index "+ batch.getEndInd());
                            isInferred.replace(batch, true);

                            double time = (double) (System.currentTimeMillis() - start)/1000;
                            metadataModel.get(model).addProcessingTime(time);

                            String []input_file = dataBatch.getInput();
                            for (int i=0 ; i < input_file.length; i++) {
                                String img_output = input_file[i].split("\\.")[0] + ".txt";
                                metadataModel.get(model).addOutput(message.getModel() + img_output);
                            }

                            metadataModel.get(model).calculateQueryRate(batchSize);


                            logger.info("QUERY_RATE: " + model + ": " + metadataModel.get(model).getQueryCount() + " with " +
                                    metadataModel.get(model).getQueryRate() + " queries/s with batch size "
                                    + batchSize + " and " + time + " s for each");

                            outputStream.close();
                            inputStream.close();
                            socket1.close();

                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        } catch (ClassNotFoundException e) {
                            throw new RuntimeException(e);
                        }


                    }
                }
            });


        }
        executor.shutdown();
//        metadataModel = new HashMap<>();
//        isInferred = new HashMap<>();
    }

//    private class SenderQuery extends Thread {
//        public MemberListEntry member;
//        private AtomicBoolean ackSignal;
//        private long timeOut;
//        private Socket socket;
//        private String testResult;
//
//        public SenderQuery(MemberListEntry member, String testResult, long timeOut) throws IOException {
//            this.member = member;
////            this.ackSignal = ackSignal;
//            this.timeOut = timeOut;
//            this.socket = new Socket(member.getHostname(), member.getPort());
//            this.testResult = testResult;
//        }
//
//        @Override
//        public void run() {
//
//            try {
//                // Send query
//                logger.info("Sending query " + member);
//                ObjectInputStream inputStream = new ObjectInputStream(this.socket.getInputStream());
//                ObjectOutputStream outputStream = new ObjectOutputStream(this.socket.getOutputStream());
//                for (DataBatch dataBatch: entryDataBatch.get(member)) {
//                    Message message = new Message(MessageType.Infer, dataBatch, testResult);
//                    logger.info("INFER_SEND: Send batch size with start index " + dataBatch.getStartInd() +
//                            " to index " + dataBatch.getEndInd() + " to host " + member.getHostname() + ":" + member.getPort());
//                    outputStream.writeObject(message);
//                    outputStream.flush();
//                }
//
//                outputStream.close();
//                socket.close();
//
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//
//    }

    private void addFileName(String filename, String timestamp) {
        logger.info("FILE: Write " + filename + " to version_list text file ");
        String list_version_file = filename + Utils.versionFileName;
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(list_version_file, true));
            writer.write(filename + timestamp + "\n");
            writer.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    // For receiving UDP messages and responding
    // For sending pings and checking ack
    private void mainProtocol() {
        try {
            socket = new DatagramSocket(selfEntry.getPort());
            // Maintain list of acknowledgements to know which member sent the ACK
            List<AtomicBoolean> ackSignals = new ArrayList<>(Utils.NUM_MONITORS);
            for (int i = 0; i < Utils.NUM_MONITORS; i++) {
                ackSignals.add(new AtomicBoolean());
            }
            // Receive ping and send ACK
            Receiver receiver = new Receiver(socket, selfEntry, end, ackSignals);
            receiver.setDaemon(true);
            receiver.start();
            logger.info("UDP Socket opened");
            while(!end.get()) {
                List<MemberListEntry> successors;
                synchronized (memberList) {
                    // Get the next successors to send ping to
                    successors = memberList.getSuccessors(Utils.NUM_MONITORS);
                    // Update receiver about the successor information
                    receiver.updateAckers(successors);
                    // Send ping
                    for (int i = 0; i < successors.size(); i++) {
                        new SenderProcess(ackSignals.get(i), successors.get(i), 500).start();
                    }
                }

                // send metadata

                //Wait for protocol time period
                Thread.sleep(Utils.PROTOCOL_TIME);
            }
            socket.close();
            logger.info("UDP Socket closed");
            receiver.join();
        }catch (SocketException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    // Process to send Pick and wait for ACK
    private class SenderProcess extends Thread {
        public MemberListEntry member;
        private AtomicBoolean ackSignal;
        private long timeOut;

        private void ping(MemberListEntry member, MemberListEntry sender) throws IOException {
            Message message = new Message(MessageType.Ping, sender);
            UDPProcessing.sendPacket(socket, message, member.getHostname(), member.getPort());
        }

        public SenderProcess(AtomicBoolean ackSignal, MemberListEntry member, long timeOut)  {
            this.member = member;
            this.ackSignal = ackSignal;
            this.timeOut = timeOut;
        }

        @Override
        public void run() {

            try {
                // Ping successor
                synchronized (ackSignal) {
                    ackSignal.set(false);
                    logger.info("Pinging " + member);
                    ping(member, selfEntry);
                    ackSignal.wait(timeOut);
                }

                // Handle ACK timeout
                if (!ackSignal.get()) {
                    logger.warning("ACK not received from " + member);
                    logger.warning("Process failure detected detected: " + member);
                    // Disseminate message first in case of false positive
                    disseminateMessage(new Message(MessageType.Crash, member));

                    // Then remove entry
                    synchronized (memberList) {
                        if (memberList.removeEntry(member)) {
                            logger.info("Process removed from membership list: " + member);
                        }
                    }
                } else {
                    logger.info("ACK received from " + member);
                }
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
}
