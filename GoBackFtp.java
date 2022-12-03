/**
 * @author Jeremy Kimotho
 * @version 28 Nov 2022
*/

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.*;

public class GoBackFtp {
	// global logger	
	private static final Logger logger = Logger.getLogger("GoBackFtp");

    // Variables used in TCP handshake and UDP sending and receiving of packets
    private int timeout; // how long to wait for ack before timeout, from user
    private String fileName;
    private Socket socket; // for tcp handshake
    private int uDPServerPort; // port number to send udp packets
    private int initialSequenceNum; // sequence number for first packet to be sent by udp
    private DatagramSocket uDPSocket; // for udp
    private InetAddress iPAddress; // ip address of udp server, from user
    private int windowSize; // number of packets we can have in transmission queue
    private TransmissionQueue queue; // concurrrentlinkedqueue for storing segments in transit
    private boolean sendingDone = false; // used to let sending thread inform receiving thread it's completed operations
    private Timer timer = new Timer(); // used for retransmissions 
    private TimerTask timerTask; // used for retransmissions

	/**
	 * Constructor to initialize the program 
	 * 
	 * @param windowSize	Size of the window for Go-Back_N in units of segments
	 * @param rtoTimer		The time-out interval for the retransmission timer
	 */
	public GoBackFtp(int windowSize, int rtoTimer)
    {
        this.windowSize = windowSize;
        timeout = rtoTimer;
    }

    /**
     * Used in the initial tcp handshake stage. sends to the server, the local udp
     * port number,
     * the filename that will be sent, and the length of the file that will be sent
     * 
     * @param serverStream - output stream to write information to socket through
     * @param file         - name of file we're sending
     * @param port         - local udp port number we'll use
     */
    void sendHandshake(OutputStream serverStream, String file, int port) {
        DataOutputStream stream = new DataOutputStream(serverStream);
        File target = new File(file);
        long length = target.length();

        try {
            stream.writeInt(port);
            stream.writeUTF(file);
            stream.writeLong(length);
            stream.flush();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException();
        }
    }

    /**
     * Used in the initial tcp handshake stage to receive response to data we sent
     * to server.
     * The information received is the port of the server we're sending to and also
     * the initial sequence number
     * 
     * @param serverStream - input stream to read information from socket
     * @return - an int array with the responses
     */
    int[] receiveHandshake(InputStream serverStream) {
        DataInputStream stream = new DataInputStream(serverStream);
        int[] response = { 1, 2 };

        try {
            response[0] = stream.readInt();
            response[1] = stream.readInt();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException();
        }

        return response;
    }

    /**
     * Used by sending and receiving threads to start timer preceding retransmission.
     * timeout - user provided value
    */
    public synchronized void startTimer() 
    {
        timerTask = new retransmitter(); 
        timer.scheduleAtFixedRate(timerTask, 300, timeout);
    }

    /**
     * Used by sending and receiving threads to stop timer preceding retransmission
     */
    public synchronized void stopTimer() {
        timerTask.cancel();
    }

    /**
     * Initiates the transfer of a file by first initialising transmission queue,
     * then creating sending and receiving threads.
     */
    void transferFile()
    {
        // queue will be used by sending, receiving, and retransmission threads to track segments
        queue = new TransmissionQueue();

        SendingThread sender = new SendingThread();
        ReceivingThread receiver = new ReceivingThread();

        // begin threads
        sender.start();
        receiver.start();

        // make sure threads wait for each other before concluding program
        try {
            sender.join();
            receiver.join();
        } catch (InterruptedException e) {}

        timer.cancel();
        timer.purge();
    }

	/**
	 * Send the specified file to the specified remote server
	 * 
	 * @param serverName	Name of the remote server
	 * @param serverPort	Port number of the remote server
	 * @param fileName		Name of the file to be trasferred to the rmeote server
	 */
	public void send(String serverName, int serverPort, String fileName)
    {
        this.fileName = fileName;

        // find server address using dns
        InetSocketAddress serverAddress = new InetSocketAddress(serverName, serverPort);
        // initialise tcp socket
        socket = new Socket();
        try
        {
            // connect to server via tcp
            socket.connect(serverAddress);
            // initialise udp socket
            uDPSocket = new DatagramSocket();
            // resolve ip of remote server
            iPAddress = InetAddress.getByName(serverName);

            // streams we'll use to read in and write to socket
            OutputStream socketWriter = socket.getOutputStream();
            InputStream socketReader = socket.getInputStream();

            // send handshake information via a tcp socket to remote server
            sendHandshake(socketWriter, fileName, uDPSocket.getLocalPort());
            // receive response
            int [] response = receiveHandshake(socketReader);
            uDPServerPort = response[0];
            initialSequenceNum = response[1];

            // tcp is no longer needed, we can close streams and socket
            socketWriter.close();
            socketReader.close();
            socket.close();

            // send via udp the file to the remote server
            transferFile();

            // close the udp socket once file has been sent
            uDPSocket.close();
        }
        catch (UnknownHostException e)
        {
            e.printStackTrace();
            throw new RuntimeException();
        }
        catch (SocketException e)
        {
            e.printStackTrace();
            throw new RuntimeException();
        }
        catch (IOException e)
        {
            e.printStackTrace();
            throw new RuntimeException();
        }
    }

    /**
     * TimerTask class handles all retransmissions
     */
    private class retransmitter extends TimerTask {
        // Variables we'll require for retransmission
        private DatagramPacket clientRetPacket; // local copy of packet to be sent

        retransmitter() {
        }

        @Override
        public void run() {
            System.out.println("timeout");

            try {
                // when retransmission is triggered, retransmit every segment currently in retransmission queue
                for(FtpSegment serverRetSegment: queue)
                {
                    // locally create packet to avoid race conditions with main thread
                    clientRetPacket = FtpSegment.makePacket(serverRetSegment, iPAddress, uDPServerPort);
                    // retransmission
                    System.out.printf("retx %d\n", serverRetSegment.getSeqNum());
                    uDPSocket.send(clientRetPacket);
                }
            } catch (IOException e) 
            {
                e.printStackTrace();
                throw new RuntimeException();
            }
        }
    }

    /**
     * Handles the initial reading from file and sending of packets
     */
    private class SendingThread extends Thread 
    {
        private FileInputStream sourceFile; // used to read from file 
        private byte [] buffer = new byte[FtpSegment.MAX_PAYLOAD_SIZE]; // where read bytes from file are stored
        private int readBytes; // tracks how many bytes we've read
        private FtpSegment segment; // used to create segment from read bytes from file
        private DatagramPacket packet; // used to create packet from segment
        private int seqNum; // tracks current sequence number

        SendingThread()
        {
            try
            {
                this.sourceFile = new FileInputStream(fileName);
            }
            catch (FileNotFoundException e)
            {
                e.printStackTrace();
                throw new RuntimeException();
            }

            // use initial sequence number from server as our starting point
            this.seqNum = initialSequenceNum;
        }

        public void run()
        {
            try
            {
                while((readBytes = sourceFile.read(buffer)) != -1)
                {
                    // read from file
                    // make into segment and packet
                    segment = new FtpSegment(seqNum, buffer, readBytes);
                    packet = FtpSegment.makePacket(segment, iPAddress, uDPServerPort);
                    // wait while transmission queue is full
                    while(queue.size() >= windowSize) Thread.yield();
                    // send to server
                    System.out.printf("send %d\n", seqNum);
                    uDPSocket.send(packet);
                    // add to transmission queue once sent
                    queue.pushSegment(segment);
                    // start ack timer if segment is first in queue
                    if(queue.size() == 1) startTimer();
                    seqNum += 1;
                }
                sendingDone = true;
                sourceFile.close();
            }
            catch (IOException e)
            {
                e.printStackTrace();
                throw new RuntimeException();
            }
        }
    }

    /**
     * Handles the processing of acks from server
     */
    private class ReceivingThread extends Thread
    {
        private byte[] buffer = new byte[FtpSegment.MAX_PAYLOAD_SIZE]; // used to store packet from server
        private FtpSegment segment; // used to extract information from packet received from server
        private DatagramPacket packet; // packet received from server
        private int lastACK; // used to track last received ack from server

        ReceivingThread()
        {
        }

        public void run()
        {
            try 
            {
                // useful to ensure no deadlock stopping the while condition from being checked
                uDPSocket.setSoTimeout(100);
                // while sending thread not finished or transmission queue not empty do
                while (!sendingDone || queue.size() > 0) {
                    try {
                        // receive ack
                        packet = new DatagramPacket(buffer, buffer.length);
                        uDPSocket.receive(packet);
                        // extract segment from packet
                        segment = new FtpSegment(packet);
                        // extract ack from segment
                        lastACK = segment.getSeqNum();
                        System.out.printf("ack %d\n", lastACK);
                        if (ackIsValid()) {
                            // stop timer
                            stopTimer();
                            // update transmission queue now that we have an ack
                            updateTransmissionQueue();
                            // start timer if transmission queue is not empty
                            if (queue.size() > 0) startTimer();
                        }
                    }
                    catch (SocketTimeoutException e)
                    {
                        // Timed out to check while condition
                    } 
                    catch (IOException e) {
                        e.printStackTrace();
                        throw new RuntimeException();
                    }
                }
                stopTimer();
                timer.cancel();
                timer.purge();
            } 
            catch (SocketException e) {
                e.printStackTrace();
                throw new RuntimeException();
            }
            
        }

        /**
         * Used to check if the ack we received from server is valid. 
         * Valid in this case means there's a segment in queue with sequence number smaller than ack
         * @return - whether or not the ack is valid 
         */
        private boolean ackIsValid()
        {
            try 
            {
                // look at the first element in queue as they're in ascending order of seqNums 
                if(queue.peek().getSeqNum() < lastACK) return true;
                return false;
            }
            catch (NullPointerException e)
            {
                // if this is caught means queue is empty currently. therefore return false as there's no ack to assess
                return false;
            }
        }

        /**
         * Go through queue and ensure we remove every segment that has been acknowledged by the server.
         * Meaning any segment with a smaller sequence number than the ack number
         */
        private void updateTransmissionQueue()
        {
            for(FtpSegment lastSegment: queue)
            {
                if(lastSegment.getSeqNum() < lastACK)
                {
                    queue.popSegment();
                }
                else return;
            }
        }
    }

    /**
     * Is the data structure used for transmission queue
     */
    private class TransmissionQueue extends ConcurrentLinkedQueue<FtpSegment>
    {
        private int size = 0;

        private void pushSegment(FtpSegment segment)
        {
            size++;
            offer(segment);
        }

        private FtpSegment popSegment()
        {
            size--;
            return poll();
        }

        // because of concurrency doesn't always return a correct value
        public int getSize()
        {
            return size;
        }
    }
	
}