package kirillbelov.ltp;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class LTPClient {

    private Socket socket; // TCP socket used for communication

    /**
     * Constructor that accepts an existing socket.
     * 
     * @param socket A pre-established TCP socket.
     */
    public LTPClient(Socket socket){
        this.socket = socket;
    }

    /**
     * Constructor that creates a new socket connection to the specified host and port.
     * 
     * @param host The server hostname or IP address.
     * @param port The port number to connect to.
     * @throws IOException If an error occurs while establishing the connection.
     */
    public LTPClient(String host, int port) throws IOException {
        this.socket = new Socket(host, port);
    }

    /**
     * Constructor that creates a new socket connection to localhost on the specified port.
     * 
     * @param port The port number to connect to.
     * @throws IOException If an error occurs while establishing the connection.
     */
    public LTPClient(int port) throws IOException {
        this.socket = new Socket("localhost", port);
    }

    /**
     * Returns the current socket instance.
     * 
     * @return The active socket connection.
     */
    public Socket getSocket(){
        return socket;
    }

    /**
     * Updates the current socket instance with a new socket.
     * 
     * @param socket The new socket instance.
     */
    public void setSocket(Socket socket){
        this.socket = socket;
    }

    /**
     * Creates a new socket connection to the specified host and port.
     * 
     * @param host The server hostname or IP address.
     * @param port The port number to connect to.
     * @throws IOException If an error occurs while establishing the connection.
     */
    public void setSocket(String host, int port) throws IOException {
        this.socket = new Socket(host, port);
    }

    /**
     * Creates a new socket connection to localhost on the specified port.
     * 
     * @param port The port number to connect to.
     * @throws IOException If an error occurs while establishing the connection.
     */
    public void setSocket(int port) throws IOException {
        this.socket = new Socket("localhost", port);
    }

    /**
     * Sends a message using the LTP protocol over the established TCP socket.
     * 
     * LTP Protocol Message Structure:
     * - The first 4 bytes contain the ASCII string "LTP#", serving as a protocol identifier.
     * - The next 4 bytes represent a 32-bit integer (big-endian) specifying the total message length.
     * - The remaining bytes contain the actual payload data.
     * 
     * @param data The payload data to be transmitted as a byte array.
     * @throws IOException If an I/O error occurs during communication with the socket.
     */
    public void sendMessage(byte[] data) throws IOException {
        // Obtain the output stream from the socket to send data.
        OutputStream out = socket.getOutputStream();
        DataOutputStream dos = new DataOutputStream(out);

        // Calculate the total length of the message (payload size + 8-byte header).
        int totalLength = data.length + 8;

        // Convert "LTP#" protocol identifier into a byte array.
        byte[] header = "LTP#".getBytes(StandardCharsets.US_ASCII);
        
        // Write the protocol identifier to the output stream.
        dos.write(header);

        // Write the total message length as a 4-byte integer in big-endian format.
        dos.writeInt(totalLength);

        // Write the actual payload data.
        dos.write(data);

        // Flush the output stream to ensure all data is sent immediately.
        dos.flush();
    }

    /**
     * Overloaded method to send a string message.
     * 
     * Converts the provided string into a UTF-8 encoded byte array and sends it using the LTP protocol.
     * 
     * @param message The string message to be sent.
     * @throws IOException If an I/O error occurs during communication with the socket.
     */
    public void sendMessage(String message) throws IOException {
        // Convert the string message to a byte array using UTF-8 encoding.
        byte[] data = message.getBytes(StandardCharsets.UTF_8);
        // Delegate sending to the byte-array version of sendMessage.
        sendMessage(data);
    }

    /**
     * Main method to demonstrate sending an LTP message to a server.
     * 
     * @param args Command-line arguments (not used in this example).
     */
    public static void main(String[] args) {
        String host = "localhost"; // The server address to connect to.
        int port = 12345;          // The port number on which the server is listening.

        // Establish a connection to the server using a try-with-resources block,
        // which ensures the socket is closed automatically after use.
        try {
            // Create an LTP client and connect to the server.
            LTPClient client = new LTPClient(new Socket(host, port)); 
            String message = "Hello, LTP!";
            // Send the message using the overloaded sendMessage method for strings.
            client.sendMessage(message);

            // Print confirmation that the message was successfully sent.
            System.out.println("Message successfully sent using LTP protocol");
        } catch (IOException e) {
            // Print the error stack trace if an I/O error occurs during socket communication.
            e.printStackTrace();
        }
    }
}
