package kirillbelov.ltp;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class LTPClient {

    private Socket socket; // TCP socket used for communication with the server

    /**
     * Constructor that accepts an existing socket.
     * 
     * @param socket A pre-established TCP socket to be used for communication.
     */
    public LTPClient(Socket socket) {
        this.socket = socket;
    }

    /**
     * Constructor that creates a new socket connection to the specified host and port.
     * 
     * @param host The server hostname or IP address (e.g., "example.com" or "192.168.1.1").
     * @param port The port number on which the server is listening (e.g., 12345).
     * @throws IOException If an error occurs while establishing the socket connection.
     */
    public LTPClient(String host, int port) throws IOException {
        this.socket = new Socket(host, port);
    }

    /**
     * Constructor that creates a new socket connection to localhost on the specified port.
     * 
     * @param port The port number on localhost to connect to (e.g., 12345).
     * @throws IOException If an error occurs while establishing the socket connection.
     */
    public LTPClient(int port) throws IOException {
        this.socket = new Socket("localhost", port);
    }

    /**
     * Returns the current socket instance.
     * 
     * @return The active TCP socket connection.
     */
    public Socket getSocket() {
        return socket;
    }

    /**
     * Updates the current socket instance with a new socket.
     * 
     * @param socket The new socket instance to replace the current one.
     */
    public void setSocket(Socket socket) {
        this.socket = socket;
    }

    /**
     * Creates a new socket connection to the specified host and port, replacing the current socket.
     * 
     * @param host The server hostname or IP address to connect to.
     * @param port The port number to connect to.
     * @throws IOException If an error occurs while establishing the new connection.
     */
    public void setSocket(String host, int port) throws IOException {
        this.socket = new Socket(host, port);
    }

    /**
     * Creates a new socket connection to localhost on the specified port, replacing the current socket.
     * 
     * @param port The port number on localhost to connect to.
     * @throws IOException If an error occurs while establishing the new connection.
     */
    public void setSocket(int port) throws IOException {
        this.socket = new Socket("localhost", port);
    }

    /**
     * Sends a message using the LTP protocol over the established TCP socket.
     * The method constructs the entire message as a single byte array before sending it in one operation.
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
        // Calculate the total length of the message, including the 8-byte header (4 bytes for "LTP#" + 4 bytes for length)
        int totalLength = data.length + 8;

        // Convert the protocol identifier "LTP#" into a 4-byte array using ASCII encoding
        byte[] header = "LTP#".getBytes(StandardCharsets.US_ASCII);

        // Allocate a ByteBuffer with enough capacity to hold the entire message
        ByteBuffer buffer = ByteBuffer.allocate(totalLength);

        // Add the protocol identifier "LTP#" (4 bytes) to the buffer
        buffer.put(header);

        // Add the total message length as a 4-byte integer in big-endian format
        buffer.putInt(totalLength);

        // Add the payload data to the buffer
        buffer.put(data);

        // Extract the complete message as a single byte array from the ByteBuffer
        byte[] fullMessage = buffer.array();

        // Obtain the output stream from the socket for sending data
        OutputStream out = socket.getOutputStream();
        
        // Wrap the output stream in a DataOutputStream for convenient writing
        DataOutputStream dos = new DataOutputStream(out);

        // Write the entire message to the output stream in one operation
        dos.write(fullMessage);

        // Flush the output stream to ensure all data is sent immediately to the server
        dos.flush();
    }

    /**
     * Overloaded method to send a string message using the LTP protocol.
     * Converts the string to a UTF-8 encoded byte array and delegates to the byte-array version.
     * 
     * @param message The string message to be sent (e.g., "Hello, LTP!").
     * @throws IOException If an I/O error occurs during communication with the socket.
     */
    public void sendMessage(String message) throws IOException {
        // Convert the input string to a byte array using UTF-8 encoding
        byte[] data = message.getBytes(StandardCharsets.UTF_8);

        // Delegate to the byte-array version of sendMessage to handle the transmission
        sendMessage(data);
    }

    /**
     * Main method to demonstrate sending an LTP message to a server.
     * Connects to localhost:12345 and sends a simple test message.
     * 
     * @param args Command-line arguments (not used in this example).
     */
    public static void main(String[] args) {
        // Define the server address (localhost) and port number to connect to
        String host = "localhost";
        int port = 12345;

        // Use a try block to handle potential I/O exceptions during socket operations
        try {
            // Create an LTPClient instance by establishing a new socket connection to the server
            LTPClient client = new LTPClient(new Socket(host, port));

            // Define a test message to send
            String message = "Hello, LTP!";

            // Send the message using the LTP protocol
            client.sendMessage(message);

            // Print a confirmation message to the console if sending is successful
            System.out.println("Message successfully sent using LTP protocol");
        } catch (IOException e) {
            // If an I/O error occurs (e.g., server not running), print the stack trace for debugging
            e.printStackTrace();
        }
    }
}