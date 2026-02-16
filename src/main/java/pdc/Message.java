package pdc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Message represents the communication unit in the CSM218 protocol.
 * 
 * Requirement: You must implement a custom WIRE FORMAT.
 * DO NOT use JSON, XML, or standard Java Serialization.
 * Use a format that is efficient for the parallel distribution of matrix
 * blocks.
 */
public class Message {
    public static final String CSM218_MAGIC = "CSM218";
    public String magic;
    public int version;
    public String messageType;
    public String studentId;
    public String type;
    public String sender;
    public long timestamp;
    public byte[] payload;

    public Message() {
    }

    /**
     * Converts the message to a byte stream for network transmission.
     * Students must implement their own framing (e.g., length-prefixing).
     */
    public byte[] pack() {
        try {
            // Fixed field order framing:
            // [magic][version][messageType][studentId][timestamp][payloadLength][payload]
            String effectiveMagic = (magic == null || magic.isEmpty()) ? CSM218_MAGIC : magic;
            String effectiveMessageType = (messageType == null || messageType.isEmpty()) ? type : messageType;
            String effectiveStudentId = (studentId == null || studentId.isEmpty()) ? sender : studentId;
            byte[] magicBytes = effectiveMagic.getBytes(StandardCharsets.UTF_8);
            byte[] typeBytes = effectiveMessageType == null ? new byte[0] : effectiveMessageType.getBytes(StandardCharsets.UTF_8);
            byte[] studentBytes = effectiveStudentId == null ? new byte[0] : effectiveStudentId.getBytes(StandardCharsets.UTF_8);
            int payloadLength = payload == null ? 0 : payload.length;
            int estimatedSize = 4 + magicBytes.length + 4 + 4 + typeBytes.length + 4 + studentBytes.length + 8 + 4
                    + payloadLength;

            ByteArrayOutputStream baos = new ByteArrayOutputStream(estimatedSize);
            DataOutputStream out = new DataOutputStream(baos);

            writeString(out, magicBytes);
            out.writeInt(version);
            writeString(out, typeBytes);
            writeString(out, studentBytes);
            out.writeLong(timestamp);

            out.writeInt(payloadLength);
            if (payloadLength > 0) {
                out.write(payload);
            }
            out.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to pack message", e);
        }
    }

    /**
     * Reconstructs a Message from a byte stream.
     */
    public static Message unpack(byte[] data) {
        if (data == null) {
            throw new IllegalArgumentException("Message data cannot be null");
        }

        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
            Message msg = new Message();

            msg.magic = readString(in);
            msg.version = in.readInt();
            msg.messageType = readString(in);
            msg.studentId = readString(in);
            msg.type = msg.messageType;
            msg.sender = msg.studentId;
            msg.timestamp = in.readLong();

            int payloadLength = in.readInt();
            if (payloadLength < 0) {
                throw new IllegalArgumentException("Invalid payload length: " + payloadLength);
            }

            msg.payload = new byte[payloadLength];
            if (payloadLength > 0) {
                in.readFully(msg.payload);
            }

            return msg;
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to unpack message", e);
        }
    }

    private static void writeString(DataOutputStream out, byte[] bytes) throws IOException {
        out.writeInt(bytes.length);
        if (bytes.length > 0) {
            out.write(bytes);
        }
    }

    private static String readString(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length < 0) {
            throw new IllegalArgumentException("Invalid string length: " + length);
        }
        if (length == 0) {
            return "";
        }
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
