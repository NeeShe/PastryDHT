package util;

import java.nio.ByteBuffer;
import java.util.Random;

public class Util {
    public static byte[] convertHexToBytes(String hexString) {
        int size = hexString.length();
        byte[] buf = new byte[size / 2];
        int j = 0;
        for (int i = 0; i < size; i++) {
            String a = hexString.substring(i, i + 2);
            int valA = Integer.parseInt(a, 16);
            i++;
            buf[j] = (byte) valA;
            j++;
        }
        return buf;
    }
    public static String convertBytesToHex(byte[] buf) {
        StringBuffer strBuf = new StringBuffer();
        for (int i = 0; i < buf.length; i++) {
            int byteValue = (int) buf[i] & 0xff;
            if (byteValue <= 15) {
                strBuf.append("0");
            }
            strBuf.append(Integer.toString(byteValue, 16));
        }
        return strBuf.toString();
    }

    public static byte[] generateRandomID(int length) {
        Random random = new Random();
        byte[] bytes = new byte[length];
        for(int i=0; i<bytes.length; i++) {
            bytes[i] = (byte) (random.nextInt() % 256);
        }
        return bytes;
    }

    public static short convertBytesToShort(byte[] bytes) {
        ByteBuffer buf = ByteBuffer.allocate(bytes.length);
        for(int i=0; i<bytes.length; i++) {
            buf.put(bytes[i]);
        }
        return buf.getShort(0);
    }
}
