package util;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
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
//        return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getShort();
    }

    public static int convertSingleHexToInt(String str) {
        return Integer.parseInt(str.replace("a","10").replace("b","11").replace("c","12").replace("d","13").replace("e","14").replace("f","15"));
    }


    public static int getHexDistance(String hex1, String hex2) {
        return Math.abs(Integer.parseInt(hex1, 16) - Integer.parseInt(hex2, 16));
    }

    public static String printByteAsString(byte[] bytes) {
        String content = new String(bytes, StandardCharsets.UTF_8);
        return content;
    }
}
