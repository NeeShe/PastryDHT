package node;

import util.Util;
import util.Util.*;
public class Trial {
    public static void main(String[] args){
        byte[] a1 = Util.convertHexToBytes(args[0]);
        byte[] a2 = Util.convertHexToBytes(args[1]);
        boolean result =getDistance(Integer.decode("0x"+args[0]),Integer.decode("0x"+args[1]));
        System.out.println(result);
        System.out.println("Result 2:"+getHexDistance(args[0],args[1]));
    }



    private static boolean getDistance(int id1, int id2) {
        int dist = 0;
        if(id1 < id2) {
            dist = Math.abs(id2 - id1);
            System.out.println("Less than:"+dist);
            return true;
        } else if(id1 > id2) {
            dist = Math.abs(Short.MAX_VALUE - id1) + Math.abs(id2 - Short.MIN_VALUE);
            System.out.println("Great than:"+dist);
        }
        return false;
    }

    public static int getHexDistance(String hex1, String hex2) {
        return Math.abs(Integer.parseInt(hex1, 16) - Integer.parseInt(hex2, 16));
    }

}
