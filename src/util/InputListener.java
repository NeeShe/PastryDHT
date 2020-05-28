package util;

import node.ClientNode;

import java.util.Scanner;

import static util.Util.*;

public class InputListener extends Thread{
    ClientNode node;
    byte[] id;
    String op;
    String content;
    final int ID_BYTES = 2;
    public InputListener(){

    }
//    public InputListener(ClientNode node){
//        this.node = node;
//    }

    public byte[] getInputId() {
        return id;
    }

    public String getOperation() {
        return op;
    }

    public String getInputDataContent() {
        return content;
    }

    @Override
    public void run() {
        //super.run();
        Scanner scn = new Scanner(System.in);
        while(true){
            String op = scn.nextLine();
            if (op.equalsIgnoreCase("Q")) {
                break;

            } else if (op.equalsIgnoreCase("S") || op.equalsIgnoreCase("R")) {
                //store in new data
                System.out.print("Data Id (or leave blank to auto-generate: ");
                String idStr = scn.nextLine();
                System.out.print("Data Content: ");
                String content = scn.nextLine();
                byte[] id;
                if (idStr == null || idStr.length() <= 0) {
                    //auto generate a new id randomly
                    id = generateRandomID(ID_BYTES);
//                    idStr = convertBytesToHex(id);
                } else {
                    id = convertHexToBytes(idStr);
                }

                this.id = id;
                this.op = op;
                this.content = content;
                if(op.equalsIgnoreCase("R")) {
                    System.out.println("Retrieved data: " + convertBytesToHex(node.getData()));
                }

            } else {
                System.out.println("Sorry, your input is invalid. Please enter again.");
                continue;
            }
        }
    }
}
