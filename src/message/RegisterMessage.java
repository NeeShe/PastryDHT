package message;

import java.net.InetAddress;

public class RegisterMessage extends Message {
    private byte[] nodeId;
    private String nodeName;
    private InetAddress inetAddress;
    private int port;

    public RegisterMessage(String nodeName, byte[] id, InetAddress inetAddress, int port) {
        this.nodeName = nodeName;
        this.nodeId = id;
        this.inetAddress = inetAddress;
        this.port = port;
    }

    public byte[] getID() {
        return nodeId;
    }

    public InetAddress getInetAddress() {
        return inetAddress;
    }

    public int getPort() {
        return port;
    }

    public String getNodeName() {
        return nodeName;
    }

    @Override
    public int getMsgType() {
        return REGISTER_NODE_MSG;
    }
}
