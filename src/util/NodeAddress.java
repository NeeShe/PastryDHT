package util;

import java.io.Serializable;
import java.net.InetAddress;

public class NodeAddress implements Serializable {
        private String nodeName;
        private InetAddress inetAddress;
        private int port;

	public NodeAddress(String nodeName, InetAddress inetAddress, int port) {
            this.nodeName = nodeName;
            this.inetAddress = inetAddress;
            this.port = port;
        }

        public void setInetAddress(InetAddress inetAddress) {
            this.inetAddress = inetAddress;
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
        public boolean equals(Object o) {
            if(o instanceof NodeAddress) {
                NodeAddress nodeAddress = (NodeAddress) o;
                int hostName = inetAddress.getHostName().compareTo(nodeAddress.getInetAddress().getHostName());
                int hostAddress = inetAddress.getHostAddress().compareTo(nodeAddress.getInetAddress().getHostAddress());

                if(port == nodeAddress.getPort() && (hostName == 0 || hostAddress == 0)) {
                    return true;
                }
            }

            return false;
        }

        @Override
        public String toString() {
            return nodeName;
        }
}
