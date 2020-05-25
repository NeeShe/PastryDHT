package message;

import util.NodeAddress;

import java.util.Map;

public class RoutingInfoMsg extends Message {
	private Map<byte[], NodeAddress> leafSet;
	private Map<byte[], NodeAddress> neighborSet;
	private int prefixLength;
	private Map<String,NodeAddress> routingTable;
	private boolean broadcastMsg;

	public RoutingInfoMsg(Map<byte[],NodeAddress> leafSet, Map<byte[],NodeAddress> neighborSet,int prefixLength, Map<String,NodeAddress> routingTable, boolean broadcastMsg) {
		this.leafSet = leafSet;
		this.neighborSet=neighborSet;
		this.prefixLength = prefixLength;
		this.routingTable = routingTable;
		this.broadcastMsg = broadcastMsg;
	}

	public Map<byte[],NodeAddress> getLeafSet() {
		return leafSet;
	}

	public Map<byte[],NodeAddress> getNeighborSet() {
		return neighborSet;
	}

	public int getPrefixLength() {
		return prefixLength;
	}

	public Map<String,NodeAddress> getRoutingTable() {
		return routingTable;
	}

	public boolean getBroadcastMsg() {
		return broadcastMsg;
	}

	@Override
	public int getMsgType() {
		return ROUTING_INFO_MSG;
	}
}
