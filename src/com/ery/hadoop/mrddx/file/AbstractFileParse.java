package com.ery.hadoop.mrddx.file;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;

public abstract class AbstractFileParse {

	/**
	 * This function identifies and returns the hosts that contribute most for a
	 * given split. For calculating the contribution, rack locality is treated
	 * on par with host locality, so hosts from racks that contribute the most
	 * are preferred over hosts on racks that contribute less
	 * 
	 * @param blkLocations
	 *            The list of block locations
	 * @param offset
	 * @param splitSize
	 * @return array of hosts that contribute most to this split
	 * @throws IOException
	 */
	public String[] getSplitHosts(BlockLocation[] blkLocations, long offset, long splitSize, NetworkTopology clusterMap)
			throws IOException {

		int startIndex = getBlockIndex(blkLocations, offset);

		long bytesInThisBlock = blkLocations[startIndex].getOffset() + blkLocations[startIndex].getLength() - offset;

		// If this is the only block, just return
		if (bytesInThisBlock >= splitSize) {
			return blkLocations[startIndex].getHosts();
		}

		long bytesInFirstBlock = bytesInThisBlock;
		int index = startIndex + 1;
		splitSize -= bytesInThisBlock;

		while (splitSize > 0) {
			bytesInThisBlock = Math.min(splitSize, blkLocations[index++].getLength());
			splitSize -= bytesInThisBlock;
		}

		long bytesInLastBlock = bytesInThisBlock;
		int endIndex = index - 1;

		Map<Node, NodeInfo> hostsMap = new IdentityHashMap<Node, NodeInfo>();
		Map<Node, NodeInfo> racksMap = new IdentityHashMap<Node, NodeInfo>();
		String[] allTopos = new String[0];

		// Build the hierarchy and aggregate the contribution of
		// bytes at each level. See TestGetSplitHosts.java

		for (index = startIndex; index <= endIndex; index++) {

			// Establish the bytes in this block
			if (index == startIndex) {
				bytesInThisBlock = bytesInFirstBlock;
			} else if (index == endIndex) {
				bytesInThisBlock = bytesInLastBlock;
			} else {
				bytesInThisBlock = blkLocations[index].getLength();
			}

			allTopos = blkLocations[index].getTopologyPaths();

			// If no topology information is available, just
			// prefix a fakeRack
			if (allTopos.length == 0) {
				allTopos = fakeRacks(blkLocations, index);
			}

			// NOTE: This code currently works only for one level of
			// hierarchy (rack/host). However, it is relatively easy
			// to extend this to support aggregation at different
			// levels

			for (String topo : allTopos) {

				Node node, parentNode;
				NodeInfo nodeInfo, parentNodeInfo;

				node = clusterMap.getNode(topo);

				if (node == null) {
					node = new NodeBase(topo);
					clusterMap.add(node);
				}

				nodeInfo = hostsMap.get(node);

				if (nodeInfo == null) {
					nodeInfo = new NodeInfo(node);
					hostsMap.put(node, nodeInfo);
					parentNode = node.getParent();
					parentNodeInfo = racksMap.get(parentNode);
					if (parentNodeInfo == null) {
						parentNodeInfo = new NodeInfo(parentNode);
						racksMap.put(parentNode, parentNodeInfo);
					}
					parentNodeInfo.addLeaf(nodeInfo);
				} else {
					nodeInfo = hostsMap.get(node);
					parentNode = node.getParent();
					parentNodeInfo = racksMap.get(parentNode);
				}

				nodeInfo.addValue(index, bytesInThisBlock);
				parentNodeInfo.addValue(index, bytesInThisBlock);

			} // for all topos

		} // for all indices

		return identifyHosts(allTopos.length, racksMap);
	}

	private String[] identifyHosts(int replicationFactor, Map<Node, NodeInfo> racksMap) {

		String[] retVal = new String[replicationFactor];

		List<NodeInfo> rackList = new LinkedList<NodeInfo>();

		rackList.addAll(racksMap.values());

		// Sort the racks based on their contribution to this split
		sortInDescendingOrder(rackList);

		boolean done = false;
		int index = 0;

		// Get the host list for all our aggregated items, sort
		// them and return the top entries
		for (NodeInfo ni : rackList) {

			Set<NodeInfo> hostSet = ni.getLeaves();

			List<NodeInfo> hostList = new LinkedList<NodeInfo>();
			hostList.addAll(hostSet);

			// Sort the hosts in this rack based on their contribution
			sortInDescendingOrder(hostList);

			for (NodeInfo host : hostList) {
				// Strip out the port number from the host name
				retVal[index++] = host.node.getName().split(":")[0];
				if (index == replicationFactor) {
					done = true;
					break;
				}
			}

			if (done == true) {
				break;
			}
		}
		return retVal;
	}

	protected int getBlockIndex(BlockLocation[] blkLocations, long offset) {
		for (int i = 0; i < blkLocations.length; i++) {
			// is the offset inside this block?
			if ((blkLocations[i].getOffset() <= offset) && (offset < blkLocations[i].getOffset() + blkLocations[i].getLength())) {
				return i;
			}
		}
		BlockLocation last = blkLocations[blkLocations.length - 1];
		long fileLength = last.getOffset() + last.getLength() - 1;
		throw new IllegalArgumentException("Offset " + offset + " is outside of file (0.." + fileLength + ")");
	}

	private void sortInDescendingOrder(List<NodeInfo> mylist) {
		Collections.sort(mylist, new Comparator<NodeInfo>() {
			public int compare(NodeInfo obj1, NodeInfo obj2) {

				if (obj1 == null || obj2 == null)
					return -1;

				if (obj1.getValue() == obj2.getValue()) {
					return 0;
				} else {
					return ((obj1.getValue() < obj2.getValue()) ? 1 : -1);
				}
			}
		});
	}

	private String[] fakeRacks(BlockLocation[] blkLocations, int index) throws IOException {
		String[] allHosts = blkLocations[index].getHosts();
		String[] allTopos = new String[allHosts.length];
		for (int i = 0; i < allHosts.length; i++) {
			allTopos[i] = NetworkTopology.DEFAULT_RACK + "/" + allHosts[i];
		}
		return allTopos;
	}

	private static class NodeInfo {
		final Node node;
		final Set<Integer> blockIds;
		final Set<NodeInfo> leaves;

		private long value;

		NodeInfo(Node node) {
			this.node = node;
			blockIds = new HashSet<Integer>();
			leaves = new HashSet<NodeInfo>();
		}

		long getValue() {
			return value;
		}

		void addValue(int blockIndex, long value) {
			if (blockIds.add(blockIndex) == true) {
				this.value += value;
			}
		}

		Set<NodeInfo> getLeaves() {
			return leaves;
		}

		void addLeaf(NodeInfo nodeInfo) {
			leaves.add(nodeInfo);
		}
	}

}
