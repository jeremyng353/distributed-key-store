package com.g2.CPEN431.A11;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Paxos {

    private int nodeId;
    private Map<Integer, Integer> promised;
    private Map<Integer, Integer> accepted;
    private int highestProposalNumber;
    private Set<Integer> aliveNodes;
    private int generationNumber = 0;

    public Paxos(int nodeId, Set<Integer> aliveNodes) {
        this.nodeId = nodeId;
        this.promised = new HashMap<>();
        this.accepted = new HashMap<>();
        this.highestProposalNumber = -1;
        this.aliveNodes = aliveNodes;
    }

    public void prepare(int proposalNumber) {
        // If the proposal number is lower than the highest seen proposal number, ignore it
        if (proposalNumber <= highestProposalNumber) {
            return;
        }

        // Remember the highest proposal number seen so far
        highestProposalNumber = proposalNumber;

        // Send a promise message to all nodes in the system
        for (int node : aliveNodes) {
            // Send a promise message to the node with the proposal number and the highest proposal number and value seen so far
            // If the node has already promised a higher proposal number, don't send another promise
            if (!promised.containsKey(node) || proposalNumber > promised.get(node)) {
                promised.put(node, proposalNumber);
                // Send promise message to node
            }
        }

        generationNumber++;
    }

    public void accept(int proposalNumber, int proposalValue) {
        // If the proposal number is lower than the highest seen proposal number, ignore it
        if (proposalNumber <= highestProposalNumber) {
            return;
        }

        // Remember the highest proposal number seen so far
        highestProposalNumber = proposalNumber;

        // Send an accept message to all nodes in the system
        for (int node : aliveNodes) {
            // Send an accept message to the node with the proposal number and proposal value
            // Only send the message if the node has promised this proposal number
            if (promised.containsKey(node) && proposalNumber == promised.get(node)) {
                accepted.put(node, proposalValue);
                // Send accept message to node
            }
        }
    }

    public int getDecidedValue() {
        int count = 0;
        int decidedValue = -1;
        for (int node : aliveNodes) {
            if (accepted.containsKey(node) && accepted.get(node) == highestProposalNumber) {
                count++;
                decidedValue = accepted.get(node);
            }
        }
        if (count > aliveNodes.size() / 2) {
            return decidedValue;
        } else {
            return -1;
        }
    }
}
