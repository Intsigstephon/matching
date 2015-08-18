package com.cloudera.science.matching.graph;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class BipartiteMatching extends AbstractComputation<
    Text, VertexState, IntWritable, AuctionMessage, AuctionMessage> {

    private static final BigDecimal ONE_HUNDRED_BILLION_DOLLARS = new BigDecimal(100L * 1000L * 1000L * 1000L);

    @Override
    public void compute(
        Vertex<Text, VertexState, IntWritable> vertex,
        Iterable<AuctionMessage> msgs) throws IOException {
        long superstep = getSuperstep();
        VertexState state = vertex.getValue();
        List<AuctionMessage> messages = Lists.newArrayList();
        for (AuctionMessage am : msgs) {
            messages.add(am.copy());
        }
        if (state.isBidder()) {
            // Bidders only do work on even supersteps.
            if (superstep % 2 == 0) {
                // Load the data about which object I own, which I'm interested in,
                // and their prices.
                VertexPriceData vpd = new VertexPriceData(messages.iterator(), state.getPriceIndex());

                // Update my object ownership if it has changed.
                if (vpd.newMatchedId != null) {
                    Text currentMatchId = state.getMatchId();
                    if (currentMatchId != null && !currentMatchId.toString().isEmpty()) {
                      sendMessage(currentMatchId, newSignal(vertex.getId(), -1));
                    }
                    state.setMatchId(new Text(vpd.newMatchedId));
                } else if (vpd.newLostId != null) {
                    state.clearMatchId();
                }

                // Compute the value I assign to each object, based on its current price.
                List<AuctionMessage> values = Lists.newArrayList();
                for (Edge<Text, IntWritable> edge : vertex.getEdges()) {
                    BigDecimal price = vpd.getPrice(edge.getTargetVertexId());
                    if (price.compareTo(ONE_HUNDRED_BILLION_DOLLARS) < 0) {
                        BigDecimal value = new BigDecimal(edge.getValue().get()).subtract(price);
                        if (value.compareTo(BigDecimal.ZERO) > 0) {
                            values.add(new AuctionMessage(edge.getTargetVertexId(), value));
                        }
                    }
                }

                if (values.isEmpty()) {
                    // Nothing to bid on, problem is ill-posed. :(
                    vertex.voteToHalt();
                    return;
                }

                // Compare the value I get from the object I own now (if any) to the highest-value
                // object that I am interested in.
                Text currentMatchId = state.getMatchId();
                AuctionMessage target = getMax(values, currentMatchId);
                if (currentMatchId == null || !currentMatchId.equals(target.getVertexId())) {
                    BigDecimal bid = ONE_HUNDRED_BILLION_DOLLARS; // Really big bid, if it's the only match for me.
                    if (values.size() > 1) {
                        // Otherwise, compute the bid relative to the value I get from the first runner-up.
                        AuctionMessage runnerUp = values.get(1);
                        BigDecimal inc = target.getValue().subtract(runnerUp.getValue()).add(getEpsilon());
                        bid = vpd.getPrice(target.getVertexId()).add(inc);
                    }
                    // Make an offer to my new favorite vertex.
                    sendMessage(target.getVertexId(), newMsg(vertex.getId(), bid));
                } else {
                    // Otherwise, I'm happy.
                    vertex.voteToHalt();
                }
            }
        } else {
            // Objects only do work on odd supersteps.
            if (superstep % 2 == 1) {
                BigDecimal price = state.getPrice();
                List<AuctionMessage> bids = sortBids(messages.iterator());

                // Check to see if any of the inputs are actually a rejection signal
                // from the current owner of this object.
                AuctionMessage rejectionSignal = popRejection(bids);
                if (rejectionSignal != null) {
                    state.clearMatchId();
                }

                if (!bids.isEmpty()) {
                    Text currentMatchId = state.getMatchId();
                    AuctionMessage winningBid = bids.get(0);
                    Text newMatchId = winningBid.getVertexId();
                    // Verify that the high bidder beats the current best price.
                    if (currentMatchId == null ||
                            (!currentMatchId.equals(newMatchId) && winningBid.getValue().compareTo(price) > 0)) {
                        state.setMatchId(newMatchId);
                        state.setPrice(winningBid.getValue());
                        // Need to send the owners a heads up.
                        if (currentMatchId != null && !currentMatchId.toString().isEmpty()) {
                            sendMessage(currentMatchId, newSignal(vertex.getId(), -1));
                        }
                        sendMessage(newMatchId, newSignal(vertex.getId(), 1));
                    }
                    // Announce my price to all the bidders.
                    sendMessageToAllEdges(vertex, newMsg(vertex.getId(), state.getPrice()));
                }
            } else {
                // Objects always vote to halt on mod zero iterations.
                vertex.voteToHalt();
            }
        }
    }

    private AuctionMessage getMax(List<AuctionMessage> values, Text currentMatchId) {
        Collections.sort(values);
        if (currentMatchId == null || currentMatchId.toString().isEmpty()) {
            return values.get(0);
        } else {
            AuctionMessage max = values.get(0);
            if (max.getVertexId().equals(currentMatchId)) {
                return max;
            } else {
                AuctionMessage currentValue = null;
                for (int i = 1; i < values.size(); i++) {
                    if (values.get(i).getVertexId().equals(currentMatchId)) {
                        currentValue = values.get(i);
                        break;
                    }
                }
                if (currentValue != null) {
                    BigDecimal plusEps = currentValue.getValue().add(getEpsilon());
                    if (max.getValue().compareTo(plusEps) <= 0) {
                        return currentValue;
                    }
                }
                return max;
            }
        }
    }

    public static class VertexPriceData {
        public Map<Text, BigDecimal> prices;
        public String newMatchedId;
        public String newLostId;

        public VertexPriceData(Iterator<AuctionMessage> iter, Map<Text, BigDecimal> priceIndex) {
            this.prices = priceIndex;
            while (iter.hasNext()) {
                AuctionMessage msg = iter.next();
                if (msg.getSignal() > 0) {
                    newMatchedId = msg.getVertexId().toString();
                } else if (msg.getSignal() < 0) {
                    newLostId = msg.getVertexId().toString();
                } else {
                    prices.put(new Text(msg.getVertexId().toString()), msg.getValue());
                }
            }
        }

        public BigDecimal getPrice(Text vertexId) {
            return prices.containsKey(vertexId) ? prices.get(vertexId) : BigDecimal.ZERO;
        }
    }

    private BigDecimal getEpsilon() {
      BigDecimal two = new BigDecimal(2);
      BigDecimal den = two.add(new BigDecimal(getTotalNumVertices()));
      return two.divide(den, MathContext.DECIMAL64);
    }

    private AuctionMessage newSignal(Text vertexId, int signal) {
      return new AuctionMessage(vertexId, signal);
    }

    private AuctionMessage newMsg(Text vertexId, BigDecimal value) {
      return new AuctionMessage(vertexId, value);
    }

    private AuctionMessage popRejection(List<AuctionMessage> bids) {
      if (bids.get(bids.size() - 1).getSignal() < 0) {
        return bids.remove(bids.size() - 1);
      }
      return null;
    }

    private List<AuctionMessage> sortBids(Iterator<AuctionMessage> msgIterator) {
      List<AuctionMessage> bids = Lists.newArrayList(msgIterator);
      Collections.sort(bids);
      return bids;
    }
}
