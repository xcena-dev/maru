#pragma once

#include <map>
#include <string>
#include <vector>

namespace maru {

/// A node's current run (epoch) and the host it is bound to.
struct NodeOwner {
    std::string epoch;
    std::string host;
};
using NodeOwners = std::map<std::string, NodeOwner>;
/// Runs of a node that ended (another run claimed it, it was released or its
/// lease ran out), oldest first. No run listed here is the node's run again.
using EndedRuns = std::map<std::string, std::vector<std::string>>;
/// Ended runs kept per node; older ones are forgotten, and a claim by one of
/// them is a new claim.
constexpr size_t kMaxEndedRuns = 64;

/// Node IDs: 1..122 printable ASCII bytes, no spaces, so that the owner ID
/// fits Allocation::clientId.
bool validNodeId(const std::string &id);
/// Owner ID of a node's allocations. '@' is never in a host name, so it
/// never equals a "hostname:pid" client ID.
std::string nodeOwnerId(const std::string &nodeId);
/// The node ID of a node owner ID; "" for a process owner.
std::string nodeIdOfOwner(const char *clientId);

/// Node table in <stateDir>/nodes, in this order: "node <node_id> <epoch>
/// <host>" for a node's current run, "ended <node_id> <epoch>" for its ended
/// runs (oldest first), and "releasing <node_id> <epoch> <host>" for a node
/// whose last run (an ended one) still has regions to free. A missing file
/// means none; a malformed one is -EINVAL, never an empty table.
int loadNodeOwners(const std::string &stateDir, NodeOwners &nodes, NodeOwners &releasing,
                   EndedRuns &ended);
/// Replace the file as saveRegionHolds does, with the same syncRc rule.
int saveNodeOwners(const std::string &stateDir, const NodeOwners &nodes,
                   const NodeOwners &releasing, const EndedRuns &ended, int &syncRc);

/// The RM's own ID (rm_domain) in <stateDir>/rm_id: 32 lowercase hex digits.
/// "" if the file is missing; -EINVAL if it is malformed.
int loadRmId(const std::string &stateDir, std::string &id);
/// Create a random ID and write it durably (directory synced).
int createRmId(const std::string &stateDir, std::string &id);

} // namespace maru
