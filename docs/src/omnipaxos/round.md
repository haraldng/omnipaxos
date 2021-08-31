# Round

Leader event that indicates a leader has been elected. Should be created when the user-defined BLE algorithm outputs a leader event. Should be then handled in Omni-Paxos by calling `crate::omnipaxos::OmniPaxos::handle_leader()`.

> **Note:** Rounds in Omni-Paxos are totally ordered.
