# Ballot Leader Election Configuration

To ease up the configuration of the Ballot Leader Election module, a hocon configuration can be provided.

- **CONFIG_ID** - The identifier for the configuration that this Omni-Paxos replica is part of.
- **PID** - The identifier of this replica.
- **HB_DELAY** - A fixed delay between heartbeats. It is measured in ticks.
- **INCREMENT_DELAY** - A fixed delay that is added to the hb_delay. It is measured in ticks.
- **INITIAL_DELAY_FACTOR** - A factor used in the beginning for a shorter hb_delay.
- **LOG_FILE_PATH** - Path where the default logger logs events.
