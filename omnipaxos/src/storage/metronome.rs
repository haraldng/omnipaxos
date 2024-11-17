use crate::{storage::Entry, util::NodeId};
use itertools::Itertools;
use std::collections::{HashMap, HashSet};

pub struct Metronome {
    /// Id of this node
    pub pid: NodeId,
    pub my_critical_ordering: Vec<usize>,
    pub all_orderings: Vec<Vec<usize>>,
    pub critical_len: usize,
    pub total_len: usize,
}

impl Metronome {
    pub(crate) fn get_critical_ordering(&self) -> Vec<usize> {
        self.my_critical_ordering
            .iter()
            .take(self.critical_len)
            .cloned()
            .collect()
    }
}

type QuorumTuple = Vec<NodeId>;

impl Metronome {
    /// Takes all possible quorum combinations and returns an ordering of quorums that maximizes the distance between consecutive quorums
    fn maximize_distance_ordering(tuples: &mut Vec<QuorumTuple>) -> Vec<QuorumTuple> {
        let mut ordered_tuples = vec![tuples.remove(0)];

        while !tuples.is_empty() {
            // all quorums have common nodes with the last quorum, pick the one with the max distance
            let next_tuple = tuples
                .iter()
                .max_by(|t1, t2| {
                    let dist1 = Self::distance(ordered_tuples.last().unwrap(), t1);
                    let dist2 = Self::distance(ordered_tuples.last().unwrap(), t2);
                    dist1.cmp(&dist2)
                })
                .cloned()
                .unwrap();
            let index = tuples.iter().position(|x| x == &next_tuple).unwrap();
            ordered_tuples.push(next_tuple);
            tuples.remove(index);
        }

        ordered_tuples
    }

    fn distance(t1: &QuorumTuple, t2: &QuorumTuple) -> usize {
        assert_eq!(
            t1.len(),
            t2.len(),
            "Vectors must have the same dimension for distance calculation"
        );

        let mut distance = 0;
        for node in t1 {
            if !t2.contains(node) {
                distance += 1;
            }
        }
        distance
    }

    fn create_ordered_quorums(num_nodes: usize, quorum_size: usize) -> Vec<QuorumTuple> {
        let mut quorum_combos = (1..=num_nodes as NodeId)
            .combinations(quorum_size)
            .collect();
        Self::maximize_distance_ordering(&mut quorum_combos)
    }

    /// Takes the ordered quorums and returns the ordering of the quorums for this node and the critical length
    fn get_my_ordering_and_critical_len(
        my_pid: NodeId,
        ordered_quorums: &Vec<QuorumTuple>,
    ) -> (Vec<usize>, usize) {
        let mut ordering = Vec::with_capacity(ordered_quorums.len());
        for (entry_id, q) in ordered_quorums.iter().enumerate() {
            if q.contains(&my_pid) {
                ordering.push(entry_id);
            }
        }
        let critical_len = ordering.len();
        (ordering, critical_len)
    }

    fn get_all_orderings(ordered_quorums: &Vec<QuorumTuple>) -> Vec<Vec<usize>> {
        todo!()
    }

    pub fn with(pid: NodeId, num_nodes: usize, quorum_size: usize) -> Self {
        let ordered_quorums = Self::create_ordered_quorums(num_nodes, quorum_size);
        let (ordering, critical_len) =
            Self::get_my_ordering_and_critical_len(pid, &ordered_quorums);
        Metronome {
            pid,
            my_critical_ordering: ordering,
            all_orderings: vec![],
            critical_len,
            total_len: ordered_quorums.len(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// Check that the ordering of the critical slots is increasing, this is important because if not increasing we would need to buffer some entries in omnipaxos
    /// e.g., if ordering is [3, 2, 1] then we will first handle entry 2 that must be buffered until we have flushed entry 3.
    fn check_increasing_slot(all_metronomes: &Vec<Metronome>) {
        for m in all_metronomes {
            let ordering = &m.my_critical_ordering;
            for i in 0..ordering.len() - 1 {
                assert!(ordering[i] < ordering[i + 1]);
            }
        }
    }

    /// check that all metronomes have the same critical length and that all operations are assigned quorum_size times at the critical length
    fn check_critical_len(all_metronomes: &Vec<Metronome>, quorum_size: usize) {
        let critical_len = all_metronomes[0].critical_len;
        let all_same_critical_len = all_metronomes
            .iter()
            .all(|m| m.critical_len == critical_len);
        assert!(all_same_critical_len);
        let num_nodes = all_metronomes.len();
        let mut num_ops = 0;
        let mut all_orderings = Vec::with_capacity(num_nodes);
        for m in all_metronomes {
            let ordering = m.get_critical_ordering();
            let max = ordering.iter().max().unwrap();
            if max > &num_ops {
                num_ops = *max;
            }
            all_orderings.push(ordering.clone());
        }
        let mut op_counters: HashMap<usize, usize> =
            HashMap::from_iter((0..=num_ops).map(|x| (x, 0)));
        for column in 0..num_ops {
            for ordering in &all_orderings {
                match ordering.get(column) {
                    Some(op_id) => {
                        op_counters.insert(*op_id, op_counters[op_id] + 1);
                    }
                    None => {}
                }
            }
            if column == critical_len - 1 {
                for (_, count) in op_counters.iter() {
                    // at critical length, all ops should have been assigned quorum_size times
                    assert_eq!(*count, quorum_size);
                }
            }
        }
        // check all ops where indeed assigned
        for (op_id, count) in op_counters.iter() {
            assert_eq!(*count, quorum_size, "op_id: {}", op_id);
        }
    }

    #[test]
    fn test_distance() {
        let t1 = vec![1, 2, 3];
        let t2 = vec![3, 5, 6];
        assert_eq!(Metronome::distance(&t1, &t2), 2);
    }

    #[test]
    fn test_maximize_distance_ordering() {
        let mut tuples = vec![
            vec![1, 2, 3],
            vec![3, 4, 5],
            vec![1, 2, 4],
            vec![4, 5, 6],
        ];

        let ordered_tuples = Metronome::maximize_distance_ordering(&mut tuples);

        assert_eq!(
            ordered_tuples,
            vec![
                vec![1, 2, 3],
                vec![4, 5, 6],
                vec![1, 2, 4],
                vec![3, 4, 5],
            ]
        );
    }

    #[test]
    fn test_new_metronome() {
        let n = vec![3, 5, 7];
        for num_nodes in n {
            let quorum_size = num_nodes / 2 + 1;
            let mut all_metronomes = Vec::with_capacity(num_nodes);
            for pid in 1..=num_nodes as NodeId {
                let m = Metronome::with(pid, num_nodes, quorum_size);
                if pid == 1 {
                    println!(
                        "N={}: ordering len: {}, critical len: {}, total len: {},",
                        num_nodes,
                        m.my_critical_ordering.len(),
                        m.critical_len,
                        m.total_len
                    );
                }
                println!("Node {}: {:?}", pid, m.my_critical_ordering);
                all_metronomes.push(m);
            }
            check_critical_len(&all_metronomes, quorum_size);
            check_increasing_slot(&all_metronomes);
        }
    }
}
