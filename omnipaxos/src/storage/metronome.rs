use crate::{storage::Entry, util::NodeId};
use itertools::Itertools;
use std::collections::HashSet;

pub(crate) const BATCH_ACCEPTED: bool = false;

pub(crate) struct Metronome {
    /// Id of this node
    pub pid: NodeId,
    pub my_ordering: Vec<usize>,
    pub all_orderings: Vec<Vec<usize>>,
    pub critical_len: usize,
}

impl Metronome {
    pub(crate) fn get_critical_ordering(&self) -> Vec<usize> {
        self.my_ordering
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
            // try order quorums such that there is no common node with the last quorum
            let no_repeat_tuples: Vec<_> = tuples
                .iter()
                .filter(|t| {
                    let t_set: HashSet<_> = t.iter().collect();
                    let last_set: HashSet<_> = ordered_tuples.last().unwrap().iter().collect();
                    t_set.intersection(&last_set).count() == 0 // no common nodes
                })
                .cloned()
                .collect();

            let next_tuple = if !no_repeat_tuples.is_empty() {
                // there is a quorum that has no common nodes with the last quorum, pick the one with the max distance (probably not necessary)
                no_repeat_tuples
                    .into_iter()
                    .max_by(|t1, t2| {
                        let dist1 = Self::distance(ordered_tuples.last().unwrap(), t1);
                        let dist2 = Self::distance(ordered_tuples.last().unwrap(), t2);
                        dist1.cmp(&dist2)
                    })
                    .unwrap()
            } else {
                // all quorums have common nodes with the last quorum, pick the one with the max distance
                tuples
                    .iter()
                    .max_by(|t1, t2| {
                        let dist1 = Self::distance(ordered_tuples.last().unwrap(), t1);
                        let dist2 = Self::distance(ordered_tuples.last().unwrap(), t2);
                        dist1.cmp(&dist2)
                    })
                    .cloned()
                    .unwrap()
            };

            ordered_tuples.push(next_tuple.clone());
            let index = tuples.iter().position(|x| x == &next_tuple).unwrap();
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

        let sum_of_squares: usize = t1
            .iter()
            .zip(t2.iter())
            .map(|(x1, x2)| (*x1 as isize - *x2 as isize).pow(2) as usize)
            .sum();

        // Integer square root approximation (you might want to use a more accurate method if needed)
        (sum_of_squares as f64).sqrt() as usize
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
        let mut rest = Vec::with_capacity(ordered_quorums.len() / 2);
        let mut critical_len = 0;
        for (entry_id, q) in ordered_quorums.iter().enumerate() {
            if q.contains(&my_pid) {
                ordering.push(entry_id);
            } else {
                // I'm not in this quorum i.e., this entry is not part of my "critical batch"
                rest.push(entry_id);
            }
            // set the critical length when processing the last entry
            if critical_len == 0 && entry_id == ordered_quorums.len() - 1 {
                critical_len = ordering.len();
            }
        }
        ordering.append(&mut rest);
        (ordering, critical_len)
    }

    fn get_all_orderings(ordered_quorums: &Vec<QuorumTuple>) -> Vec<Vec<usize>> {
        todo!()
    }

    pub(crate) fn with(pid: NodeId, num_nodes: usize, quorum_size: usize) -> Self {
        let ordered_quorums = Self::create_ordered_quorums(num_nodes, quorum_size);
        let (ordering, critical_len) =
            Self::get_my_ordering_and_critical_len(pid, &ordered_quorums);
        Metronome {
            pid,
            my_ordering: ordering,
            all_orderings: vec![],
            critical_len,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn check_critical_len(all_metronomes: &Vec<Metronome>) {
        let critical_len = all_metronomes[0].critical_len;
        let all_same_critical_len = all_metronomes
            .iter()
            .all(|m| m.critical_len == critical_len);
        assert!(all_same_critical_len);
        let num_nodes = all_metronomes.len();
        let mut all_orderings = Vec::with_capacity(num_nodes);
        for m in all_metronomes {
            all_orderings.push(m.my_ordering.clone());
        }
        let quorum_size = num_nodes / 2 + 1;
        let num_ops = all_orderings[0].len();
        let mut h: HashMap<usize, usize> = HashMap::from_iter((0..num_ops).map(|x| (x, 0)));
        for column in 0..num_ops {
            for ordering in &all_orderings {
                // row
                let op_id = ordering[column];
                h.insert(op_id, h[&op_id] + 1);
            }
            if column == critical_len - 1 {
                for (_, count) in h.iter() {
                    // at critical length, all ops should have been assigned quorum_size times
                    assert_eq!(*count, quorum_size);
                }
            }
        }
        // check all ops where indeed assigned
        for (_, count) in h.iter() {
            assert_eq!(*count, num_nodes);
        }
    }

    #[test]
    fn test_distance() {
        let t1 = vec![1, 2, 3];
        let t2 = vec![4, 5, 6];
        assert_eq!(Metronome::distance(&t1, &t2), 5);
    }

    #[test]
    fn test_maximize_distance_ordering() {
        let mut tuples = vec![
            vec![1, 2, 3],
            vec![4, 5, 6],
            vec![7, 8, 9],
            vec![10, 11, 12],
        ];

        let ordered_tuples = Metronome::maximize_distance_ordering(&mut tuples);

        assert_eq!(
            ordered_tuples,
            vec![
                vec![1, 2, 3],
                vec![10, 11, 12],
                vec![4, 5, 6],
                vec![7, 8, 9],
            ]
        );
    }

    #[test]
    fn test_new() {
        let n = vec![3, 5, 7, 9, 11];
        for num_nodes in n {
            let quorum_size = num_nodes / 2 + 1;
            let mut all_metronomes = Vec::with_capacity(num_nodes);
            for pid in 1..=num_nodes as NodeId {
                let m = Metronome::with(pid, num_nodes, quorum_size);
                if pid == 1 {
                    println!(
                        "N={}: ordering len: {}, critical len: {}",
                        num_nodes,
                        m.my_ordering.len(),
                        m.critical_len
                    );
                }
                all_metronomes.push(m);
            }
            check_critical_len(&all_metronomes);
        }
    }
}
