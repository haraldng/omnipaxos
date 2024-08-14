use crate::storage::Entry;
use crate::util::NodeId;
use itertools::Itertools;
use std::collections::HashSet;

pub(crate) const BATCH_ACCEPTED: bool = true;

pub(crate) struct Metronome {
    /// Id of this node
    pub pid: NodeId,
    pub ordering: Vec<usize>
}

type QuorumTuple = Vec<NodeId>;

impl Metronome {
    fn maximize_distance_ordering(tuples: &mut Vec<QuorumTuple>) -> Vec<QuorumTuple> {
        let mut ordered_tuples = vec![tuples.remove(0)];

        while !tuples.is_empty() {
            let valid_tuples: Vec<_> = tuples
                .iter()
                .filter(|t| {
                    let t_set: HashSet<_> = t.iter().collect();
                    let last_set: HashSet<_> = ordered_tuples.last().unwrap().iter().collect();
                    t_set.intersection(&last_set).count() == 0
                })
                .cloned()
                .collect();

            let next_tuple = if !valid_tuples.is_empty() {
                valid_tuples
                    .into_iter()
                    .max_by(|t1, t2| {
                        let dist1 = Self::distance(ordered_tuples.last().unwrap(), t1);
                        let dist2 = Self::distance(ordered_tuples.last().unwrap(), t2);
                        dist1.cmp(&dist2)
                    })
                    .unwrap()
            } else {
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
        assert_eq!(t1.len(), t2.len(), "Vectors must have the same dimension for distance calculation");

        let sum_of_squares: usize = t1
            .iter()
            .zip(t2.iter())
            .map(|(x1, x2)| (*x1 as isize - *x2 as isize).pow(2) as usize)
            .sum();

        // Integer square root approximation (you might want to use a more accurate method if needed)
        (sum_of_squares as f64).sqrt() as usize
    }

    fn create_ordered_quorums(num_nodes: usize, quorum_size: usize) -> Vec<QuorumTuple> {
        let mut quorum_combos = (1..=num_nodes as NodeId).combinations(quorum_size).collect();
        Self::maximize_distance_ordering(&mut quorum_combos)
    }

    fn get_my_ordering(my_pid: NodeId, ordered_quorums: &Vec<QuorumTuple>) -> Vec<usize> {
        let mut ordering = Vec::with_capacity(ordered_quorums.len());
        let mut rest = Vec::with_capacity(ordered_quorums.len()/2);
        for (qid, q) in ordered_quorums.iter().enumerate() {
            if q.contains(&my_pid) {
                ordering.push(qid);
            } else {
                rest.push(qid);
            }
        }
        ordering.append(&mut rest);
        ordering
    }


    fn new(pid: NodeId, num_nodes: usize, quorum_size: usize, batch_size: usize) -> Self {
        let ordered_quorums = Self::create_ordered_quorums(num_nodes, quorum_size);
        let ordering = Self::get_my_ordering(pid, &ordered_quorums);
        Metronome {
            pid,
            ordering
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

        assert_eq!(ordered_tuples, vec![
            vec![1, 2, 3],
            vec![10, 11, 12],
            vec![4, 5, 6],
            vec![7, 8, 9],
        ]);
    }

    #[test]
    fn test_new() {
        let num_nodes = 5;
        let quorum_size = num_nodes / 2 + 1;

        let mut orderings = vec![vec![]; num_nodes];
        for pid in (1..=num_nodes as NodeId) {
            let m = Metronome::new(pid, num_nodes, quorum_size, 10);
            orderings[pid as usize - 1] = m.ordering;
        }
        for o in &orderings {
            println!("{:?}", o);
        }
    }
}