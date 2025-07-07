use std::collections::hash_map::Values;
use std::collections::HashMap;
use std::ops::{Deref, DerefMut, Index, IndexMut};

/// A Wrapper around `Hashmap<usize, T>` that behaves like a vector with lazy allocation.
#[derive(Debug, Clone)]
pub struct VecLike<T> {
    default_value: T,
    max_size: usize,
    data: HashMap<usize, T>,
}

impl<T: Clone> VecLike<T> {
    /// Creates a new VecLike with a default value and a maximum size.
    pub fn new(default_value: T, max_size: usize) -> Self {
        Self {
            default_value,
            max_size,
            data: HashMap::new(),
        }
    }

    /// Gets a reference at `Index`
    pub fn get(&self, index: usize) -> Option<&T> {
        if index >= self.max_size {
            None
        } else {
            self.data.get(&index).or(Some(&self.default_value))
        }
    }

    
    #[cfg(test)]
    fn allocated_count(&self) -> usize {
        self.data.len()
    }


    /// is_empty checks if the VecLike is empty.
    pub fn is_empty(&self) -> bool {
        self.max_size == 0
    }

    /// Returns an iterator over the values in the VecLike.
    pub fn iter(&self) -> Values<'_, usize, T>
    {
        self.values()
    }
}

impl <T: Default> Default for VecLike<T> {
    fn default() -> Self {
        Self {
            default_value: T::default(),
            max_size: 0,
            data: HashMap::new(),
        }
    }
}

impl<T: Clone> Index<usize> for VecLike<T> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        if index >= self.max_size {
            panic!("Index {} out of bounds for VecLike of size {}", index, self.max_size);
        }
        self.data.get(&index).unwrap_or(&self.default_value)
    }
}

impl<T: Clone> IndexMut<usize> for VecLike<T> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        if index >= self.max_size {
            panic!("Index {} out of bounds for VecLike of size {}", index, self.max_size);
        }
        self.data.entry(index).or_insert_with(|| self.default_value.clone())
    }
}

// Deref directly into the inner HashMap
impl<T: Clone> Deref for VecLike<T> {
    type Target = HashMap<usize, T>;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<T: Clone> DerefMut for VecLike<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

/// Macro to create a VecLike
#[macro_export]
macro_rules! vec_like {
    ($default:expr; $size:expr) => {
        $crate::utils::VecLike::new($default, $size)
    };
}


#[cfg(test)]
mod tests {

    #[derive(Debug, Clone, PartialEq)]
    enum PromiseState {
        NotPromised,
        PromisedHigher,
        Promised(u64),
    }

    #[test]
    fn test_vec_like_direct_deref_and_index() {
        let mut promises = vec_like![PromiseState::NotPromised; 1000];

        promises[123] = PromiseState::Promised(77);
        promises[456] = PromiseState::PromisedHigher;

        assert_eq!(promises[123], PromiseState::Promised(77));
        assert_eq!(promises[456], PromiseState::PromisedHigher);
        assert_eq!(promises[789], PromiseState::NotPromised);

        assert_eq!(promises.allocated_count(), 2);

        // Accessing underlying hashmap directly
        assert!(promises.contains_key(&123));
        assert_eq!(promises.get(123), Some(&PromiseState::Promised(77)));
    }
}
