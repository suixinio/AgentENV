//! Scriptable without-replacement sampling for shadow placement.

/// Produces an index into the current nonempty shrinking pool without panicking.
pub trait ShadowRng {
    fn index_below(&mut self, n: usize) -> usize;
}

/// Lock-free thread-local production sampler.
#[derive(Debug, Default, Clone, Copy)]
pub struct ThreadShadowRng;

impl ShadowRng for ThreadShadowRng {
    fn index_below(&mut self, n: usize) -> usize {
        use rand::RngExt as _;
        if n <= 1 {
            return 0;
        }
        rand::rng().random_range(0..n)
    }
}

/// Total scripted draw sequence used to pin test sample order.
#[derive(Debug, Clone, Default)]
pub struct ScriptedRng {
    draws: Vec<usize>,
    next: usize,
}

impl ScriptedRng {
    pub fn new(draws: impl Into<Vec<usize>>) -> Self {
        Self {
            draws: draws.into(),
            next: 0,
        }
    }

    pub fn reset(&mut self) {
        self.next = 0;
    }
}

impl ShadowRng for ScriptedRng {
    fn index_below(&mut self, n: usize) -> usize {
        if n == 0 {
            return 0;
        }
        let draw = self.draws.get(self.next).copied().unwrap_or(0);
        self.next += 1;
        draw % n
    }
}

/// Draws `min(k, n)` distinct ordered-pool indices without replacement.
///
/// Capacity is capped at `n`, not the operator-supplied `k`.
pub fn sample_without_replacement(n: usize, k: u32, rng: &mut dyn ShadowRng) -> Vec<usize> {
    let take = usize::try_from(k).unwrap_or(usize::MAX).min(n);
    if take == 0 {
        return Vec::new();
    }
    let mut pool: Vec<usize> = (0..n).collect();
    let mut picked = Vec::with_capacity(take);
    for _ in 0..take {
        let slot = rng.index_below(pool.len());
        // Preserve shadow infallibility even for a misbehaving RNG.
        let slot = slot.min(pool.len() - 1);
        picked.push(pool.remove(slot));
    }
    picked
}
