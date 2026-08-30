//! Without-replacement sampling for the shadow scorer's best-of-K.
//!
//! Split out from [`super`] for one reason: the draw sequence is the half
//! that decides which node a tie goes to, and a mutation that turns
//! "sample K at random" into "take the first K" is invisible to any test
//! that cannot script the draws. [`ShadowRng`] is that seam.

/// A source of pool indices. One call per draw, each answering "which slot
/// of the *remaining* pool", so a scripted implementation reads as a
/// shrinking-pool transcript rather than as a permutation.
///
/// Total by contract: `n` is always the current pool length and is never
/// zero at a call site, but an implementation must still answer rather
/// than panic — K-1 forbids the shadow path from panicking.
pub trait ShadowRng {
    fn index_below(&mut self, n: usize) -> usize;
}

/// Production's RNG: a per-call thread-local draw.
///
/// 🔴 Deliberately not a shared, seeded, mutex-guarded generator. Placement
/// runs on every `Schedule`, and a process-wide `Mutex<StdRng>` would put a
/// lock on the hot path to buy reproducibility that nothing in production
/// reads (the shadow result is a metric, not a decision).
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

/// A fixed draw transcript, for tests that need the sample order pinned.
///
/// Draws are taken in order and reduced modulo the current pool size, so a
/// transcript is total: running past its end answers `0` rather than
/// panicking.
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

    /// Rewinds to the first draw — §4.2's "每例重置 scripted RNG".
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

/// Draws `min(k, n)` distinct indices from `0..n`, without replacement.
///
/// The pool shrinks in place and keeps its order (`Vec::remove`, not
/// `swap_remove`), so draw *i* addresses the *i*-th surviving slot — the
/// reading a scripted transcript has to be able to rely on.
///
/// 🔴 The output is capped at `n` and allocated at `min(k, n)`: `k` is an
/// operator-supplied `u32`, and reserving `k` slots would let a config
/// typo reserve four billion of them.
pub fn sample_without_replacement(n: usize, k: u32, rng: &mut dyn ShadowRng) -> Vec<usize> {
    let take = usize::try_from(k).unwrap_or(usize::MAX).min(n);
    if take == 0 {
        return Vec::new();
    }
    let mut pool: Vec<usize> = (0..n).collect();
    let mut picked = Vec::with_capacity(take);
    for _ in 0..take {
        let slot = rng.index_below(pool.len());
        // `index_below`'s contract bounds this, but the shadow path may not
        // panic even on a misbehaving implementation.
        let slot = slot.min(pool.len() - 1);
        picked.push(pool.remove(slot));
    }
    picked
}
