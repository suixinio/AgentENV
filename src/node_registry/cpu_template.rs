//! Port of `services/scheduler/internal/cpu_template.go` (300 lines) — the
//! bitwise-AND intersection of Firecracker `cpu_config_json` blobs.
//!
//! This is the algorithm behind the CPU-config-intersection link CLAUDE.md
//! calls out as one that "must keep working" across the Stage A port: every
//! node's heartbeat carries `MachineInfo.cpu_config_json`, the registry
//! intersects every reporting node's config in a cluster, and the result
//! rides back on the heartbeat response to gate what a node may hand
//! Firecracker's pre-boot `PUT /cpu-config`. A leaf, register or MSR address
//! only survives into the result if *every* config in the batch has it — a
//! CPU feature only one machine offers must never be requested on a machine
//! that lacks it, so intersection (not union) is the only safe operation.
//!
//! A pure, stateless function: JSON strings in, one JSON string out (or an
//! error on malformed input). No knowledge of the registry, of a cluster, or
//! of "every node has reported" — that gating (`allConfigsReadyLocked` in
//! Go) belongs to whoever calls this, so it lives with the registry state in
//! `super::registry`, not here.
//!
//! Ported field-for-field from `cpu_template.go`'s `cpuConfig` /
//! `cpuidModifier` / `msrModifier` / `registerMod` shapes so the JSON produced
//! here is byte-identical to what the Go scheduler emits for the same input
//! (field order, `"0b"`-prefixed zero-padded bitmaps, empty arrays rather than
//! `null`) — the two must agree because a heartbeat may be answered by
//! whichever of the two implementations Stage A's placement switch currently
//! selects, and a node applying the result cannot tell which one produced it.

use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// Mirrors Go's `cpuConfig`: the top-level Firecracker `cpu_config_json`
/// structure. Field order matters — it is JSON output order, and Go's
/// `encoding/json` emits struct fields in declaration order with no
/// whitespace, which is what `serde_json::to_string` does too as long as the
/// field order here matches.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct CpuConfig {
    #[serde(default)]
    kvm_capabilities: Vec<String>,
    #[serde(default)]
    cpuid_modifiers: Vec<CpuidModifier>,
    #[serde(default)]
    msr_modifiers: Vec<MsrModifier>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CpuidModifier {
    leaf: String,
    subleaf: String,
    #[serde(default)]
    flags: i64,
    #[serde(default)]
    modifiers: Vec<RegisterMod>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RegisterMod {
    register: String,
    bitmap: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MsrModifier {
    addr: String,
    bitmap: String,
}

/// `(leaf, subleaf, flags)` — the composite key identifying a CPUID entry.
type CpuidLeafKey = (u32, u32, i64);

/// KVM refuses to let a leaf in this set be overridden (leaf `0xb`, the
/// topology enumeration leaf).
fn is_kvm_read_only_leaf(leaf: u32) -> bool {
    leaf == 0xb
}

/// Computes a conservative bitwise-AND intersection of Firecracker
/// `cpu_config_json` strings. Only entries present in every input config are
/// retained; bitmap fields for shared entries are ANDed together. Returns an
/// empty string when `jsons` is empty.
pub fn intersect_cpu_configs(jsons: &[String]) -> Result<String> {
    if jsons.is_empty() {
        return Ok(String::new());
    }

    let configs: Vec<CpuConfig> = jsons
        .iter()
        .enumerate()
        .map(|(i, j)| serde_json::from_str(j).with_context(|| format!("parse config {i}")))
        .collect::<Result<_>>()?;

    let cpuid_modifiers = intersect_cpuid_modifiers(&configs)?;
    let msr_modifiers = intersect_msr_modifiers(&configs)?;
    let kvm_capabilities = intersect_kvm_capabilities(&configs);

    let result = CpuConfig {
        kvm_capabilities,
        cpuid_modifiers,
        msr_modifiers,
    };

    serde_json::to_string(&result).context("marshal result")
}

fn make_cpuid_leaf_key(modifier: &CpuidModifier) -> Result<CpuidLeafKey> {
    let leaf = parse_hex_u32(&modifier.leaf).context("parse leaf")?;
    let subleaf = parse_hex_u32(&modifier.subleaf).context("parse subleaf")?;
    Ok((leaf, subleaf, modifier.flags))
}

fn intersect_cpuid_modifiers(configs: &[CpuConfig]) -> Result<Vec<CpuidModifier>> {
    let n = configs.len();

    // Build per-config maps: leaf key -> register -> u32 bitmap value.
    let mut per_config: Vec<HashMap<CpuidLeafKey, HashMap<String, u32>>> = Vec::with_capacity(n);
    for (i, cfg) in configs.iter().enumerate() {
        let mut by_leaf: HashMap<CpuidLeafKey, HashMap<String, u32>> = HashMap::new();
        for modifier in &cfg.cpuid_modifiers {
            let key = make_cpuid_leaf_key(modifier).with_context(|| {
                format!("config {i} cpuid {}/{}", modifier.leaf, modifier.subleaf)
            })?;
            let entry = by_leaf.entry(key).or_default();
            for register_mod in &modifier.modifiers {
                let value = parse_bitmap_u32(&register_mod.bitmap).with_context(|| {
                    format!(
                        "config {i} cpuid {} {} bitmap",
                        modifier.leaf, register_mod.register
                    )
                })?;
                entry.insert(register_mod.register.clone(), value);
            }
        }
        per_config.push(by_leaf);
    }

    // Count how many configs contain each leaf key.
    let mut leaf_count: HashMap<CpuidLeafKey, usize> = HashMap::new();
    for by_leaf in &per_config {
        for key in by_leaf.keys() {
            *leaf_count.entry(*key).or_insert(0) += 1;
        }
    }

    // Iterate in the order of configs[0] so output is deterministic.
    let mut result = Vec::new();
    let mut seen_leaf: HashSet<CpuidLeafKey> = HashSet::new();
    for modifier in &configs[0].cpuid_modifiers {
        // Already validated above.
        let key = make_cpuid_leaf_key(modifier)?;
        if !seen_leaf.insert(key) {
            continue;
        }
        if leaf_count.get(&key).copied().unwrap_or(0) < n {
            continue; // not present in every config
        }
        if is_kvm_read_only_leaf(key.0) {
            continue; // KVM does not allow overriding this leaf
        }

        // Count how many configs have each register for this leaf.
        let mut reg_count: HashMap<String, usize> = HashMap::new();
        for by_leaf in &per_config {
            if let Some(registers) = by_leaf.get(&key) {
                for register in registers.keys() {
                    *reg_count.entry(register.clone()).or_insert(0) += 1;
                }
            }
        }

        // The register candidate set is seeded from configs[0], same
        // conservative-but-not-maximal note as the Go source: registers
        // present in configs[1..n] but absent in configs[0] are never
        // visited, even if present in every config.
        let mut register_mods = Vec::new();
        let mut seen_register: HashSet<String> = HashSet::new();
        for register_mod in &modifier.modifiers {
            if !seen_register.insert(register_mod.register.clone()) {
                continue;
            }
            if reg_count.get(&register_mod.register).copied().unwrap_or(0) < n {
                continue; // not present in every config
            }
            // AND across all configs. Starting value is all-ones (AND identity).
            let mut and_value: u32 = u32::MAX;
            for by_leaf in &per_config {
                let value = by_leaf
                    .get(&key)
                    .and_then(|registers| registers.get(&register_mod.register))
                    .copied()
                    .unwrap_or(0);
                and_value &= value;
            }
            register_mods.push(RegisterMod {
                register: register_mod.register.clone(),
                bitmap: format_bitmap(u64::from(and_value), 32),
            });
        }

        if !register_mods.is_empty() {
            result.push(CpuidModifier {
                leaf: modifier.leaf.clone(),
                subleaf: modifier.subleaf.clone(),
                flags: modifier.flags,
                modifiers: register_mods,
            });
        }
    }

    Ok(result)
}

fn intersect_msr_modifiers(configs: &[CpuConfig]) -> Result<Vec<MsrModifier>> {
    let n = configs.len();

    let mut per_config: Vec<HashMap<u32, u64>> = Vec::with_capacity(n);
    for (i, cfg) in configs.iter().enumerate() {
        let mut by_addr: HashMap<u32, u64> = HashMap::new();
        for modifier in &cfg.msr_modifiers {
            let addr = parse_hex_u32(&modifier.addr)
                .with_context(|| format!("config {i} msr addr {:?}", modifier.addr))?;
            let value = parse_bitmap_u64(&modifier.bitmap)
                .with_context(|| format!("config {i} msr {} bitmap", modifier.addr))?;
            by_addr.insert(addr, value);
        }
        per_config.push(by_addr);
    }

    let mut addr_count: HashMap<u32, usize> = HashMap::new();
    for by_addr in &per_config {
        for addr in by_addr.keys() {
            *addr_count.entry(*addr).or_insert(0) += 1;
        }
    }

    let mut result = Vec::new();
    let mut seen_addr: HashSet<u32> = HashSet::new();
    for modifier in &configs[0].msr_modifiers {
        // Already validated above.
        let addr = parse_hex_u32(&modifier.addr)?;
        if !seen_addr.insert(addr) {
            continue;
        }
        if addr_count.get(&addr).copied().unwrap_or(0) < n {
            continue;
        }
        let mut and_value: u64 = u64::MAX;
        for by_addr in &per_config {
            and_value &= by_addr.get(&addr).copied().unwrap_or(0);
        }
        result.push(MsrModifier {
            addr: modifier.addr.clone(),
            bitmap: format_bitmap(and_value, 64),
        });
    }

    Ok(result)
}

fn intersect_kvm_capabilities(configs: &[CpuConfig]) -> Vec<String> {
    let mut counts: HashMap<String, usize> = HashMap::new();
    for cfg in configs {
        let mut seen: HashSet<&str> = HashSet::new();
        for capability in &cfg.kvm_capabilities {
            if seen.insert(capability.as_str()) {
                *counts.entry(capability.clone()).or_insert(0) += 1;
            }
        }
    }
    let n = configs.len();
    let mut result: Vec<String> = counts
        .into_iter()
        .filter(|(_, count)| *count == n)
        .map(|(capability, _)| capability)
        .collect();
    result.sort();
    result
}

fn parse_hex_u32(s: &str) -> Result<u32> {
    let lower = s.to_lowercase();
    let trimmed = lower.strip_prefix("0x").unwrap_or(&lower);
    u32::from_str_radix(trimmed, 16).map_err(|e| anyhow!("invalid hex value {s:?}: {e}"))
}

fn parse_bitmap_u32(s: &str) -> Result<u32> {
    let trimmed = s.strip_prefix("0b").unwrap_or(s);
    u32::from_str_radix(trimmed, 2).map_err(|e| anyhow!("invalid bitmap {s:?}: {e}"))
}

fn parse_bitmap_u64(s: &str) -> Result<u64> {
    let trimmed = s.strip_prefix("0b").unwrap_or(s);
    u64::from_str_radix(trimmed, 2).map_err(|e| anyhow!("invalid bitmap {s:?}: {e}"))
}

/// Serializes `v` as a `"0b"`-prefixed binary string, zero-padded to `bits`
/// digits (32 for CPUID registers, 64 for MSRs).
fn format_bitmap(v: u64, bits: usize) -> String {
    format!("0b{v:0bits$b}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bm32(v: u32) -> String {
        format!("0b{v:032b}")
    }

    fn bm64(v: u64) -> String {
        format!("0b{v:064b}")
    }

    fn build_config(
        kvm_caps: Vec<String>,
        cpuid_entries: Vec<CpuidModifier>,
        msr_entries: Vec<MsrModifier>,
    ) -> String {
        serde_json::to_string(&CpuConfig {
            kvm_capabilities: kvm_caps,
            cpuid_modifiers: cpuid_entries,
            msr_modifiers: msr_entries,
        })
        .expect("serialize test fixture")
    }

    fn cpuid(leaf: &str, subleaf: &str, modifiers: Vec<RegisterMod>) -> CpuidModifier {
        CpuidModifier {
            leaf: leaf.to_string(),
            subleaf: subleaf.to_string(),
            flags: 0,
            modifiers,
        }
    }

    fn reg(register: &str, bitmap: String) -> RegisterMod {
        RegisterMod {
            register: register.to_string(),
            bitmap,
        }
    }

    fn msr(addr: &str, bitmap: String) -> MsrModifier {
        MsrModifier {
            addr: addr.to_string(),
            bitmap,
        }
    }

    fn parsed_result(jsons: &[String]) -> CpuConfig {
        let out = intersect_cpu_configs(jsons).expect("IntersectCpuConfigs error");
        serde_json::from_str(&out).expect("unmarshal result")
    }

    #[test]
    fn empty_input_is_empty_string() {
        let out = intersect_cpu_configs(&[]).expect("unexpected error");
        assert_eq!(out, "");
    }

    #[test]
    fn single_config_drops_read_only_leaf() {
        let safe = cpuid("0x1", "0x0", vec![reg("eax", bm32(1))]);
        let input = build_config(
            vec![],
            vec![safe.clone(), cpuid("0xb", "0x1", vec![reg("eax", bm32(2))])],
            vec![],
        );
        let want = build_config(vec![], vec![safe], vec![]);
        let out = intersect_cpu_configs(&[input]).expect("unexpected error");
        assert_eq!(out, want, "single config");
    }

    #[test]
    fn cpuid_bitmap_and() {
        let mod_a = cpuid("0x1", "0x0", vec![reg("eax", bm32(0xFFFF_FFFF))]);
        let mod_b = cpuid("0x1", "0x0", vec![reg("eax", bm32(0x0F0F_0F0F))]);
        let cfg_a = build_config(vec![], vec![mod_a], vec![]);
        let cfg_b = build_config(vec![], vec![mod_b], vec![]);

        let result = parsed_result(&[cfg_a, cfg_b]);

        assert_eq!(result.cpuid_modifiers.len(), 1);
        assert_eq!(
            result.cpuid_modifiers[0].modifiers[0].bitmap,
            bm32(0x0F0F_0F0F)
        );
    }

    #[test]
    fn cpuid_extra_leaf_dropped() {
        let leaf1 = cpuid("0x1", "0x0", vec![reg("eax", bm32(0xFFFF))]);
        let leaf7 = cpuid("0x7", "0x0", vec![reg("ebx", bm32(0xABCD))]);
        let cfg_a = build_config(vec![], vec![leaf1.clone(), leaf7], vec![]);
        let cfg_b = build_config(vec![], vec![leaf1], vec![]);

        let result = parsed_result(&[cfg_a, cfg_b]);

        assert!(!result.cpuid_modifiers.iter().any(|m| m.leaf == "0x7"));
        assert_eq!(result.cpuid_modifiers.len(), 1);
    }

    #[test]
    fn cpuid_extra_register_dropped() {
        let mod_a = cpuid(
            "0x1",
            "0x0",
            vec![reg("eax", bm32(0xF)), reg("ecx", bm32(0xF))],
        );
        let mod_b = cpuid("0x1", "0x0", vec![reg("eax", bm32(0x3))]);
        let cfg_a = build_config(vec![], vec![mod_a], vec![]);
        let cfg_b = build_config(vec![], vec![mod_b], vec![]);

        let result = parsed_result(&[cfg_a, cfg_b]);

        assert_eq!(result.cpuid_modifiers.len(), 1);
        let regs = &result.cpuid_modifiers[0].modifiers;
        assert_eq!(regs.len(), 1);
        assert_eq!(regs[0].register, "eax");
        assert_eq!(regs[0].bitmap, bm32(0x3));
    }

    #[test]
    fn msr_bitmap_and() {
        let msr_a = msr("0x10a", bm64(0xFFFF_FFFF_FFFF_FFFF));
        let msr_b = msr("0x10a", bm64(0x0000_0000_0000_00FF));
        let cfg_a = build_config(vec![], vec![], vec![msr_a]);
        let cfg_b = build_config(vec![], vec![], vec![msr_b]);

        let result = parsed_result(&[cfg_a, cfg_b]);

        assert_eq!(result.msr_modifiers.len(), 1);
        assert_eq!(result.msr_modifiers[0].bitmap, bm64(0x0000_0000_0000_00FF));
    }

    #[test]
    fn msr_extra_addr_dropped() {
        let common = msr("0x10a", bm64(0xFF));
        let extra = msr("0x10b", bm64(0xFF));
        let cfg_a = build_config(vec![], vec![], vec![common.clone(), extra]);
        let cfg_b = build_config(vec![], vec![], vec![common]);

        let result = parsed_result(&[cfg_a, cfg_b]);

        assert!(!result.msr_modifiers.iter().any(|m| m.addr == "0x10b"));
        assert_eq!(result.msr_modifiers.len(), 1);
    }

    #[test]
    fn kvm_capabilities_intersect() {
        let cfg_a = build_config(
            vec!["cap-a".to_string(), "cap-b".to_string()],
            vec![],
            vec![],
        );
        let cfg_b = build_config(
            vec!["cap-b".to_string(), "cap-c".to_string()],
            vec![],
            vec![],
        );

        let result = parsed_result(&[cfg_a, cfg_b]);

        assert_eq!(result.kvm_capabilities, vec!["cap-b".to_string()]);
    }

    #[test]
    fn three_configs_and_across_all() {
        let build = |v: u32| cpuid("0x1", "0x0", vec![reg("eax", bm32(v))]);
        let cfg_a = build_config(vec![], vec![build(0xFF)], vec![]);
        let cfg_b = build_config(vec![], vec![build(0x0F)], vec![]);
        let cfg_c = build_config(vec![], vec![build(0x03)], vec![]);

        let result = parsed_result(&[cfg_a, cfg_b, cfg_c]);

        assert_eq!(result.cpuid_modifiers[0].modifiers[0].bitmap, bm32(0x03));
    }

    #[test]
    fn hex_normalization_treats_0x01_and_0x1_as_same_leaf() {
        let mod_a = cpuid("0x01", "0x00", vec![reg("eax", bm32(0xFF))]);
        let mod_b = cpuid("0x1", "0x0", vec![reg("eax", bm32(0x0F))]);
        let cfg_a = build_config(vec![], vec![mod_a], vec![]);
        let cfg_b = build_config(vec![], vec![mod_b], vec![]);

        let result = parsed_result(&[cfg_a, cfg_b]);

        assert_eq!(result.cpuid_modifiers.len(), 1, "hex-normalized leaf");
        assert_eq!(result.cpuid_modifiers[0].modifiers[0].bitmap, bm32(0x0F));
    }

    #[test]
    fn identical_configs_are_unchanged() {
        let modifier = cpuid("0x1", "0x0", vec![reg("eax", bm32(0xABCD_1234))]);
        let cfg = build_config(vec![], vec![modifier], vec![]);

        let result = parsed_result(&[cfg.clone(), cfg]);

        assert_eq!(
            result.cpuid_modifiers[0].modifiers[0].bitmap,
            bm32(0xABCD_1234)
        );
    }

    #[test]
    fn invalid_json_is_an_error() {
        let err = intersect_cpu_configs(&["{}".to_string(), "not-json".to_string()]);
        assert!(err.is_err(), "expected error for invalid JSON");
    }

    #[test]
    fn invalid_bitmap_is_an_error() {
        let bad = build_config(
            vec![],
            vec![cpuid("0x1", "0x0", vec![reg("eax", "0bXXXX".to_string())])],
            vec![],
        );
        let good = build_config(
            vec![],
            vec![cpuid("0x1", "0x0", vec![reg("eax", bm32(0xFF))])],
            vec![],
        );

        let err = intersect_cpu_configs(&[good, bad]);
        assert!(err.is_err(), "expected error for invalid bitmap string");
    }

    #[test]
    fn empty_slices_serialize_as_arrays_not_null() {
        let cfg_a = build_config(vec![], vec![], vec![]);
        let cfg_b = build_config(vec![], vec![], vec![]);

        let out = intersect_cpu_configs(&[cfg_a, cfg_b]).expect("unexpected error");
        let raw: serde_json::Value = serde_json::from_str(&out).expect("unmarshal");
        for field in ["kvm_capabilities", "cpuid_modifiers", "msr_modifiers"] {
            assert!(
                raw.get(field).is_some_and(|v| v.is_array()),
                "{field} did not serialize as an array"
            );
        }
    }

    // 🔴 Regression guard: intersection must never leak a config's own
    // entries through unfiltered. If `intersect_cpuid_modifiers` (or the msr
    // / kvm-capability counterparts) is ever simplified into "return
    // configs[0]'s own data" — the kind of shortcut that looks harmless when
    // configs[0] happens to be the narrowest one in a test fixture — this
    // catches it: pairing a config that has entries against one that has
    // none must intersect down to nothing, not echo the wide side.
    #[test]
    fn intersection_never_exceeds_the_narrowest_config() {
        let wide = build_config(
            vec!["cap-a".to_string()],
            vec![cpuid("0x1", "0x0", vec![reg("eax", bm32(0xFFFF_FFFF))])],
            vec![msr("0x10a", bm64(0xFFFF_FFFF_FFFF_FFFF))],
        );
        let narrow = build_config(vec![], vec![], vec![]);

        let result = parsed_result(&[wide, narrow]);

        assert!(result.kvm_capabilities.is_empty());
        assert!(result.cpuid_modifiers.is_empty());
        assert!(result.msr_modifiers.is_empty());
    }
}
