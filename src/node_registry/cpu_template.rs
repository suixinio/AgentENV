//! Computes the conservative bitwise-AND intersection of Firecracker CPU configs.
//!
//! A capability, CPUID register, or MSR survives only when every input contains it.
//! Callers own cluster completeness gating; this module is pure JSON transformation.
//! Output preserves stable field order and zero-padded bitmap formatting.

use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// Firecracker CPU-config shape in output field order.
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

type CpuidLeafKey = (u32, u32, i64);

/// Returns whether KVM forbids overriding the CPUID leaf.
fn is_kvm_read_only_leaf(leaf: u32) -> bool {
    leaf == 0xb
}

/// Intersects configs, retaining only shared entries and ANDing their bitmaps.
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

    let mut leaf_count: HashMap<CpuidLeafKey, usize> = HashMap::new();
    for by_leaf in &per_config {
        for key in by_leaf.keys() {
            *leaf_count.entry(*key).or_insert(0) += 1;
        }
    }

    // Preserve first-config order for deterministic output.
    let mut result = Vec::new();
    let mut seen_leaf: HashSet<CpuidLeafKey> = HashSet::new();
    for modifier in &configs[0].cpuid_modifiers {
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

        let mut reg_count: HashMap<String, usize> = HashMap::new();
        for by_leaf in &per_config {
            if let Some(registers) = by_leaf.get(&key) {
                for register in registers.keys() {
                    *reg_count.entry(register.clone()).or_insert(0) += 1;
                }
            }
        }

        // Seed from the first config; entries absent there cannot be in the intersection.
        let mut register_mods = Vec::new();
        let mut seen_register: HashSet<String> = HashSet::new();
        for register_mod in &modifier.modifiers {
            if !seen_register.insert(register_mod.register.clone()) {
                continue;
            }
            if reg_count.get(&register_mod.register).copied().unwrap_or(0) < n {
                continue; // not present in every config
            }
            // Start AND reduction from its identity.
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

    #[test]
    fn matches_the_real_go_implementation_byte_for_byte() {
        let single_full = r#"{"kvm_capabilities":["cap.a","cap.b"],"cpuid_modifiers":[{"leaf":"0x1","subleaf":"0x0","flags":0,"modifiers":[{"register":"eax","bitmap":"0b00000000000000000000000000001111"}]}],"msr_modifiers":[{"addr":"0x10","bitmap":"0b0000000000000000000000000000000000000000000000000000000011111111"}]}"#;
        assert_eq!(
            intersect_cpu_configs(&[single_full.to_string()]).expect("go golden: single_full"),
            single_full,
            "single-config intersection diverged from the real Go output"
        );

        let two_a = r#"{"kvm_capabilities":["cap.a","cap.b","cap.c"],"cpuid_modifiers":[{"leaf":"0x1","subleaf":"0x0","flags":0,"modifiers":[{"register":"eax","bitmap":"0b00000000000000000000000000001111"},{"register":"ebx","bitmap":"0b00000000000000000000000011110000"}]},{"leaf":"0x7","subleaf":"0x0","flags":0,"modifiers":[{"register":"ecx","bitmap":"0b00000000000000000000000000000001"}]}],"msr_modifiers":[{"addr":"0x10","bitmap":"0b0000000000000000000000000000000000000000000000000000000011111111"},{"addr":"0x20","bitmap":"0b0000000000000000000000000000000000000000000000000000000000001111"}]}"#;
        let two_b = r#"{"kvm_capabilities":["cap.a","cap.c","cap.d"],"cpuid_modifiers":[{"leaf":"0x1","subleaf":"0x0","flags":0,"modifiers":[{"register":"eax","bitmap":"0b00000000000000000000000000000101"},{"register":"ebx","bitmap":"0b00000000000000000000000010100000"}]}],"msr_modifiers":[{"addr":"0x10","bitmap":"0b0000000000000000000000000000000000000000000000000000000010101010"}]}"#;
        let two_want = r#"{"kvm_capabilities":["cap.a","cap.c"],"cpuid_modifiers":[{"leaf":"0x1","subleaf":"0x0","flags":0,"modifiers":[{"register":"eax","bitmap":"0b00000000000000000000000000000101"},{"register":"ebx","bitmap":"0b00000000000000000000000010100000"}]}],"msr_modifiers":[{"addr":"0x10","bitmap":"0b0000000000000000000000000000000000000000000000000000000010101010"}]}"#;
        assert_eq!(
            intersect_cpu_configs(&[two_a.to_string(), two_b.to_string()])
                .expect("go golden: two_intersect_subset"),
            two_want,
            "two-config intersection diverged from the real Go output"
        );

        let three_a = r#"{"kvm_capabilities":["cap.x"],"cpuid_modifiers":[{"leaf":"0xb","subleaf":"0x0","flags":0,"modifiers":[{"register":"eax","bitmap":"0b00000000000000000000000000001111"}]},{"leaf":"0x1","subleaf":"0x0","flags":1,"modifiers":[{"register":"eax","bitmap":"0b00000000000000000000000000000011"}]}],"msr_modifiers":[]}"#;
        let three_b = r#"{"kvm_capabilities":["cap.x"],"cpuid_modifiers":[{"leaf":"0xb","subleaf":"0x0","flags":0,"modifiers":[{"register":"eax","bitmap":"0b00000000000000000000000000000111"}]},{"leaf":"0x1","subleaf":"0x0","flags":1,"modifiers":[{"register":"eax","bitmap":"0b00000000000000000000000000000110"}]}],"msr_modifiers":[]}"#;
        let three_c = r#"{"kvm_capabilities":["cap.x","cap.y"],"cpuid_modifiers":[{"leaf":"0xb","subleaf":"0x0","flags":0,"modifiers":[{"register":"eax","bitmap":"0b00000000000000000000000000000011"}]},{"leaf":"0x1","subleaf":"0x0","flags":1,"modifiers":[{"register":"eax","bitmap":"0b00000000000000000000000000000111"}]}],"msr_modifiers":[]}"#;
        let three_want = r#"{"kvm_capabilities":["cap.x"],"cpuid_modifiers":[{"leaf":"0x1","subleaf":"0x0","flags":1,"modifiers":[{"register":"eax","bitmap":"0b00000000000000000000000000000010"}]}],"msr_modifiers":[]}"#;
        assert_eq!(
            intersect_cpu_configs(&[
                three_a.to_string(),
                three_b.to_string(),
                three_c.to_string()
            ])
            .expect("go golden: three_way_leaf_0xb_readonly"),
            three_want,
            "three-config intersection (with the read-only leaf 0xb dropped) diverged from the \
             real Go output"
        );
    }
}
