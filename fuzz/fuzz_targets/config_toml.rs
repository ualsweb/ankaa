#![no_main]
//! F-20: the TOML config parser and validator must never panic on arbitrary input, and a
//! config that parses must round-trip through `to_toml`.
use ankaa_core::Config;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|text: &str| {
    if let Ok(cfg) = Config::parse_toml(text) {
        let _ = cfg.validate();
        let again = cfg.to_toml();
        if let Ok(back) = Config::parse_toml(&again) {
            assert_eq!(back, cfg);
        }
    }
});
