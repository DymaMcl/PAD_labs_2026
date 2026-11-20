//! Construirea comenzilor pentru procesele Go ale laboratoarelor.
//!
//! Panoul NU executa comenzi arbitrare primite din browser: fiecare tip de
//! proces are o lista fixa de parametri, iar valorile sunt validate (numere in
//! intervale, nume doar cu litere/cifre). Asa interfata ramane sigura chiar
//! daca cineva trimite cereri manuale catre ea.

use crate::http::Query;
use std::path::{Path, PathBuf};

/// Un laborator detectat in repository.
#[derive(Debug, Clone)]
pub struct Lab {
    pub id: String, // "lab1" | "lab2"
    pub title: String,
    pub dir: PathBuf,
}

impl Lab {
    pub fn is_lab2(&self) -> bool {
        self.id == "lab2"
    }

    /// Calea unui executabil compilat (`bin/broker` sau `bin\broker.exe`).
    pub fn bin(&self, name: &str) -> PathBuf {
        self.dir
            .join("bin")
            .join(format!("{name}{}", std::env::consts::EXE_SUFFIX))
    }

    /// Toate executabilele sunt compilate?
    pub fn built(&self) -> bool {
        let mut names = vec!["broker", "producer", "consumer", "demo"];
        if self.is_lab2() {
            names.push("accounts");
        }
        names.iter().all(|n| self.bin(n).exists())
    }
}

/// Detecteaza laboratoarele din radacina repository-ului.
pub fn detect_labs(root: &Path) -> Vec<Lab> {
    let mut out = Vec::new();
    for (id, folder, title) in [
        (
            "lab1",
            "laborator_1",
            "Laboratorul 1 · Comunicare fiabilă și mesagerie",
        ),
        (
            "lab2",
            "laborator_2",
            "Laboratorul 2 · RPC, contracte și integrare",
        ),
    ] {
        let dir = root.join(folder);
        if dir.join("go.mod").exists() {
            out.push(Lab {
                id: id.into(),
                title: title.into(),
                dir,
            });
        }
    }
    out
}

/// Tipurile de procese pe care panoul le poate porni.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Build,
    Broker,
    Accounts,
    Consumer,
    Producer,
    Demo,
}

impl Kind {
    pub fn parse(s: &str) -> Option<Kind> {
        Some(match s {
            "build" => Kind::Build,
            "broker" => Kind::Broker,
            "accounts" => Kind::Accounts,
            "consumer" => Kind::Consumer,
            "producer" => Kind::Producer,
            "demo" => Kind::Demo,
            _ => return None,
        })
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Kind::Build => "build",
            Kind::Broker => "broker",
            Kind::Accounts => "accounts",
            Kind::Consumer => "consumer",
            Kind::Producer => "producer",
            Kind::Demo => "demo",
        }
    }
}

/// Specificatia completa a unui proces de pornit.
#[derive(Debug, Clone)]
pub struct Spec {
    pub kind: Kind,
    pub name: String,
    pub program: PathBuf,
    pub args: Vec<String>,
    /// Un singur proces cu acest (tip, nume) poate rula simultan.
    pub unique: bool,
    /// Procesul se termina singur (compilare, producer, demo).
    pub oneshot: bool,
}

impl Spec {
    pub fn command_line(&self) -> String {
        let prog = self
            .program
            .file_name()
            .map(|f| f.to_string_lossy().into_owned())
            .unwrap_or_default();
        let mut s = prog;
        for a in &self.args {
            s.push(' ');
            s.push_str(a);
        }
        s
    }
}

fn int(q: &Query, key: &str, min: i64, max: i64, default: i64) -> Result<i64, String> {
    match q.get(key) {
        None | Some("") => Ok(default),
        Some(v) => {
            let n: i64 = v
                .trim()
                .parse()
                .map_err(|_| format!("{key}: \"{v}\" nu este un numar intreg"))?;
            if n < min || n > max {
                return Err(format!("{key} trebuie sa fie intre {min} si {max}"));
            }
            Ok(n)
        }
    }
}

fn float(q: &Query, key: &str, min: f64, max: f64, default: f64) -> Result<f64, String> {
    match q.get(key) {
        None | Some("") => Ok(default),
        Some(v) => {
            let n: f64 = v
                .trim()
                .parse()
                .map_err(|_| format!("{key}: \"{v}\" nu este un numar"))?;
            if !(min..=max).contains(&n) {
                return Err(format!("{key} trebuie sa fie intre {min} si {max}"));
            }
            Ok(n)
        }
    }
}

/// Nume permis pentru consumer/sursa: 1..24 caractere [A-Za-z0-9_-].
pub fn valid_name(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 24
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

const DEMO_LAB1: &[&str] = &["all", "critic", "producer", "retry", "ordering"];
const DEMO_LAB2: &[&str] = &[
    "lab2",
    "lab1",
    "toate",
    "rpc-critic",
    "ambiguitate",
    "versiuni",
    "deadline",
    "lant",
    "critic",
    "producer",
    "retry",
    "ordering",
];

/// Construieste specificatia procesului cerut, cu validarea parametrilor.
pub fn build_spec(lab: &Lab, kind: Kind, q: &Query) -> Result<Spec, String> {
    let s = |v: &str| v.to_string();
    let spec = match kind {
        Kind::Build => Spec {
            kind,
            name: "compilare".into(),
            program: PathBuf::from("go"),
            args: vec![s("build"), s("-o"), s("bin/"), s("./cmd/...")],
            unique: true,
            oneshot: true,
        },
        Kind::Broker => {
            let lease = int(q, "lease", 1, 120, 5)?;
            Spec {
                kind,
                name: "broker".into(),
                program: lab.bin("broker"),
                args: vec![
                    s("-addr"),
                    s(":7070"),
                    s("-data"),
                    s("data/broker.journal"),
                    s("-lease"),
                    format!("{lease}s"),
                ],
                unique: true,
                oneshot: false,
            }
        }
        Kind::Accounts => {
            if !lab.is_lab2() {
                return Err("serviciul de conturi exista doar in laboratorul 2".into());
            }
            let mut args = vec![
                s("-grpc"),
                s(":7080"),
                s("-http"),
                s(":7081"),
                s("-data"),
                s("data/accounts.journal"),
                s("-drop-rate"),
                format!("{}", float(q, "drop_rate", 0.0, 1.0, 0.0)?),
                s("-drop-next"),
                format!("{}", int(q, "drop_next", 0, 1000, 0)?),
                s("-slow-ms"),
                format!("{}", int(q, "slow_ms", 0, 60_000, 0)?),
                s("-unavailable-next"),
                format!("{}", int(q, "unavailable_next", 0, 1000, 0)?),
            ];
            if q.flag("disable_v2") {
                args.push(s("-disable-v2"));
            }
            Spec {
                kind,
                name: "serviciu-conturi".into(),
                program: lab.bin("accounts"),
                args,
                unique: true,
                oneshot: false,
            }
        }
        Kind::Consumer => {
            let name = q.get("name").unwrap_or("c1").trim().to_string();
            if !valid_name(&name) {
                return Err(
                    "numele consumer-ului poate contine doar litere, cifre, - si _ (max 24)".into(),
                );
            }
            let mut args = vec![
                s("-name"),
                name.clone(),
                s("-lease"),
                format!("{}s", int(q, "lease", 1, 120, 5)?),
                s("-crash-after-effect"),
                format!("{}", int(q, "crash", 0, 1000, 0)?),
                s("-fail-rate"),
                format!("{}", float(q, "fail_rate", 0.0, 1.0, 0.0)?),
                s("-batch"),
                format!("{}", int(q, "batch", 1, 50, 1)?),
            ];
            let effect = q
                .get("effect")
                .unwrap_or(if lab.is_lab2() { "remote" } else { "local" });
            if lab.is_lab2() {
                match effect {
                    "remote" | "local" => args.extend([s("-effect"), s(effect)]),
                    _ => return Err("effect trebuie sa fie remote sau local".into()),
                }
                match q.get("api").unwrap_or("auto") {
                    "auto" | "" => {}
                    "v1" => args.extend([s("-api"), s("v1")]),
                    _ => return Err("api trebuie sa fie auto sau v1".into()),
                }
            } else if effect != "local" {
                return Err("in laboratorul 1 efectul este doar local".into());
            }
            if q.flag("naive") {
                if effect != "local" {
                    return Err(
                        "modul naiv se aplica doar efectului local (registrul din lab. 1)".into(),
                    );
                }
                args.push(s("-naive"));
            }
            Spec {
                kind,
                name,
                program: lab.bin("consumer"),
                args,
                unique: true,
                oneshot: false,
            }
        }
        Kind::Producer => {
            let mut args = vec![
                s("-count"),
                format!("{}", int(q, "count", 1, 1000, 5)?),
                s("-accounts"),
                format!("{}", int(q, "accounts", 1, 50, 1)?),
                s("-amount"),
                format!("{}", int(q, "amount", 1, 10_000_000, 100)?),
                s("-poison-every"),
                format!("{}", int(q, "poison_every", 0, 1000, 0)?),
                s("-interval"),
                s("0s"),
            ];
            if q.flag("invalid") {
                args.push(s("-invalid"));
            }
            Spec {
                kind,
                name: "producer".into(),
                program: lab.bin("producer"),
                args,
                unique: false,
                oneshot: true,
            }
        }
        Kind::Demo => {
            let allowed = if lab.is_lab2() { DEMO_LAB2 } else { DEMO_LAB1 };
            let default = allowed[0];
            let sc = q
                .get("scenario")
                .filter(|v| !v.is_empty())
                .unwrap_or(default);
            if !allowed.contains(&sc) {
                return Err(format!("scenariu necunoscut: {sc}"));
            }
            Spec {
                kind,
                name: format!("demo {sc}"),
                program: lab.bin("demo"),
                args: vec![s("-s"), sc.to_string()],
                unique: false,
                oneshot: true,
            }
        }
    };
    Ok(spec)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lab(id: &str) -> Lab {
        Lab {
            id: id.into(),
            title: String::new(),
            dir: PathBuf::from("/tmp/x"),
        }
    }

    #[test]
    fn consumer_lab1_naive_crash() {
        let q = Query::parse("name=c-naiv&crash=3&naive=1&lease=4");
        let s = build_spec(&lab("lab1"), Kind::Consumer, &q).unwrap();
        assert_eq!(s.name, "c-naiv");
        let line = s.command_line();
        assert!(line.contains("-crash-after-effect 3"), "{line}");
        assert!(line.contains("-naive"));
        assert!(line.contains("-lease 4s"));
        assert!(!line.contains(" -effect "), "lab1 nu cunoaste -effect");
    }

    #[test]
    fn consumer_lab2_remote_v1() {
        let q = Query::parse("name=c2&api=v1");
        let s = build_spec(&lab("lab2"), Kind::Consumer, &q).unwrap();
        let line = s.command_line();
        assert!(
            line.contains("-effect remote") && line.contains("-api v1"),
            "{line}"
        );
    }

    #[test]
    fn rejects_bad_values() {
        let l1 = lab("lab1");
        assert!(build_spec(&l1, Kind::Consumer, &Query::parse("name=a;rm -rf")).is_err());
        assert!(build_spec(&l1, Kind::Consumer, &Query::parse("crash=-1")).is_err());
        assert!(build_spec(&l1, Kind::Consumer, &Query::parse("fail_rate=2")).is_err());
        assert!(build_spec(&l1, Kind::Producer, &Query::parse("count=abc")).is_err());
        assert!(build_spec(&l1, Kind::Accounts, &Query::default()).is_err());
        assert!(build_spec(&l1, Kind::Demo, &Query::parse("scenario=rpc-critic")).is_err());
        assert!(build_spec(&lab("lab2"), Kind::Consumer, &Query::parse("naive=1")).is_err());
    }

    #[test]
    fn accounts_faults() {
        let q = Query::parse("drop_rate=0.3&drop_next=2&disable_v2=1");
        let s = build_spec(&lab("lab2"), Kind::Accounts, &q).unwrap();
        let line = s.command_line();
        assert!(
            line.contains("-drop-rate 0.3")
                && line.contains("-drop-next 2")
                && line.contains("-disable-v2"),
            "{line}"
        );
    }

    #[test]
    fn demo_defaults_per_lab() {
        let s1 = build_spec(&lab("lab1"), Kind::Demo, &Query::default()).unwrap();
        let s2 = build_spec(&lab("lab2"), Kind::Demo, &Query::default()).unwrap();
        assert_eq!(s1.args, vec!["-s", "all"]);
        assert_eq!(s2.args, vec!["-s", "lab2"]);
    }

    #[test]
    fn names() {
        assert!(valid_name("c1") && valid_name("c-naiv_2"));
        assert!(
            !valid_name("")
                && !valid_name("a b")
                && !valid_name("../x")
                && !valid_name(&"x".repeat(25))
        );
    }
}
