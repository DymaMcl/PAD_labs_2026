//! Supervizorul de procese: porneste executabilele Go ale laboratoarelor,
//! le captureaza iesirea linie cu linie si le poate opri (inclusiv "brutal",
//! pentru a simula caderea unui consumer).

use crate::commands::{Lab, Spec};
use crate::json;
use std::collections::VecDeque;
use std::io::{BufRead, BufReader, Read};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

const MAX_LINES: usize = 8000;

/// O linie de iesire a unui proces.
#[derive(Debug, Clone)]
pub struct Line {
    pub seq: u64,
    pub pid: u64,
    pub text: String,
}

/// Informatii publice despre un proces.
#[derive(Debug, Clone)]
pub struct ProcInfo {
    pub id: u64,
    pub kind: String,
    pub name: String,
    pub lab: String,
    pub cmdline: String,
    pub started_ms: u128,
    pub status: String, // running | exited | stopped
    pub exit_code: Option<i32>,
    pub oneshot: bool,
}

impl ProcInfo {
    pub fn to_json(&self) -> String {
        json::obj(&[
            ("id", self.id.to_string()),
            ("kind", json::str(&self.kind)),
            ("name", json::str(&self.name)),
            ("lab", json::str(&self.lab)),
            ("cmdline", json::str(&self.cmdline)),
            ("started_ms", self.started_ms.to_string()),
            ("status", json::str(&self.status)),
            (
                "exit_code",
                self.exit_code
                    .map(|c| c.to_string())
                    .unwrap_or_else(|| "null".into()),
            ),
            ("oneshot", self.oneshot.to_string()),
        ])
    }
}

struct Entry {
    info: ProcInfo,
    child: Option<Child>,
}

#[derive(Default)]
struct Inner {
    next_id: u64,
    seq: u64,
    procs: Vec<Entry>,
    lines: VecDeque<Line>,
}

impl Inner {
    fn push_line(&mut self, pid: u64, text: String) {
        self.seq += 1;
        self.lines.push_back(Line {
            seq: self.seq,
            pid,
            text,
        });
        while self.lines.len() > MAX_LINES {
            self.lines.pop_front();
        }
    }

    /// Actualizeaza starea proceselor care s-au terminat singure.
    fn refresh(&mut self) {
        let mut ended = Vec::new();
        for e in self.procs.iter_mut() {
            if let Some(child) = e.child.as_mut() {
                if let Ok(Some(st)) = child.try_wait() {
                    e.info.status = "exited".into();
                    e.info.exit_code = st.code();
                    e.child = None;
                    ended.push((e.info.id, st.code()));
                }
            }
        }
        for (id, code) in ended {
            let msg = match code {
                Some(0) => "── procesul s-a terminat normal (cod 0)".to_string(),
                Some(3) => "── procesul a CAZUT intentionat (cod 3: cadere simulata dupa efect, inainte de ACK)".to_string(),
                Some(c) => format!("── procesul s-a terminat cu codul {c}"),
                None => "── procesul a fost oprit de un semnal".to_string(),
            };
            self.push_line(id, msg);
        }
    }
}

/// Supervizorul, partajat intre firele serverului HTTP.
#[derive(Clone, Default)]
pub struct Supervisor {
    inner: Arc<Mutex<Inner>>,
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

impl Supervisor {
    pub fn new() -> Supervisor {
        Supervisor::default()
    }

    /// Exista un proces activ de tipul/numele dat?
    pub fn is_running(&self, kind: &str, name: Option<&str>) -> bool {
        let mut g = self.inner.lock().unwrap();
        g.refresh();
        g.procs.iter().any(|e| {
            e.child.is_some() && e.info.kind == kind && name.is_none() || name == Some(e.info.name.as_str())
        })
    }

    /// Exista vreun proces activ?
    pub fn any_running(&self) -> bool {
        let mut g = self.inner.lock().unwrap();
        g.refresh();
        g.procs.iter().any(|e| e.child.is_some())
    }

    /// Porneste procesul descris de `spec` in folderul laboratorului.
    pub fn start(&self, lab: &Lab, spec: Spec) -> Result<u64, String> {
        let kind = spec.kind.as_str().to_string();
        if spec.unique && self.is_running(&kind, Some(&spec.name)) {
            return Err(format!("{} \"{}\" ruleaza deja", kind, spec.name));
        }
        if spec.kind != crate::commands::Kind::Build && !spec.program.exists() {
            return Err("executabilele nu sunt compilate: apasa intai \"Compileaza\"".into());
        }

        let mut child = Command::new(&spec.program)
            .args(&spec.args)
            .current_dir(&lab.dir)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| {
                if spec.kind == crate::commands::Kind::Build {
                    format!("nu pot porni \"go\" ({e}). Este Go instalat si in PATH?")
                } else {
                    format!("nu pot porni {}: {e}", spec.program.display())
                }
            })?;

        let mut g = self.inner.lock().unwrap();
        g.next_id += 1;
        let id = g.next_id;
        let info = ProcInfo {
            id,
            kind,
            name: spec.name.clone(),
            lab: lab.id.clone(),
            cmdline: spec.command_line(),
            started_ms: now_ms(),
            status: "running".into(),
            exit_code: None,
            oneshot: spec.oneshot,
        };
        let header = format!("── pornit: {}", info.cmdline);
        g.push_line(id, header);
        if let Some(out) = child.stdout.take() {
            self.pump(id, out);
        }
        if let Some(err) = child.stderr.take() {
            self.pump(id, err);
        }
        g.procs.push(Entry {
            info,
            child: Some(child),
        });
        Ok(id)
    }

    /// Citeste iesirea procesului intr-un fir separat.
    fn pump<R: Read + Send + 'static>(&self, id: u64, r: R) {
        let inner = self.inner.clone();
        thread::spawn(move || {
            let mut reader = BufReader::new(r);
            let mut buf = Vec::new();
            loop {
                buf.clear();
                match reader.read_until(b'\n', &mut buf) {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {
                        let text = String::from_utf8_lossy(&buf)
                            .trim_end_matches(['\r', '\n'])
                            .to_string();
                        inner.lock().unwrap().push_line(id, text);
                    }
                }
            }
        });
    }

    /// Opreste brutal un proces (echivalentul unei caderi / kill -9).
    pub fn stop(&self, id: u64) -> Result<(), String> {
        let mut g = self.inner.lock().unwrap();
        g.refresh();
        let entry = g
            .procs
            .iter_mut()
            .find(|e| e.info.id == id)
            .ok_or("proces inexistent")?;
        let mut child = entry.child.take().ok_or("procesul nu mai ruleaza")?;
        let _ = child.kill();
        let _ = child.wait();
        entry.info.status = "stopped".into();
        entry.info.exit_code = None;
        g.push_line(id, "── oprit din panou (kill)".into());
        Ok(())
    }

    /// Opreste toate procesele active.
    pub fn stop_all(&self) {
        let ids: Vec<u64> = {
            let mut g = self.inner.lock().unwrap();
            g.refresh();
            g.procs
                .iter()
                .filter(|e| e.child.is_some())
                .map(|e| e.info.id)
                .collect()
        };
        for id in ids {
            let _ = self.stop(id);
        }
    }

    /// Sterge din lista procesele terminate (si liniile lor).
    pub fn clear_finished(&self) {
        let mut g = self.inner.lock().unwrap();
        g.refresh();
        let alive: Vec<u64> = g
            .procs
            .iter()
            .filter(|e| e.child.is_some())
            .map(|e| e.info.id)
            .collect();
        g.procs.retain(|e| e.child.is_some());
        g.lines.retain(|l| alive.contains(&l.pid));
    }

    /// Starea pentru interfata: toate procesele + liniile noi (seq > since).
    pub fn snapshot(&self, since: u64) -> (Vec<ProcInfo>, Vec<Line>, u64) {
        let mut g = self.inner.lock().unwrap();
        g.refresh();
        let procs = g.procs.iter().map(|e| e.info.clone()).collect();
        let lines: Vec<Line> = g
            .lines
            .iter()
            .filter(|l| l.seq > since)
            .take(2000)
            .cloned()
            .collect();
        let next = lines.last().map(|l| l.seq).unwrap_or(since);
        (procs, lines, next)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::Kind;
    use std::path::PathBuf;
    use std::time::Duration;

    fn shell_spec(script: &str) -> (Lab, Spec) {
        let lab = Lab {
            id: "lab1".into(),
            title: String::new(),
            dir: std::env::temp_dir(),
        };
        let (prog, args) = if cfg!(windows) {
            (
                PathBuf::from("cmd"),
                vec!["/C".to_string(), script.to_string()],
            )
        } else {
            (
                PathBuf::from("/bin/sh"),
                vec!["-c".to_string(), script.to_string()],
            )
        };
        // Build ocoleste verificarea existentei executabilului din bin/
        (
            lab,
            Spec {
                kind: Kind::Build,
                name: "test".into(),
                program: prog,
                args,
                unique: false,
                oneshot: true,
            },
        )
    }

    fn wait_status(s: &Supervisor, id: u64, want: &str) -> ProcInfo {
        for _ in 0..100 {
            let (procs, _, _) = s.snapshot(0);
            let p = procs.into_iter().find(|p| p.id == id).unwrap();
            if p.status == want {
                return p;
            }
            thread::sleep(Duration::from_millis(30));
        }
        panic!("procesul {id} nu a ajuns in starea {want}");
    }

    #[test]
    fn captures_output_and_exit_code() {
        let s = Supervisor::new();
        let (lab, spec) = shell_spec("echo salut && echo eroare 1>&2 && exit 3");
        let id = s.start(&lab, spec).unwrap();
        let p = wait_status(&s, id, "exited");
        assert_eq!(p.exit_code, Some(3));
        thread::sleep(Duration::from_millis(50));
        let (_, lines, next) = s.snapshot(0);
        let texts: Vec<&str> = lines.iter().map(|l| l.text.as_str()).collect();
        assert!(
            texts.contains(&"salut") && texts.contains(&"eroare"),
            "{texts:?}"
        );
        assert!(texts.iter().any(|t| t.contains("CAZUT")), "{texts:?}");
        let (_, newer, _) = s.snapshot(next);
        assert!(newer.is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn stop_kills_running_process() {
        let s = Supervisor::new();
        let (lab, spec) = shell_spec("sleep 30");
        let id = s.start(&lab, spec).unwrap();
        assert!(s.any_running());
        s.stop(id).unwrap();
        assert!(!s.any_running());
        assert!(s.stop(id).is_err());
        s.clear_finished();
        assert!(s.snapshot(0).0.is_empty());
    }

    #[test]
    fn unique_processes() {
        let s = Supervisor::new();
        let (lab, mut spec) = shell_spec(if cfg!(windows) {
            "ping -n 3 127.0.0.1 >NUL"
        } else {
            "sleep 2"
        });
        spec.unique = true;
        s.start(&lab, spec.clone()).unwrap();
        assert!(s.start(&lab, spec).unwrap_err().contains("ruleaza deja"));
        s.stop_all();
    }
}
