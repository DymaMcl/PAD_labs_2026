//! Rutele API ale panoului.

use crate::commands::{build_spec, valid_name, Kind, Lab};
use crate::http::{Request, Response};
use crate::json;
use crate::procs::Supervisor;
use crate::proxy;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Duration;

const INDEX_HTML: &str = include_str!("../ui/index.html");
const APP_JS: &str = include_str!("../ui/app.js");
const STYLE_CSS: &str = include_str!("../ui/style.css");

pub const BROKER_PORT: u16 = 7070;
pub const ACCOUNTS_ADMIN_PORT: u16 = 7081;

/// Starea aplicatiei.
pub struct App {
    pub root: PathBuf,
    pub labs: Vec<Lab>,
    pub active: Mutex<String>,
    pub sup: Supervisor,
    pub go_version: Option<String>,
}

impl App {
    pub fn new(root: PathBuf, labs: Vec<Lab>, go_version: Option<String>) -> App {
        // implicit: cel mai nou laborator disponibil
        let active = labs.last().map(|l| l.id.clone()).unwrap_or_default();
        App {
            root,
            labs,
            active: Mutex::new(active),
            sup: Supervisor::new(),
            go_version,
        }
    }

    fn active_lab(&self) -> Option<Lab> {
        let id = self.active.lock().unwrap().clone();
        self.labs.iter().find(|l| l.id == id).cloned()
    }

    /// Trateaza o cerere si intoarce raspunsul.
    pub fn handle(&self, req: &Request) -> Response {
        let m = req.method.as_str();
        let p = req.path.as_str();
        match (m, p) {
            ("GET", "/") | ("GET", "/index.html") => {
                Response::static_file("text/html; charset=utf-8", INDEX_HTML)
            }
            ("GET", "/app.js") => {
                Response::static_file("application/javascript; charset=utf-8", APP_JS)
            }
            ("GET", "/style.css") => Response::static_file("text/css; charset=utf-8", STYLE_CSS),
            ("GET", "/favicon.ico") => Response {
                status: 204,
                content_type: "text/plain".into(),
                body: vec![],
            },
            ("GET", "/api/info") => self.info(),
            ("POST", "/api/lab") => self.select_lab(req),
            ("POST", "/api/start") => self.start(req),
            ("POST", "/api/stop") => self.stop(req),
            ("POST", "/api/stopall") => {
                self.sup.stop_all();
                Response::ok_json(r#"{"ok":true}"#.into())
            }
            ("POST", "/api/clear") => {
                self.sup.clear_finished();
                Response::ok_json(r#"{"ok":true}"#.into())
            }
            ("POST", "/api/reset-data") => self.reset_data(),
            ("GET", "/api/state") => self.state(req),
            ("GET", "/api/ledger") => self.ledger(req),
            _ if p.starts_with("/api/broker/") => self.proxy(req, BROKER_PORT, "/api/broker"),
            _ if p.starts_with("/api/accounts/") => {
                self.proxy(req, ACCOUNTS_ADMIN_PORT, "/api/accounts")
            }
            ("GET", _) => Response::error(404, "pagina inexistenta"),
            _ => Response::error(405, "metoda nepermisa"),
        }
    }

    fn info(&self) -> Response {
        let labs: Vec<String> = self
            .labs
            .iter()
            .map(|l| {
                json::obj(&[
                    ("id", json::str(&l.id)),
                    ("title", json::str(&l.title)),
                    ("dir", json::str(&l.dir.display().to_string())),
                    ("built", l.built().to_string()),
                ])
            })
            .collect();
        let active = self.active.lock().unwrap().clone();
        Response::ok_json(json::obj(&[
            ("labs", json::arr(&labs)),
            ("active", json::str(&active)),
            ("root", json::str(&self.root.display().to_string())),
            ("go", json::opt_str(self.go_version.as_deref())),
            ("os", json::str(std::env::consts::OS)),
        ]))
    }

    fn select_lab(&self, req: &Request) -> Response {
        let id = req.query.get("id").unwrap_or("");
        if !self.labs.iter().any(|l| l.id == id) {
            return Response::error(400, "laborator inexistent in acest branch");
        }
        let mut active = self.active.lock().unwrap();
        if *active != id {
            // brokerul si serviciul folosesc aceleasi porturi in ambele laboratoare
            self.sup.stop_all();
            *active = id.to_string();
        }
        Response::ok_json(r#"{"ok":true}"#.into())
    }

    fn start(&self, req: &Request) -> Response {
        let Some(lab) = self.active_lab() else {
            return Response::error(400, "niciun laborator activ");
        };
        let Some(kind) = req.query.get("kind").and_then(Kind::parse) else {
            return Response::error(400, "tip de proces necunoscut");
        };
        let spec = match build_spec(&lab, kind, &req.query) {
            Ok(s) => s,
            Err(e) => return Response::error(400, &e),
        };
        if kind != Kind::Build && self.sup.is_running("build", None) {
            return Response::error(409, "compilarea este in curs, asteapta sa se termine");
        }
        match self.sup.start(&lab, spec) {
            Ok(id) => {
                Response::ok_json(json::obj(&[("ok", "true".into()), ("id", id.to_string())]))
            }
            Err(e) => Response::error(409, &e),
        }
    }

    fn stop(&self, req: &Request) -> Response {
        let id: u64 = req
            .query
            .get("id")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        match self.sup.stop(id) {
            Ok(()) => Response::ok_json(r#"{"ok":true}"#.into()),
            Err(e) => Response::error(409, &e),
        }
    }

    fn reset_data(&self) -> Response {
        let Some(lab) = self.active_lab() else {
            return Response::error(400, "niciun laborator activ");
        };
        if self.sup.any_running() {
            return Response::error(409, "opreste intai toate procesele");
        }
        let data = lab.dir.join("data");
        if data.exists() {
            if let Err(e) = std::fs::remove_dir_all(&data) {
                return Response::error(500, &format!("nu pot sterge {}: {e}", data.display()));
            }
        }
        self.sup.clear_finished();
        Response::ok_json(r#"{"ok":true}"#.into())
    }

    fn state(&self, req: &Request) -> Response {
        let since: u64 = req
            .query
            .get("since")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        let (procs, lines, next) = self.sup.snapshot(since);
        let procs: Vec<String> = procs.iter().map(|p| p.to_json()).collect();
        let lines: Vec<String> = lines
            .iter()
            .map(|l| {
                json::obj(&[
                    ("seq", l.seq.to_string()),
                    ("pid", l.pid.to_string()),
                    ("text", json::str(&l.text)),
                ])
            })
            .collect();
        let built = self.active_lab().map(|l| l.built()).unwrap_or(false);
        Response::ok_json(json::obj(&[
            ("active", json::str(&self.active.lock().unwrap())),
            ("built", built.to_string()),
            ("procs", json::arr(&procs)),
            ("lines", json::arr(&lines)),
            ("next", next.to_string()),
        ]))
    }

    /// Registrul local al unui consumer (laboratorul 1 / efect local).
    fn ledger(&self, req: &Request) -> Response {
        let name = req.query.get("name").unwrap_or("");
        if !valid_name(name) {
            return Response::error(400, "nume invalid");
        }
        let Some(lab) = self.active_lab() else {
            return Response::error(400, "niciun laborator activ");
        };
        let path = lab.dir.join("data").join(format!("{name}.ledger.json"));
        match std::fs::read(&path) {
            Ok(body) => Response {
                status: 200,
                content_type: "application/json; charset=utf-8".into(),
                body,
            },
            Err(_) => Response::error(404, "registrul nu exista inca"),
        }
    }

    fn proxy(&self, req: &Request, port: u16, prefix: &str) -> Response {
        let target = &req.target[prefix.len()..];
        // long-poll-ul brokerului poate dura pana la 30s
        match proxy::forward(
            port,
            &req.method,
            target,
            &req.body,
            Duration::from_secs(35),
        ) {
            Ok(up) => Response {
                status: up.status,
                content_type: up.content_type,
                body: up.body,
            },
            Err(e) => Response::error(502, &e),
        }
    }
}
