//! Panoul de control PAD (Rust): o interfata web locala pentru laboratoarele
//! 1 si 2. Programul porneste un server HTTP pe localhost, afiseaza linkul si
//! deschide browserul. Din pagina se pornesc/opresc brokerul, serviciul de
//! conturi, producerii si consumerii, se injecteaza defecte si se urmaresc
//! cozile, soldurile si jurnalele in timp real.
//!
//!     cargo run --release                  # http://localhost:8080/
//!     cargo run --release -- --port 9000
//!     cargo run --release -- --no-browser

mod app;
mod commands;
mod http;
mod json;
mod procs;
mod proxy;

use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

struct Args {
    port: u16,
    root: Option<PathBuf>,
    open_browser: bool,
}

fn parse_args() -> Args {
    let mut a = Args {
        port: 8080,
        root: None,
        open_browser: true,
    };
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--port" => a.port = it.next().and_then(|v| v.parse().ok()).unwrap_or(8080),
            "--root" => a.root = it.next().map(PathBuf::from),
            "--no-browser" => a.open_browser = false,
            "-h" | "--help" => {
                println!(
                    "pad-panou [--port 8080] [--root <folderul repository-ului>] [--no-browser]"
                );
                std::process::exit(0);
            }
            other => eprintln!("argument necunoscut ignorat: {other}"),
        }
    }
    a
}

/// Gaseste radacina repository-ului (folderul care contine laborator_1/2).
fn find_root(explicit: Option<PathBuf>) -> Option<PathBuf> {
    let has_lab = |p: &Path| {
        p.join("laborator_1").join("go.mod").exists()
            || p.join("laborator_2").join("go.mod").exists()
    };
    let mut candidates = Vec::new();
    if let Some(r) = explicit {
        candidates.push(r);
    }
    if let Ok(cwd) = std::env::current_dir() {
        candidates.push(cwd.clone());
        if let Some(p) = cwd.parent() {
            candidates.push(p.to_path_buf());
        }
    }
    if let Some(p) = Path::new(env!("CARGO_MANIFEST_DIR")).parent() {
        candidates.push(p.to_path_buf());
    }
    candidates
        .into_iter()
        .find(|c| has_lab(c))
        .map(|c| c.canonicalize().unwrap_or(c))
}

fn go_version() -> Option<String> {
    let out = Command::new("go")
        .arg("version")
        .stdin(Stdio::null())
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

fn open_browser(url: &str) {
    let mut cmd = if cfg!(windows) {
        let mut c = Command::new("cmd");
        c.args(["/C", "start", "", url]);
        c
    } else if cfg!(target_os = "macos") {
        let mut c = Command::new("open");
        c.arg(url);
        c
    } else {
        let mut c = Command::new("xdg-open");
        c.arg(url);
        c
    };
    let _ = cmd
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn();
}

fn main() {
    let args = parse_args();
    let Some(root) = find_root(args.root) else {
        eprintln!("Nu gasesc folderele laborator_1 / laborator_2.");
        eprintln!("Ruleaza programul din folderul interfata_rust al repository-ului sau foloseste --root <cale>.");
        std::process::exit(1);
    };
    let labs = commands::detect_labs(&root);
    let go = go_version();

    // daca portul e ocupat, incercam urmatoarele
    let mut bound = None;
    for port in args.port..args.port.saturating_add(20) {
        if let Ok(l) = TcpListener::bind(("127.0.0.1", port)) {
            bound = Some((l, port));
            break;
        }
    }
    let Some((listener, port)) = bound else {
        eprintln!("Nu pot deschide niciun port incepand cu {}", args.port);
        std::process::exit(1);
    };

    let url = format!("http://localhost:{port}/");
    println!();
    println!("  ╔══════════════════════════════════════════════════════════╗");
    println!("  ║  Panoul de control PAD (Rust) ruleaza                    ║");
    println!("  ║                                                          ║");
    println!("  ║  Deschide in browser:   {:<33}║", url);
    println!("  ║                                                          ║");
    println!("  ║  Oprire: Ctrl+C (opreste si procesele pornite din panou) ║");
    println!("  ╚══════════════════════════════════════════════════════════╝");
    println!();
    println!("  Repository: {}", root.display());
    for l in &labs {
        println!(
            "  {}  -> {}{}",
            l.title,
            l.dir.display(),
            if l.built() { "" } else { "  (necompilat)" }
        );
    }
    match &go {
        Some(v) => println!("  Go: {v}"),
        None => println!("  ATENTIE: comanda \"go\" nu a fost gasita. Instaleaza Go 1.22+ si redeschide terminalul."),
    }
    println!();

    if args.open_browser {
        open_browser(&url);
    }

    let app = Arc::new(app::App::new(root, labs, go));
    for stream in listener.incoming() {
        let Ok(mut stream) = stream else { continue };
        let app = app.clone();
        thread::spawn(move || {
            stream.set_read_timeout(Some(Duration::from_secs(15))).ok();
            let resp = match http::read_request(&mut stream) {
                Ok(req) => app.handle(&req),
                Err(_) => return,
            };
            let _ = http::write_response(&mut stream, &resp);
        });
    }
}
