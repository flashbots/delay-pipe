use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Child};
use std::thread;
use std::time::Duration;
use tempfile::TempDir;

fn start(src: &std::path::Path, dst: &std::path::Path, delay_secs: u64) -> Child {
    Command::new(env!("CARGO_BIN_EXE_delay-pipe"))
        .args([src.to_str().unwrap(), dst.to_str().unwrap(), &delay_secs.to_string()])
        .spawn().unwrap()
}

fn setup() -> (TempDir, std::path::PathBuf, std::path::PathBuf) {
    let dir = TempDir::new().unwrap();
    let (src, dst) = (dir.path().join("src.log"), dir.path().join("dst.log"));
    File::create(&src).unwrap();
    (dir, src, dst)
}

fn read_lines(path: &std::path::Path) -> Vec<String> {
    if !path.exists() {
        return vec![];
    }
    BufReader::new(File::open(path).unwrap()).lines().map_while(Result::ok).collect()
}

fn sleep(ms: u64) {
    thread::sleep(Duration::from_millis(ms));
}

#[test]
fn handles_copytruncate() {
    let (_dir, src, dst) = setup();

    // Write initial content to get file to a reasonable size
    let mut f = OpenOptions::new().append(true).open(&src).unwrap();
    for i in 0..10 {
        writeln!(f, "Initial line {}", i).unwrap();
    }
    f.flush().unwrap();

    // Start delay-pipe with 1 second delay
    let mut child = start(&src, &dst, 1);
    sleep(200);

    // Write a marker line before truncation
    writeln!(f, "Before truncate").unwrap();
    f.flush().unwrap();
    drop(f);

    sleep(200);

    // Simulate logrotate copytruncate
    std::fs::copy(&src, src.with_extension("1")).unwrap();
    File::create(&src).unwrap(); // Truncates to 0

    // Write new content after truncation
    let mut f = OpenOptions::new().append(true).open(&src).unwrap();
    writeln!(f, "After truncate line 1").unwrap();
    writeln!(f, "After truncate line 2").unwrap();
    f.flush().unwrap();

    // Wait for delay to pass
    sleep(1500);

    // Verify post-truncation lines appear in output
    let lines = read_lines(&dst);
    assert!(lines.iter().any(|l| l.contains("After truncate line 1")), 
            "Missing post-truncation content. Got: {:?}", lines);
    assert!(lines.iter().any(|l| l.contains("After truncate line 2")), 
            "Missing post-truncation content. Got: {:?}", lines);

    child.kill().unwrap();
}
