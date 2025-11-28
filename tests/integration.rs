use rand::Rng;
use std::{fs::{File, OpenOptions}, io::{BufRead, BufReader, Write}, process::{Child, Command}, thread, time::{Duration, Instant, SystemTime, UNIX_EPOCH}};
use tempfile::TempDir;

fn start(src: &std::path::Path, dst: &std::path::Path, delay_secs: u64) -> Child {
    Command::new(env!("CARGO_BIN_EXE_delay-pipe"))
        .args([src.to_str().unwrap(), dst.to_str().unwrap(), &delay_secs.to_string()])
        .spawn().unwrap()
}

fn setup() -> (TempDir, std::path::PathBuf, std::path::PathBuf) {
    let dir = TempDir::new().unwrap();
    let (src, dst) = (dir.path().join("src"), dir.path().join("dst"));
    File::create(&src).unwrap();
    (dir, src, dst)
}

fn now_us() -> u128 { SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() }

fn read_lines(path: &std::path::Path) -> Vec<String> {
    if !path.exists() { return vec![]; }
    BufReader::new(File::open(path).unwrap()).lines().map_while(Result::ok).collect()
}

fn wait_lines(path: &std::path::Path, n: usize, timeout: Duration) -> Vec<String> {
    let start = Instant::now();
    loop {
        let lines = read_lines(path);
        if lines.len() >= n || start.elapsed() >= timeout { return lines; }
        thread::sleep(Duration::from_millis(10));
    }
}

fn stop(child: &mut Child) { let _ = child.kill(); let _ = child.wait(); }

fn sleep(ms: u64) { thread::sleep(Duration::from_millis(ms)); }

#[test]
fn basic_delay() {
    let (_dir, src, dst) = setup();
    let mut child = start(&src, &dst, 1);
    sleep(100);

    let mut f = OpenOptions::new().append(true).open(&src).unwrap();
    let t = Instant::now();
    writeln!(f, "test").unwrap(); f.flush().unwrap();

    sleep(500);
    assert!(read_lines(&dst).is_empty());
    sleep(700);
    assert_eq!(read_lines(&dst).len(), 1);
    assert!(t.elapsed() >= Duration::from_secs(1));
    stop(&mut child);
}

#[test]
fn no_early_arrival() {
    let (_dir, src, dst) = setup();
    let delay_secs = 1u64;
    let mut child = start(&src, &dst, delay_secs);
    sleep(100);

    let mut f = OpenOptions::new().append(true).open(&src).unwrap();
    for i in 0..10 {
        writeln!(f, "{}|{}", i, now_us()).unwrap(); f.flush().unwrap();
        sleep(20);
    }

    let lines = wait_lines(&dst, 10, Duration::from_secs(3));
    let arrival = now_us();
    assert_eq!(lines.len(), 10);
    for line in &lines {
        let ts: u128 = line.split('|').nth(1).unwrap().parse().unwrap();
        assert!((arrival - ts) / 1_000_000 >= delay_secs as u128, "arrived early");
    }
    stop(&mut child);
}

#[test]
fn timing_under_load() {
    let (_dir, src, dst) = setup();
    let (delay_secs, tol_ms) = (1u64, 500u64);
    let mut child = start(&src, &dst, delay_secs);
    sleep(100);

    let mut f = OpenOptions::new().append(true).open(&src).unwrap();
    for i in 0..50 { writeln!(f, "{}|{}", i, now_us()).unwrap(); }
    f.flush().unwrap();

    let lines = wait_lines(&dst, 50, Duration::from_secs(3));
    let arrival = now_us();
    assert_eq!(lines.len(), 50);
    for line in &lines {
        let ts: u128 = line.split('|').nth(1).unwrap().parse().unwrap();
        let elapsed_ms = (arrival - ts) / 1000;
        let min_ms = delay_secs as u128 * 1000;
        let max_ms = min_ms + tol_ms as u128;
        assert!(elapsed_ms >= min_ms && elapsed_ms <= max_ms, "bad timing: {}ms", elapsed_ms);
    }
    stop(&mut child);
}

#[test]
fn sustained_load() {
    let (_dir, src, dst) = setup();
    let mut child = start(&src, &dst, 1);
    sleep(100);

    let mut f = OpenOptions::new().append(true).open(&src).unwrap();
    let mut rng = rand::rng();
    for i in 0..1000 {
        let pad: String = (0..rng.random_range(10..200)).map(|_| rng.random_range(b'a'..=b'z') as char).collect();
        writeln!(f, "{}|{}", i, pad).unwrap();
    }
    f.flush().unwrap();

    sleep(1500);
    assert_eq!(read_lines(&dst).len(), 1000);
    stop(&mut child);
}

#[test]
fn burst_traffic() {
    let (_dir, src, dst) = setup();
    let mut child = start(&src, &dst, 1);
    sleep(100);

    let mut f = OpenOptions::new().append(true).open(&src).unwrap();
    let mut rng = rand::rng();
    for _ in 0..3 {
        for i in 0..500 {
            let pad: String = (0..rng.random_range(10..150)).map(|_| rng.random_range(b'a'..=b'z') as char).collect();
            writeln!(f, "{}|{}", i, pad).unwrap();
        }
        f.flush().unwrap();
        sleep(20);
    }

    sleep(1500);
    assert_eq!(read_lines(&dst).len(), 1500);
    stop(&mut child);
}

#[test]
fn large_lines() {
    let (_dir, src, dst) = setup();
    let mut child = start(&src, &dst, 1);
    sleep(100);

    let mut f = OpenOptions::new().append(true).open(&src).unwrap();
    let mut rng = rand::rng();
    for i in 0..100 {
        let pad: String = (0..rng.random_range(1000..5000)).map(|_| rng.random_range(b'a'..=b'z') as char).collect();
        writeln!(f, "{}|{}", i, pad).unwrap();
    }
    f.flush().unwrap();

    sleep(1500);
    assert_eq!(read_lines(&dst).len(), 100);
    stop(&mut child);
}

#[test]
fn ordering_preserved() {
    let (_dir, src, dst) = setup();
    let mut child = start(&src, &dst, 1);
    sleep(100);

    let mut f = OpenOptions::new().append(true).open(&src).unwrap();
    for i in 0..500 { writeln!(f, "{}", i).unwrap(); }
    f.flush().unwrap();

    sleep(1500);
    let lines = read_lines(&dst);
    for (i, line) in lines.iter().enumerate() { assert_eq!(line, &i.to_string()); }
    stop(&mut child);
}

#[test]
#[ignore] // cargo test stress -- --ignored --nocapture
fn stress_20k_lps() {
    let (_dir, src, dst) = setup();
    let mut child = start(&src, &dst, 60);
    sleep(100);

    let mut f = OpenOptions::new().append(true).open(&src).unwrap();
    let t0 = Instant::now();
    let mut written = 0u64;

    while t0.elapsed() < Duration::from_secs(59) {
        let t = Instant::now();
        for _ in 0..200 { writeln!(f, "{}", written).unwrap(); written += 1; }
        f.flush().unwrap();
        if let Some(r) = Duration::from_millis(10).checked_sub(t.elapsed()) { thread::sleep(r); }
    }
    println!("Wrote {} in {:.1}s ({:.0}/s)", written, t0.elapsed().as_secs_f64(), written as f64 / t0.elapsed().as_secs_f64());

    let early = read_lines(&dst).len() as u64;
    println!("@{:.0}s: {}", t0.elapsed().as_secs_f64(), early);
    assert_eq!(early, 0, "lines arrived early");

    thread::sleep(Duration::from_secs(125).saturating_sub(t0.elapsed()));
    let read = read_lines(&dst).len() as u64;
    println!("@{:.0}s: {}", t0.elapsed().as_secs_f64(), read);
    assert_eq!(read, written, "lost {} lines", written - read);
    stop(&mut child);
}
