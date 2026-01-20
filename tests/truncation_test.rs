use std::fs::{File, OpenOptions};
use std::io::Write;
use std::process::{Command, Child};
use std::thread;
use std::time::Duration;
use tempfile::TempDir;

fn start(src: &std::path::Path, dst: &std::path::Path, delay_secs: u64) -> Child {
    Command::new(env!("CARGO_BIN_EXE_delay-pipe"))
        .args([src.to_str().unwrap(), dst.to_str().unwrap(), &delay_secs.to_string()])
        .spawn().unwrap()
}

#[test]
fn test_truncation_handling() {
    let dir = TempDir::new().unwrap();
    let src = dir.path().join("source.log");
    let dst = dir.path().join("dest.log");
    
    // Create initial file with content
    let mut file = File::create(&src).unwrap();
    for i in 0..10 {
        writeln!(file, "Initial line {}", i).unwrap();
    }
    file.sync_all().unwrap();
    drop(file);
    
    // Start delay-pipe with 1 second delay
    let mut child = start(&src, &dst, 1);
    thread::sleep(Duration::from_millis(500));
    
    // Append more lines
    let mut file = OpenOptions::new().append(true).open(&src).unwrap();
    writeln!(file, "Before truncate").unwrap();
    file.sync_all().unwrap();
    drop(file);
    
    thread::sleep(Duration::from_millis(500));
    
    // Simulate copytruncate: copy the file then truncate
    std::fs::copy(&src, src.with_extension("1")).unwrap();
    File::create(&src).unwrap(); // Truncates the file
    
    // Write new content after truncation
    let mut file = OpenOptions::new().append(true).open(&src).unwrap();
    writeln!(file, "After truncate line 1").unwrap();
    writeln!(file, "After truncate line 2").unwrap();
    file.sync_all().unwrap();
    drop(file);
    
    // Wait for delay to pass
    thread::sleep(Duration::from_secs(2));
    
    // Check that the post-truncation lines appear in destination
    let content = std::fs::read_to_string(&dst).unwrap();
    assert!(content.contains("After truncate line 1"), 
            "Destination should contain post-truncation content, but got: {}", content);
    assert!(content.contains("After truncate line 2"), 
            "Destination should contain post-truncation content, but got: {}", content);
    
    child.kill().unwrap();
}