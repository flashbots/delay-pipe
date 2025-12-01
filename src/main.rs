use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use std::{
    collections::VecDeque,
    env,
    io::{BufRead, BufReader, Seek, SeekFrom},
    os::unix::fs::OpenOptionsExt,
    path::PathBuf,
};
use tokio::{fs::OpenOptions, io::AsyncWriteExt, sync::mpsc, time::Instant};

const MAX_BUF_BYTES: usize = 1 << 30; // 1 GiB

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 4 {
        eprintln!("Usage: {} <source> <dest> <delay_secs>", args[0]);
        std::process::exit(1);
    }

    let src = PathBuf::from(&args[1]);
    let dst = PathBuf::from(&args[2]);
    let delay = std::time::Duration::from_secs(args[3].parse()?);

    let src_file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW) // Prevent attacks like logrotten
        .open(&src)?;

    // RX channel for handling source log changes
    let (tx, mut rx) = mpsc::channel::<Event>(1024);
    let mut watcher = RecommendedWatcher::new(
        move |res: Result<Event, _>| {
            if let Ok(e) = res {
                let _ = tx.blocking_send(e);
            }
        },
        notify::Config::default(),
    )?;
    watcher.watch(&src, RecursiveMode::NonRecursive)?;

    let mut buf: VecDeque<(Instant, String)> = VecDeque::new();
    let mut buf_bytes = 0usize;
    let mut rdr = BufReader::new(src_file);
    rdr.seek(SeekFrom::End(0))?;

    loop {
        let next_release = buf.front().map(|(t, _)| *t);

        tokio::select! {
            Some(event) = rx.recv() => {
                // File was changed
                if matches!(event.kind, EventKind::Modify(_)) {
                    let mut line = String::new();
                    while rdr.read_line(&mut line)? > 0 {
                        if buf_bytes + line.len() <= MAX_BUF_BYTES {
                            buf_bytes += line.len();
                            buf.push_back((Instant::now() + delay, std::mem::take(&mut line)));
                        } else {
                            line.clear();
                        }
                    }
                }
            }
            _ = async {
                match next_release {
                    Some(t) => tokio::time::sleep_until(t).await,
                    None => std::future::pending::<()>().await,
                }
            } => {
                // Delay completed for buffered entries
                let now = Instant::now();
                let mut out = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&dst)
                    .await?;
                while buf.front().is_some_and(|(t, _)| *t <= now) {
                    let (_, s) = buf.pop_front().unwrap();
                    buf_bytes -= s.len();
                    out.write_all(s.as_bytes()).await?;
                }
                out.sync_data().await?;
            }
        }
    }
}
