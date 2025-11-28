# delay-pipe

Delays lines written to a source file before appending them to a destination file.

## Usage

```
delay-pipe <source> <dest> <delay_secs>
```

## Example

```bash
touch /tmp/src
delay-pipe /tmp/src /tmp/dst 5 &
echo "hello" >> /tmp/src
sleep 5
cat /tmp/dst
```

## Build

```bash
cargo build --release
```

## Test

```bash
cargo test
cargo test stress -- --ignored --nocapture  # 100k lines/sec for 60s
```
