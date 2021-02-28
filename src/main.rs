use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};
use memmap::{Mmap, MmapMut, MmapOptions};
use std::thread;
use std::time::{Instant, Duration, SystemTime};
use std::thread::sleep;
use borrowed_byte_buffer::{ByteBuf, ByteBufMut};

const PAGE_SIZE: u64 = 4 * 1024;            // 4 Kb
const FILE_SIZE: u64 = PAGE_SIZE * 1024;    // 4 Mb
const FILE_PATH: &str = "target/test.bin";

#[derive(Default, Debug, Eq, PartialEq)]
struct KV {
    key: Vec<u8>,
    val: Vec<u8>,
}

impl KV {
    fn read(buf: &mut ByteBuf) -> Self {
        let klen = buf.get_u32().unwrap() as usize;
        let vlen = buf.get_u32().unwrap() as usize;
        KV {
            key: buf.get_bytes(klen).to_vec(),
            val: buf.get_bytes(vlen).to_vec(),
        }
    }

    fn write(&self, buf: &mut ByteBufMut) {
        buf.put_u32(self.key.len() as u32);
        buf.put_u32(self.val.len() as u32);
        buf.put_bytes(self.key.as_ref());
        buf.put_bytes(self.val.as_ref());
    }
}

fn millis() -> u64 {
    SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs() as u64
}

fn kv() -> KV {
    let millis = millis();
    let key = format!("key={}", millis).as_bytes().to_vec();
    let val = format!("val={}", millis).as_bytes().to_vec();
    KV { key, val }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kv() {
        let kv = kv();

        let mut buf = [0u8; 64];
        kv.write(&mut ByteBufMut::wrap(&mut buf));

        let r = KV::read(&mut ByteBuf::wrap(&buf));
        assert_eq!(r, kv);
    }
}


fn main() {
    const N: usize = 1000000;

    let before = Instant::now();
    make(FILE_PATH, ((14 + 4) * 2 * N) as u64);
    println!("file reset in {} ms", before.elapsed().as_millis());

    let now = Instant::now();

    let r_handle = thread::spawn(move || {
        let read = read(FILE_PATH);
        println!("r.len={}", read.len());
        sleep(Duration::from_millis(500));

        let mut buf = ByteBuf::wrap(&read[..]);
        let kvs = (0..N).map(|_| KV::read(&mut buf)).collect::<Vec<_>>();
        (kvs, buf.pos())
    });

    let w_handle = thread::spawn(move || {
        let mut open = open(FILE_PATH, FILE_SIZE);
        println!("w.len={}", open.len());

        let mut buf = ByteBufMut::wrap(&mut open[..]);
        let kvs = (0..N).map(|_| kv()).collect::<Vec<_>>();
        kvs.iter().for_each(|kv| kv.write(&mut buf));

        let pos = buf.pos();
        open.flush().unwrap();
        (kvs, pos)
    });

    let t = thread::spawn(move ||
        (w_handle.join().unwrap(), r_handle.join().unwrap())
    );
    let ((r, rp), (w, wp)) = t.join().unwrap();

    let ms = now.elapsed().as_millis() as usize;
    println!("time: {} ms", ms);
    println!("rn={} rp={} wn={} wp={}", r.len(), rp, w.len(), wp);
    let eq = r.into_iter()
        .zip(w.into_iter())
        .filter(|(a, b)| a == b)
        .count();
    println!("eq={}", eq);
}

fn make(path: &str, size: u64) {
    std::fs::remove_file(path).unwrap_or_default();

    let mut f = OpenOptions::new()
        .write(true)
        .create(true)
        .open(path)
        .expect("Unable to open file");

    f.seek(SeekFrom::Start(size-1)).unwrap();
    f.write_all(&[0]).unwrap();
    f.seek(SeekFrom::Start(0)).unwrap();
    f.sync_all().unwrap();
}

fn open(path: &str, _size: u64) -> MmapMut {
    let mut f = OpenOptions::new()
        .write(true)
        .create(true)
        .read(true)
        .open(path)
        .expect("Unable to open file");
    f.seek(SeekFrom::Start(0)).unwrap();

    unsafe {
        MmapOptions::new()
            .map_mut(&f)
            .expect("Could not access data from memory mapped file")
    }
}

fn read(path: &str) -> Mmap {
    let mut f = OpenOptions::new()
        .read(true)
        .open(path)
        .expect("Unable to open file");
    f.seek(SeekFrom::Start(0)).unwrap();

    unsafe {
        MmapOptions::new()
            .map(&f)
            .expect("Could not access data from memory mapped file")
    }
}
