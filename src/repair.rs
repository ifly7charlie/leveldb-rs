use crate::block::Block;
use crate::cache::Cache;
use crate::db_impl::{build_table, log_file_name};
use crate::error::Result;
use crate::key_types::parse_internal_key;
use crate::log::{LogReader, LogWriter};
use crate::memtable::MemTable;
use crate::options::Options;
use crate::table_cache::table_file_name;
use crate::table_reader::Table;
use crate::types::{parse_file_name, share, FileMetaData, FileNum, FileType, LdbIterator, Shared};
use crate::version_edit::VersionEdit;
use crate::version_set::{manifest_file_name, set_current_file};
use crate::write_batch::WriteBatch;

use std::path::Path;
use std::rc::Rc;

const HEADER_SIZE: usize = 12;

/// Repairs a corrupted database by rebuilding the MANIFEST from the table and
/// log files that exist on disk. All recovered tables are placed at level 0;
/// a subsequent compaction will organize them into the correct levels.
///
/// This matches the behavior of C++ LevelDB's `RepairDB()`.
pub fn repair_db<P: AsRef<Path>>(dbname: P, opt: Options) -> Result<()> {
    let dbname = dbname.as_ref();
    let mut next_file_num: FileNum = 1;
    let mut max_seq: u64 = 0;
    let mut tables: Vec<FileMetaData> = vec![];

    // 1. Scan directory for log and table files.
    let filenames = opt.env.children(dbname)?;
    let mut logs = vec![];
    let mut existing_tables = vec![];

    for f in &filenames {
        if let Ok((num, typ)) = parse_file_name(f) {
            if num >= next_file_num {
                next_file_num = num + 1;
            }
            match typ {
                FileType::Log => logs.push(num),
                FileType::Table => existing_tables.push(num),
                _ => {}
            }
        }
    }
    logs.sort();

    // 2. Replay log files into new tables.
    let cmp: Rc<Box<dyn crate::cmp::Cmp>> = opt.cmp.clone();
    for &log_num in &logs {
        let filename = log_file_name(dbname, log_num);
        let logfile = match opt.env.open_sequential_file(Path::new(&filename)) {
            Ok(f) => f,
            Err(e) => {
                log!(opt.log, "repair: skipping log {}: {}", log_num, e);
                continue;
            }
        };

        let mut reader = LogReader::new(logfile, true);
        let mut scratch = vec![];
        let mut mem = MemTable::new(cmp.clone());
        let mut batch = WriteBatch::new();

        while let Ok(len) = reader.read(&mut scratch) {
            if len == 0 {
                break;
            }
            if len < HEADER_SIZE {
                continue;
            }
            batch.set_contents(&scratch);
            let last_seq = batch.sequence() + batch.count() as u64 - 1;
            if last_seq > max_seq {
                max_seq = last_seq;
            }
            // Best-effort: skip corrupted batches.
            for (k, v) in batch.iter() {
                let seq = batch.sequence();
                match v {
                    Some(v_) => {
                        mem.add(seq, crate::key_types::ValueType::TypeValue, k, v_)
                    }
                    None => {
                        mem.add(seq, crate::key_types::ValueType::TypeDeletion, k, b"")
                    }
                }
            }
            batch.clear();
        }

        if mem.len() > 0 {
            let fnum = next_file_num;
            next_file_num += 1;
            match build_table(dbname, &opt, mem.iter(), fnum) {
                Ok(fmd) if fmd.size > 0 => {
                    log!(opt.log, "repair: recovered log {} -> table {}", log_num, fnum);
                    tables.push(fmd);
                }
                Ok(_) => {
                    // Empty table, clean up.
                    let _ = opt.env.delete(Path::new(&table_file_name(dbname, fnum)));
                }
                Err(e) => {
                    log!(opt.log, "repair: failed to write table from log {}: {}", log_num, e);
                }
            }
        }
    }

    // 3. Extract metadata from existing table files.
    let block_cache: Shared<Cache<Block>> = share(Cache::new(opt.max_open_files));
    for &table_num in &existing_tables {
        match extract_table_metadata(dbname, &opt, block_cache.clone(), table_num) {
            Ok((fmd, seq)) => {
                if seq > max_seq {
                    max_seq = seq;
                }
                tables.push(fmd);
            }
            Err(e) => {
                log!(
                    opt.log,
                    "repair: skipping table {} (could not read): {}",
                    table_num,
                    e
                );
            }
        }
    }

    // 4. Write new MANIFEST.
    let manifest_num = next_file_num;
    next_file_num += 1;

    let mut ve = VersionEdit::new();
    ve.set_comparator_name(opt.cmp.id());
    ve.set_log_num(0);
    ve.set_next_file(next_file_num);
    ve.set_last_seq(max_seq);
    for fmd in tables {
        ve.add_file(0, fmd);
    }

    let manifest = manifest_file_name(dbname, manifest_num);
    let manifest_file = opt.env.open_writable_file(Path::new(&manifest))?;
    let mut lw = LogWriter::new(manifest_file);
    lw.add_record(&ve.encode())?;
    lw.flush()?;

    // 5. Update CURRENT to point to the new MANIFEST.
    set_current_file(opt.env.as_ref().as_ref(), dbname, manifest_num)?;

    // 6. Delete recovered log files (their data is now in tables).
    for &log_num in &logs {
        let _ = opt.env.delete(Path::new(&log_file_name(dbname, log_num)));
    }

    Ok(())
}

fn extract_table_metadata<P: AsRef<Path>>(
    dbname: P,
    opt: &Options,
    block_cache: Shared<Cache<Block>>,
    file_num: FileNum,
) -> Result<(FileMetaData, u64)> {
    let path = table_file_name(dbname.as_ref(), file_num);
    let file_size = opt.env.size_of(Path::new(&path))?;
    let file = Rc::new(opt.env.open_random_access_file(Path::new(&path))?);
    let table = Table::new(opt.clone(), block_cache, file, file_size)?;

    let mut iter = table.iter();
    let mut smallest = None;
    let mut largest = None;
    let mut max_seq: u64 = 0;

    iter.reset();
    while iter.advance() {
        if let Some((key, _)) = iter.current() {
            if smallest.is_none() {
                smallest = Some(key.clone());
            }
            largest = Some(key.clone());

            if key.len() >= 8 {
                let (_, seq, _) = parse_internal_key(&key);
                if seq > max_seq {
                    max_seq = seq;
                }
            }
        }
    }

    match (smallest, largest) {
        (Some(s), Some(l)) => Ok((
            FileMetaData {
                num: file_num,
                size: file_size,
                smallest: s,
                largest: l,
                ..Default::default()
            },
            max_seq,
        )),
        _ => crate::error::err(crate::error::StatusCode::Corruption, "empty table"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db_impl::DB;
    use crate::mem_env::MemEnv;
    use crate::options;

    fn test_options() -> Options {
        let mut opt = options::for_test();
        opt.env = Rc::new(Box::new(MemEnv::new()));
        opt
    }

    #[test]
    fn test_repair_db_missing_files() {
        let opt = test_options();
        let dbname = "repair_test_missing";

        // Create DB and write some data, using compact_range to force table creation.
        {
            let mut db = DB::open(dbname, opt.clone()).unwrap();
            for i in 0..100 {
                db.put(format!("key{:04}", i).as_bytes(), b"value").unwrap();
            }
            db.compact_range(b"key0000", b"key0099").unwrap();
            // Write more data to create a second table.
            for i in 100..200 {
                db.put(format!("key{:04}", i).as_bytes(), b"value2").unwrap();
            }
            db.compact_range(b"key0100", b"key0199").unwrap();
        }

        // Find and delete one table file.
        let files = opt.env.children(Path::new(dbname)).unwrap();
        let mut deleted_table = None;
        for f in &files {
            if let Ok((num, FileType::Table)) = parse_file_name(f) {
                let _ = opt.env.delete(&Path::new(dbname).join(f));
                deleted_table = Some(num);
                break;
            }
        }
        assert!(deleted_table.is_some(), "should have found a table to delete");

        // Normal open should fail.
        assert!(DB::open(dbname, opt.clone()).is_err());

        // Repair should succeed.
        repair_db(dbname, opt.clone()).unwrap();

        // Should be able to open after repair.
        let mut db = DB::open(dbname, opt.clone()).unwrap();
        // At least some data should be readable (from the surviving table).
        let mut found = 0;
        for i in 0..200 {
            if db.get(format!("key{:04}", i).as_bytes()).is_some() {
                found += 1;
            }
        }
        assert!(found > 0, "should have recovered some data");
    }

    #[test]
    fn test_repair_db_corrupted_manifest() {
        let opt = test_options();
        let dbname = "repair_test_manifest";

        // Create DB and write data, forcing table creation.
        {
            let mut db = DB::open(dbname, opt.clone()).unwrap();
            for i in 0..50 {
                db.put(format!("key{:04}", i).as_bytes(), b"value").unwrap();
            }
            db.compact_range(b"key0000", b"key0049").unwrap();
        }

        // Delete MANIFEST files.
        let files = opt.env.children(Path::new(dbname)).unwrap();
        for f in &files {
            if let Ok((_, FileType::Descriptor)) = parse_file_name(f) {
                let _ = opt.env.delete(&Path::new(dbname).join(f));
            }
        }
        // Also delete CURRENT.
        let _ = opt.env.delete(&Path::new(dbname).join("CURRENT"));

        // Repair should succeed.
        repair_db(dbname, opt.clone()).unwrap();

        // Should be able to open and read data.
        let mut db = DB::open(dbname, opt.clone()).unwrap();
        let mut found = 0;
        for i in 0..50 {
            if db.get(format!("key{:04}", i).as_bytes()).is_some() {
                found += 1;
            }
        }
        assert_eq!(found, 50, "all data should be recovered from tables");
    }
}
