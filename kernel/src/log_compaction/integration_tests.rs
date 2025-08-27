use std::sync::Arc;

use crate::engine::sync::SyncEngine;
use crate::{DeltaResult, Snapshot};

/// Integration test demonstrating the complete log compaction workflow
///
/// This test shows how users would interact with the log compaction API in practice.
#[test]
fn test_log_compaction_workflow() -> DeltaResult<()> {
    // Use a test table with multiple versions
    let path =
        std::fs::canonicalize(std::path::PathBuf::from("./tests/data/basic_partitioned/")).unwrap();
    let url = url::Url::from_directory_path(path).unwrap();

    let engine = SyncEngine::new();
    let snapshot = Arc::new(Snapshot::try_new(url, &engine, None)?);

    // Step 1: Create a log compaction writer for versions 0-1
    println!("Creating log compaction writer for versions 0-1...");
    let mut writer = snapshot.compact_log(0, 1)?;

    // Step 2: Get the compaction path
    let compaction_path = writer.compaction_path()?;
    println!("Compaction file will be written to: {}", compaction_path);

    // Verify path format is correct
    let expected_filename = "00000000000000000000.00000000000000000001.compacted.json";
    assert!(compaction_path.to_string().ends_with(expected_filename));

    // Step 3: Get the compaction data iterator
    println!("Generating compaction data...");
    let compaction_data = writer.compaction_data(&engine)?;

    // Step 4: Simulate writing the data (normally this would be engine-specific)
    // For testing, we'll just consume the iterator to validate it works
    let mut batch_count = 0;
    for batch_result in compaction_data {
        let _batch = batch_result?;
        batch_count += 1;
        println!("Processed batch {}", batch_count);
    }
    println!("Generated {} data batches", batch_count);

    // Step 5: Finalize the compaction (in real usage, you'd pass file metadata)
    // For this test, we'll create dummy metadata
    let dummy_metadata = crate::FileMeta {
        location: compaction_path,
        last_modified: 0,
        size: 1000, // dummy size
    };

    // Create a new iterator for finalization (since the previous one was consumed)
    let final_data = writer.compaction_data(&engine)?;

    println!("Finalizing compaction...");
    writer.finalize(&engine, &dummy_metadata, final_data)?;

    println!("Log compaction workflow completed successfully!");
    Ok(())
}

/// Test compaction with invalid parameters
#[test]
fn test_invalid_compaction_parameters() {
    let path = std::fs::canonicalize(std::path::PathBuf::from(
        "./tests/data/table-with-dv-small/",
    ))
    .unwrap();
    let url = url::Url::from_directory_path(path).unwrap();

    let engine = SyncEngine::new();
    let snapshot = Snapshot::try_new(url, &engine, None).unwrap();

    // Test invalid version range (start > end)
    let result = snapshot.compact_log(5, 2);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Invalid version range"));

    // Test equal versions (should work)
    let result = snapshot.compact_log(1, 1);
    assert!(result.is_ok());
}

/// Test compaction path generation
#[test]
fn test_compaction_path_generation() {
    let path = std::fs::canonicalize(std::path::PathBuf::from(
        "./tests/data/table-with-dv-small/",
    ))
    .unwrap();
    let url = url::Url::from_directory_path(path).unwrap();

    let engine = SyncEngine::new();
    let snapshot = Snapshot::try_new(url, &engine, None).unwrap();

    // Test various version ranges
    let test_cases = vec![
        (
            0,
            0,
            "00000000000000000000.00000000000000000000.compacted.json",
        ),
        (
            0,
            1,
            "00000000000000000000.00000000000000000001.compacted.json",
        ),
        (
            10,
            20,
            "00000000000000000010.00000000000000000020.compacted.json",
        ),
        (
            100,
            999,
            "00000000000000000100.00000000000000000999.compacted.json",
        ),
    ];

    for (start, end, expected_filename) in test_cases {
        let writer = snapshot.compact_log(start, end).unwrap();
        let path = writer.compaction_path().unwrap();
        assert!(
            path.to_string().ends_with(expected_filename),
            "Path {} does not end with {}",
            path,
            expected_filename
        );
        println!("✓ Version range {}-{} generates correct path", start, end);
    }
}
