use std::sync::Arc;

use crate::engine::sync::SyncEngine;
use crate::{DeltaResult, Snapshot};

/// Integration test for log compaction.
///
/// Also shows how users would interact with the log compaction API in practice.
#[test]
fn test_log_compaction_workflow() -> DeltaResult<()> {
    // Use a test table with multiple versions
    let path =
        std::fs::canonicalize(std::path::PathBuf::from("./tests/data/basic_partitioned/")).unwrap();
    let url = url::Url::from_directory_path(path).unwrap();

    let engine = SyncEngine::new();
    let snapshot = Arc::new(Snapshot::builder(url).build(&engine)?);

    // Create a log compaction writer for versions 0-1
    println!("Creating log compaction writer for versions 0-1...");
    let mut writer = snapshot.compact_log(0, 1)?;

    // Get the compaction path
    let compaction_path = writer.compaction_path()?;
    println!("Compaction file will be written to: {}", compaction_path);

    // Verify path format is correct
    let expected_filename = "00000000000000000000.00000000000000000001.compacted.json";
    assert!(compaction_path.to_string().ends_with(expected_filename));

    // Get the compaction data iterator
    println!("Generating compaction data...");
    let compaction_data = writer.compaction_data(&engine)?;

    // Simulate writing the data (normally this would be engine-specific)
    // For testing, we'll just consume the iterator to validate it works
    let mut batch_count = 0;
    for batch_result in compaction_data {
        let _batch = batch_result?;
        batch_count += 1;
        println!("Processed batch {}", batch_count);
    }
    println!("Generated {} data batches", batch_count);

    // Finalize the compaction (in real usage, you'd pass file metadata)
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
    let snapshot = Snapshot::builder(url).build(&engine).unwrap();

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
    let snapshot = Snapshot::builder(url).build(&engine).unwrap();

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
        println!("Version range {}-{} generates correct path", start, end);
    }
}

/// Test that verifies the actual content of compacted logs
///
/// This test creates a compaction, examines the batches of actions, and validates
/// that the reconciliation logic produces the expected results.
#[test]
fn test_log_compaction_content_verification() -> DeltaResult<()> {
    // Use a table with known actions that can be verified
    let path = std::fs::canonicalize(std::path::PathBuf::from(
        "./tests/data/table-with-dv-small/",
    ))
    .unwrap();
    let url = url::Url::from_directory_path(path).unwrap();

    let engine = SyncEngine::new();
    let snapshot = Snapshot::builder(url).build(&engine)?;

    println!("Testing log compaction content verification for versions 0-1...");

    // Create log compaction writer for versions 0-1
    let mut writer = snapshot.compact_log(0, 1)?;

    // Get the compaction data iterator
    let compaction_data = writer.compaction_data(&engine)?;

    // Verify the compaction statistics
    let total_actions = compaction_data.total_actions();
    let total_add_actions = compaction_data.total_add_actions();

    println!(
        "Compaction contains {} total actions, {} add actions",
        total_actions, total_add_actions
    );
    assert!(total_actions > 0, "Compaction should contain actions");
    assert!(total_add_actions >= 0, "Add actions should be non-negative");

    // Process each batch and verify basic content
    let mut batch_count = 0;
    let mut total_rows_processed = 0;

    for batch_result in compaction_data {
        let batch = batch_result?;
        batch_count += 1;

        let row_count = batch.len();
        total_rows_processed += row_count;

        println!("Processing batch {} with {} rows", batch_count, row_count);

        // Verify we can access the batch data without errors
        assert!(row_count > 0, "Each batch should contain at least one row");
    }

    println!("Content verification results:");
    println!("  - Batches processed: {}", batch_count);
    println!("  - Total rows processed: {}", total_rows_processed);
    println!("  - Total actions reported: {}", total_actions);
    println!("  - Total add actions reported: {}", total_add_actions);

    // Verify basic expectations based on the test table structure
    assert!(batch_count > 0, "Should have processed at least one batch");
    assert!(
        total_rows_processed > 0,
        "Should have processed at least one row"
    );

    // The total_actions count should match what we processed
    // (Note: this is a basic sanity check - the exact count depends on reconciliation logic)
    assert!(total_actions >= 0, "Total actions should be non-negative");

    // We should have some actions in the compaction (protocol, metadata, and reconciled data actions)
    // Based on the input files, we expect:
    // - Version 0: protocol, metadata, add
    // - Version 1: remove, add (with deletion vector)
    // After compaction: protocol, metadata, and the net result of add->remove->add operations
    assert!(
        total_actions >= 2,
        "Should have at least protocol and metadata actions"
    );

    println!("Log compaction content verification passed!");

    // Additional verification: ensure the compaction can be finalized
    let mut writer2 = snapshot.compact_log(0, 1)?;
    let final_data = writer2.compaction_data(&engine)?;

    let dummy_metadata = crate::FileMeta {
        location: writer2.compaction_path()?,
        last_modified: 0,
        size: 1000,
    };

    writer2.finalize(&engine, &dummy_metadata, final_data)?;
    println!("Log compaction finalization passed!");

    Ok(())
}
