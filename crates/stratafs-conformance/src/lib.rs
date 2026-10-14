pub mod content_cases;
pub mod metadata_cases;
pub mod reference;
mod support;

use std::time::Duration;

#[cfg(target_arch = "wasm32")]
#[doc(hidden)]
pub use wasm_bindgen_test;

pub const CASE_DEADLINE: Duration = Duration::from_secs(120);
pub const LONG_CASE_DEADLINE: Duration = Duration::from_secs(1800);

#[cfg(not(target_arch = "wasm32"))]
pub fn run<F: Future>(future: F) -> F::Output {
    pollster::block_on(future)
}

#[cfg(not(target_arch = "wasm32"))]
pub fn run_with_deadline(name: &str, deadline: Duration, case: impl FnOnce() + Send + 'static) {
    use std::sync::mpsc::{RecvTimeoutError, channel};

    let (finished, finished_receiver) = channel();
    let thread = std::thread::Builder::new()
        .name(name.to_owned())
        .spawn(move || {
            case();
            finished.send(()).ok();
        })
        .unwrap_or_else(|error| panic!("could not spawn a thread for case {name}: {error}"));
    match finished_receiver.recv_timeout(deadline) {
        Ok(()) => {
            if let Err(payload) = thread.join() {
                std::panic::resume_unwind(payload);
            }
        }
        Err(RecvTimeoutError::Disconnected) => match thread.join() {
            Ok(()) => panic!("case {name} ended without reporting completion"),
            Err(payload) => std::panic::resume_unwind(payload),
        },
        Err(RecvTimeoutError::Timeout) => {
            panic!("case {name} did not finish within {deadline:?}")
        }
    }
}

#[cfg(target_arch = "wasm32")]
pub async fn await_with_deadline<F: Future>(name: &str, deadline: Duration, case: F) -> F::Output {
    use futures_util::future::{Either, select};
    use gloo_timers::future::TimeoutFuture;

    const LONGEST_TIMER_MILLIS: u32 = i32::MAX.unsigned_abs();
    let millis = u32::try_from(deadline.as_millis()).map_or(LONGEST_TIMER_MILLIS, |millis| {
        millis.min(LONGEST_TIMER_MILLIS)
    });
    match select(std::pin::pin!(case), TimeoutFuture::new(millis)).await {
        Either::Left((output, _)) => output,
        Either::Right(((), _)) => panic!("case {name} did not finish within {deadline:?}"),
    }
}

#[doc(hidden)]
#[macro_export]
macro_rules! conformance_case {
    ($case:ident, $deadline:expr, let $store:ident = $prepare:expr; $run:expr) => {
        #[cfg(not(target_arch = "wasm32"))]
        #[test]
        fn $case() {
            $crate::run_with_deadline(stringify!($case), $deadline, || {
                let $store = $prepare;
                $crate::run($run);
            });
        }

        #[cfg(target_arch = "wasm32")]
        #[$crate::wasm_bindgen_test::wasm_bindgen_test(crate = $crate::wasm_bindgen_test)]
        async fn $case() {
            let $store = $prepare;
            $crate::await_with_deadline(stringify!($case), $deadline, $run).await;
        }
    };
}

#[macro_export]
macro_rules! metadata_store_tests {
    ($module:ident, $make:expr) => {
        $crate::metadata_store_tests!(
            @expand $module, $make,
            quick: [
                volumes::create_returns_empty_volume,
                volumes::create_returns_distinct_ids,
                volumes::unknown_volume_is_rejected_by_execute,
                volumes::unknown_volume_is_rejected_by_fork,
                volumes::unknown_volume_is_rejected_by_entries,
                volumes::unknown_volume_is_rejected_by_list_keys,
                volumes::unknown_volume_is_rejected_by_watch,
                forks::fork_inherits_entries_up_to_position,
                forks::fork_and_parent_diverge_after_fork,
                forks::fork_beyond_head_is_rejected,
                forks::fork_at_zero_is_empty,
                forks::fork_of_empty_volume_at_zero,
                forks::fork_at_older_position_ignores_later_parent_entries,
                forks::nested_forks_see_ancestors_within_bounds,
                forks::fork_of_fork_inside_inherited_range,
                forks::fork_of_fork_inside_own_range,
                forks::sibling_forks_are_independent,
                transactions::operations_run_in_order_with_read_your_writes,
                transactions::failed_assertion_leaves_no_trace,
                transactions::failed_assertion_in_fork_leaves_no_trace,
                transactions::assertion_index_counts_tuple_operations,
                transactions::assertion_index_counts_vec_operations,
                transactions::evaluation_stops_at_first_failing_assertion,
                transactions::passing_assertions_append_nothing,
                transactions::get_head_reflects_earlier_writes,
                transactions::read_only_transaction_leaves_head_unchanged,
                transactions::execute_returns_one_outcome_per_operation,
                transactions::write_operations_append_one_entry_each,
                keys::set_then_get_returns_value_and_seq,
                keys::overwrite_replaces_value_and_seq,
                keys::setting_unchanged_value_appends_entry,
                keys::delete_then_get_returns_none,
                keys::deleting_absent_key_is_noop,
                keys::set_after_delete_restores_key,
                keys::empty_value_is_live,
                keys::deletion_in_fork_hides_parent_key_in_fork_only,
                keys::deleting_inherited_key_appends_entry,
                keys::deleting_key_deleted_before_fork_is_noop,
                keys::parent_deletion_after_fork_does_not_affect_child,
                keys::setting_in_fork_does_not_leak_to_parent,
                keys::keys_compare_bytewise,
                conditions::equals_condition,
                conditions::empty_condition_with_nested_prefixes,
                conditions::empty_condition_ignores_deleted_keys,
                conditions::empty_prefix_covers_whole_key_space,
                conditions::empty_condition_sees_fork_overlay,
                conditions::head_condition,
                conditions::file_version_condition,
                conditions::file_version_condition_in_fork,
                conditions::combinators,
                conditions::conditions_observe_earlier_operations,
                files::unwritten_file_is_empty,
                files::write_then_read_provenance,
                files::overwrite_in_middle_splits_piece,
                files::overwrite_covering_several_pieces,
                files::exact_overwrite_replaces_piece,
                files::write_past_end_leaves_zero_hole,
                files::write_at_end_appends_at_length,
                files::zero_size_write_is_noop,
                files::content_offset_is_honoured,
                files::truncate_shrink_then_grow_reads_zeros,
                files::truncate_to_same_length_appends_entry,
                files::truncate_of_unwritten_file_creates_hole,
                files::truncate_in_fork_hides_parent_bytes_after_regrowth,
                files::write_in_fork_overlays_inherited_bytes,
                files::fork_at_older_position_sees_older_file,
                files::file_info_tracks_latest_write_or_truncate,
                files::range_beyond_length_gives_no_blobs,
                files::partial_range_clips_pieces,
                files::files_are_independent,
                streams::entries_from_zero_lists_whole_history,
                streams::entries_from_middle,
                streams::entries_at_or_after_head_yield_nothing,
                streams::entries_across_fork_boundary,
                streams::entries_across_nested_forks,
                streams::list_keys_is_in_ascending_order,
                streams::list_keys_filters_by_prefix,
                streams::list_keys_skips_deleted_keys,
                streams::list_keys_merges_fork_overlay,
                streams::watch_yields_current_head_immediately,
                streams::watch_yields_after_append,
                streams::watch_values_strictly_increase,
                streams::watch_on_fork_ignores_parent_appends,
                streams::watch_on_parent_ignores_fork_appends,
                streams::watch_wakes_every_pending_watcher,
                streams::watch_after_beyond_head_waits_until_head_passes_it,
                streams::watch_sees_commit_of_abandoned_execute,
                streams::entries_page_through_long_history_across_forks,
                streams::list_keys_skips_long_runs_of_deleted_keys,
                streams::list_keys_on_fork_skips_parent_keys_written_after_fork,
                streams::list_keys_across_deep_fork_chain,
                conditions::empty_condition_pages_through_deleted_keys,
                files::fork_after_exact_overwrite_hides_dead_piece,
                files::older_fork_hides_dead_piece_starting_inside_range,
                files::fork_reads_bytes_through_long_parent_history,
                keys::empty_key_behaves_like_any_other_key,
                files::write_overflow_is_rejected_as_too_large,
                streams::streams_are_owned_and_static,
                streams::entries_end_at_the_head_observed_when_reading_starts,
            ],
            long: [
                streams::list_keys_pages_through_many_keys,
                streams::list_keys_pages_through_fork_overlay,
                streams::list_keys_tolerates_concurrent_changes,
                streams::list_keys_on_fork_tolerates_concurrent_changes,
                differential::differential_against_reference,
            ]
        );
    };
    (
        @expand $module:ident, $make:expr,
        quick: [$($quick_group:ident :: $quick_case:ident),* $(,)?],
        long: [$($long_group:ident :: $long_case:ident),* $(,)?]
    ) => {
        mod $module {
            #[allow(unused_imports)]
            use super::*;

            $(
                $crate::conformance_case!(
                    $quick_case,
                    $crate::CASE_DEADLINE,
                    let store = {
                        let make = $make;
                        make()
                    };
                    $crate::metadata_cases::$quick_group::$quick_case(&store)
                );
            )*

            $(
                $crate::conformance_case!(
                    $long_case,
                    $crate::LONG_CASE_DEADLINE,
                    let store = {
                        let make = $make;
                        make()
                    };
                    $crate::metadata_cases::$long_group::$long_case(&store)
                );
            )*
        }
    };
}

#[macro_export]
macro_rules! metadata_store_reopen_tests {
    ($module:ident, $location:expr, $open:expr) => {
        mod $module {
            #[allow(unused_imports)]
            use super::*;

            $crate::conformance_case!(
                state_survives_reopen,
                $crate::CASE_DEADLINE,
                let location = {
                    let location = $location;
                    location()
                };
                $crate::metadata_cases::persistence::state_survives_reopen(&location, $open)
            );
        }
    };
}

#[macro_export]
macro_rules! metadata_store_failure_tests {
    ($module:ident, $location:expr, $open:expr, unreadable_volume: $break:expr, failure: $failure:expr $(,)?) => {
        mod $module {
            #[allow(unused_imports)]
            use super::*;

            $crate::metadata_store_failure_tests!(
                @expand $location, $open, $break, $failure,
                entries_end_after_failing_to_read_the_volume,
                list_keys_ends_after_failing_to_read_the_volume,
                watch_ends_after_failing_to_read_the_volume,
            );
        }
    };
    ($module:ident, $location:expr, $open:expr, unreadable_entry: $break:expr, failure: $failure:expr $(,)?) => {
        mod $module {
            #[allow(unused_imports)]
            use super::*;

            $crate::metadata_store_failure_tests!(
                @expand $location, $open, $break, $failure,
                entries_end_after_failing_to_read_an_entry,
            );
        }
    };
    (@expand $location:expr, $open:expr, $break:expr, $failure:expr, $($case:ident),* $(,)?) => {
        $(
            $crate::conformance_case!(
                $case,
                $crate::CASE_DEADLINE,
                let location = {
                    let location = $location;
                    location()
                };
                $crate::metadata_cases::failures::$case(&location, $open, $break, $failure)
            );
        )*
    };
}

#[macro_export]
macro_rules! content_store_tests {
    ($module:ident, $make:expr) => {
        mod $module {
            #[allow(unused_imports)]
            use super::*;

            $crate::content_store_tests!(@cases $make);
        }
    };
    ($module:ident, $make:expr, corrupt: $corrupt:expr) => {
        mod $module {
            #[allow(unused_imports)]
            use super::*;

            $crate::content_store_tests!(@cases $make);
            $crate::content_store_tests!(@repair_cases $make, $corrupt);
        }
    };
    ($module:ident, $make:expr, corrupt: $corrupt:expr, inspect: $inspect:expr $(,)?) => {
        mod $module {
            #[allow(unused_imports)]
            use super::*;

            $crate::content_store_tests!(@cases $make);
            $crate::content_store_tests!(@repair_cases $make, $corrupt);
            $crate::content_store_tests!(
                @expand_with_hook $make, $inspect,
                put_never_exposes_a_partial_object,
            );
        }
    };
    (@cases $make:expr) => {
        $crate::content_store_tests!(
            @expand $make,
            put_returns_blake3_hash,
            put_then_get_roundtrip,
            put_is_idempotent,
            distinct_blobs_are_independent,
            ranged_get,
            get_past_end_is_out_of_bounds,
            zero_size_get_returns_nothing,
            unknown_hash_is_rejected,
            empty_blob_roundtrip,
            large_blob_ranged_reads,
            get_during_put_sees_absent_or_complete_blob,
        );
    };
    (@repair_cases $make:expr, $corrupt:expr) => {
        $crate::content_store_tests!(
            @expand_with_hook $make, $corrupt,
            put_repairs_shorter_stored_object,
            put_repairs_longer_stored_object,
            put_repairs_same_length_stored_object,
            put_repairs_corrupt_empty_blob,
            empty_placeholder_is_never_served_and_put_repairs_it,
        );
    };
    (@expand $make:expr, $($case:ident),* $(,)?) => {
        $(
            $crate::conformance_case!(
                $case,
                $crate::CASE_DEADLINE,
                let store = {
                    let make = $make;
                    make()
                };
                $crate::content_cases::$case(&store)
            );
        )*
    };
    (@expand_with_hook $make:expr, $hook:expr, $($case:ident),* $(,)?) => {
        $(
            $crate::conformance_case!(
                $case,
                $crate::CASE_DEADLINE,
                let store = {
                    let make = $make;
                    make()
                };
                $crate::content_cases::$case(&store, $hook)
            );
        )*
    };
}
