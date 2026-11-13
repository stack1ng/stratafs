use futures_lite::future::{block_on, poll_once};
use futures_util::future::{join, join3};

use super::*;

// read is a read that takes the round trips, as a transaction's does.
async fn read(round_trips: &RoundTrips) {
    round_trips.read().await;
}

// counted is how many round trips reads takes.
fn counted(reads: impl AsyncFnOnce(&RoundTrips)) -> usize {
    let round_trips = RoundTrips::default();
    round_trips.during(|| block_on(reads(&round_trips)))
}

#[test]
fn reads_one_after_another_are_a_round_trip_each() {
    let count = counted(async |round_trips| {
        read(round_trips).await;
        read(round_trips).await;
        read(round_trips).await;
    });
    assert_eq!(count, 3);
}

#[test]
fn reads_sent_together_are_one_round_trip() {
    let count = counted(async |round_trips| {
        join3(read(round_trips), read(round_trips), read(round_trips)).await;
    });
    assert_eq!(count, 1);
}

#[test]
fn a_read_that_follows_one_of_a_join_is_the_next_round_trip() {
    let count = counted(async |round_trips| {
        let chain = async {
            read(round_trips).await;
            read(round_trips).await;
        };
        join(read(round_trips), chain).await;
    });
    assert_eq!(count, 2);
}

#[test]
fn chains_run_side_by_side_take_as_many_round_trips_as_the_longest() {
    let count = counted(async |round_trips| {
        let two = async {
            read(round_trips).await;
            read(round_trips).await;
        };
        let three = async {
            read(round_trips).await;
            read(round_trips).await;
            read(round_trips).await;
        };
        join(two, three).await;
    });
    assert_eq!(count, 3);
}

#[test]
fn a_read_dropped_while_it_waits_leaves_no_round_trip_gathering() {
    let count = counted(async |round_trips| {
        let mut dropped = Box::pin(read(round_trips));
        assert!(poll_once(dropped.as_mut()).await.is_none());
        drop(dropped);
        read(round_trips).await;
    });
    assert_eq!(count, 2);
}

#[test]
fn each_during_counts_from_nothing() {
    let round_trips = RoundTrips::default();
    assert_eq!(round_trips.during(|| block_on(read(&round_trips))), 1);
    let together = || {
        block_on(join(read(&round_trips), read(&round_trips)));
    };
    assert_eq!(round_trips.during(together), 1);
}

#[test]
fn outside_during_a_read_runs_at_once_and_is_not_counted() {
    let round_trips = RoundTrips::default();
    let mut outside = Box::pin(read(&round_trips));
    assert!(block_on(poll_once(outside.as_mut())).is_some());
    assert_eq!(round_trips.during(|| ()), 0);
}

#[test]
#[should_panic(expected = "another thread read")]
fn a_read_from_another_thread_while_counting_fails_the_count() {
    let round_trips = RoundTrips::default();
    round_trips.during(|| {
        std::thread::scope(|scope| {
            scope.spawn(|| block_on(read(&round_trips)));
        });
    });
}
