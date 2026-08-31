use std::cmp::Ordering;
use std::fmt::Display;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::{engine::general_purpose::URL_SAFE, Engine};
use thiserror::Error;

use crate::snapshot::{SnapshotCursor, SnapshotId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PaginationCursor<T> {
    time: SystemTime,
    value: T,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Paginated<T> {
    pub items: Vec<T>,
    pub next_token: Option<String>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum PaginationError {
    #[error("error decoding cursor: {0}")]
    DecodeCursor(String),
    #[error("invalid cursor format")]
    InvalidCursorFormat,
    #[error("invalid cursor format (not utf-8): {0}")]
    InvalidCursorUtf8(String),
    #[error("invalid timestamp format in cursor: {0}")]
    InvalidCursorTimestamp(String),
    #[error("invalid cursor value: {0}")]
    InvalidCursorValue(String),
}

impl<T> PaginationCursor<T> {
    pub fn new(time: SystemTime, value: T) -> Self {
        Self { time, value }
    }

    pub fn time(&self) -> SystemTime {
        self.time
    }

    pub fn value(&self) -> &T {
        &self.value
    }

    pub fn parse(token: &str) -> Result<Self, PaginationError>
    where
        T: for<'a> TryFrom<&'a str>,
        for<'a> <T as TryFrom<&'a str>>::Error: ToString,
    {
        let decoded = URL_SAFE
            .decode(token)
            .map_err(|err| PaginationError::DecodeCursor(err.to_string()))?;
        let decoded = String::from_utf8(decoded)
            .map_err(|err| PaginationError::InvalidCursorUtf8(err.to_string()))?;
        let (timestamp, value) = decoded
            .split_once("__")
            .ok_or(PaginationError::InvalidCursorFormat)?;
        let cursor_time = chrono::DateTime::parse_from_rfc3339(timestamp)
            .map_err(|err| PaginationError::InvalidCursorTimestamp(err.to_string()))?
            .with_timezone(&chrono::Utc);
        let value = T::try_from(value)
            .map_err(|err| PaginationError::InvalidCursorValue(err.to_string()))?;

        Ok(Self::new(SystemTime::from(cursor_time), value))
    }

    pub fn paginate<Item>(
        &self,
        mut items: Vec<Item>,
        limit: Option<u32>,
        mut sort: impl FnMut(&Item, &Item) -> Ordering,
        compare_cursor: impl FnMut(&Item, &Self) -> Ordering,
        to_cursor: impl FnMut(&Item) -> Self,
    ) -> Paginated<Item>
    where
        T: Display,
    {
        if matches!(limit, Some(0)) {
            return Paginated {
                items: Vec::new(),
                next_token: None,
            };
        }

        items.sort_by(|a, b| sort(a, b));
        self.paginate_sorted(items, limit, compare_cursor, to_cursor)
    }

    pub fn paginate_sorted<Item>(
        &self,
        mut items: Vec<Item>,
        limit: Option<u32>,
        mut compare_cursor: impl FnMut(&Item, &Self) -> Ordering,
        mut to_cursor: impl FnMut(&Item) -> Self,
    ) -> Paginated<Item>
    where
        T: Display,
    {
        if matches!(limit, Some(0)) {
            return Paginated {
                items: Vec::new(),
                next_token: None,
            };
        }

        items.retain(|item| compare_cursor(item, self) == Ordering::Greater);

        let next_token = match limit.and_then(|l| usize::try_from(l).ok()) {
            Some(limit) if items.len() > limit => {
                let token = items.get(limit - 1).map(|item| to_cursor(item).encode());
                items.truncate(limit);
                token
            }
            _ => None,
        };

        Paginated { items, next_token }
    }
}

impl<T> PaginationCursor<T>
where
    T: Display,
{
    pub fn encode(&self) -> String {
        URL_SAFE.encode(format!(
            "{}__{}",
            chrono::DateTime::<chrono::Utc>::from(self.time)
                .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
            self.value,
        ))
    }
}

impl<T> PaginationCursor<T>
where
    T: Ord,
{
    pub fn compare_desc(
        a_time: SystemTime,
        a_value: &T,
        b_time: SystemTime,
        b_value: &T,
    ) -> Ordering {
        b_time.cmp(&a_time).then_with(|| a_value.cmp(b_value))
    }
}

/// A snapshot record's `created_at` as an instant.
pub fn system_time_from_unix_ms(unix_ms: i64) -> SystemTime {
    if unix_ms >= 0 {
        UNIX_EPOCH + Duration::from_millis(unix_ms as u64)
    } else {
        UNIX_EPOCH - Duration::from_millis(unix_ms.unsigned_abs())
    }
}

/// Converts an instant to whole milliseconds, rounding toward positive infinity.
fn unix_ms_ceil(time: SystemTime) -> i64 {
    match time.duration_since(UNIX_EPOCH) {
        Ok(elapsed) => {
            let ms = i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX);
            if elapsed.subsec_nanos() % 1_000_000 == 0 {
                ms
            } else {
                ms.saturating_add(1)
            }
        }
        // Truncating a pre-epoch magnitude toward zero is already a ceiling.
        Err(before) => i64::try_from(before.duration().as_millis())
            .unwrap_or(i64::MAX)
            .saturating_neg(),
    }
}

/// Decodes the public snapshot-listing token into its catalog cursor.
///
/// The token format must remain stable across catalog backends.
pub fn snapshot_cursor_from_token(token: &str) -> Result<SnapshotCursor, PaginationError> {
    let parsed = PaginationCursor::<SnapshotId>::parse(token)?;
    Ok(SnapshotCursor::new(
        unix_ms_ceil(parsed.time()),
        parsed.value().clone(),
    ))
}

/// The public `x-next-token` for a position in a snapshot listing.
pub fn snapshot_next_token(cursor: &SnapshotCursor) -> String {
    PaginationCursor::new(
        system_time_from_unix_ms(cursor.created_at_unix_ms),
        cursor.snapshot_id.clone(),
    )
    .encode()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, UNIX_EPOCH};

    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
    struct TestId(u32);

    impl std::fmt::Display for TestId {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            self.0.fmt(f)
        }
    }

    impl TryFrom<&str> for TestId {
        type Error = std::num::ParseIntError;

        fn try_from(value: &str) -> Result<Self, Self::Error> {
            value.parse().map(Self)
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestItem {
        created_at: SystemTime,
        id: TestId,
    }

    fn item(id: u32, created_at: SystemTime) -> TestItem {
        TestItem {
            created_at,
            id: TestId(id),
        }
    }

    fn sort_test_items(a: &TestItem, b: &TestItem) -> Ordering {
        PaginationCursor::compare_desc(a.created_at, &a.id, b.created_at, &b.id)
    }

    fn compare_test_item_cursor(item: &TestItem, cursor: &PaginationCursor<TestId>) -> Ordering {
        PaginationCursor::compare_desc(item.created_at, &item.id, cursor.time(), cursor.value())
    }

    fn cursor_for_test_item(item: &TestItem) -> PaginationCursor<TestId> {
        PaginationCursor::new(item.created_at, item.id.clone())
    }

    fn encode_raw_cursor(timestamp: &str, value: &str) -> String {
        URL_SAFE.encode(format!("{timestamp}__{value}"))
    }

    #[test]
    fn parse_next_token_valid() {
        let ts = UNIX_EPOCH + Duration::from_secs(1_767_225_600);
        let token = encode_raw_cursor("2026-01-01T00:00:00Z", "2");

        let cursor = PaginationCursor::<TestId>::parse(&token).unwrap();

        assert_eq!(cursor.time(), ts);
        assert_eq!(cursor.value().to_string(), "2");
    }

    #[test]
    fn parse_next_token_invalid_returns_error() {
        let err = PaginationCursor::<TestId>::parse("not-base64").unwrap_err();
        assert!(matches!(err, PaginationError::DecodeCursor(_)));
    }

    #[test]
    fn parse_next_token_empty_returns_error() {
        let err = PaginationCursor::<TestId>::parse("").unwrap_err();
        assert_eq!(err, PaginationError::InvalidCursorFormat);
    }

    #[test]
    fn parse_next_token_invalid_format_returns_error() {
        let token = URL_SAFE.encode("2026-01-01T00:00:00Z-only");
        let err = PaginationCursor::<TestId>::parse(&token).unwrap_err();
        assert_eq!(err, PaginationError::InvalidCursorFormat);
    }

    #[test]
    fn parse_next_token_invalid_utf8_returns_error() {
        let token = URL_SAFE.encode([0xff, 0xfe, 0xfd]);
        let err = PaginationCursor::<TestId>::parse(&token).unwrap_err();
        assert!(matches!(err, PaginationError::InvalidCursorUtf8(_)));
    }

    #[test]
    fn parse_next_token_invalid_timestamp_returns_error() {
        let token = URL_SAFE.encode("bad-timestamp__1");
        let err = PaginationCursor::<TestId>::parse(&token).unwrap_err();
        assert!(matches!(err, PaginationError::InvalidCursorTimestamp(_)));
    }

    #[test]
    fn parse_next_token_invalid_value_returns_error() {
        let token = encode_raw_cursor("2026-01-01T00:00:00Z", "not-a-number");
        let err = PaginationCursor::<TestId>::parse(&token).unwrap_err();
        assert!(matches!(err, PaginationError::InvalidCursorValue(_)));
    }

    #[test]
    fn paginate_sorts_and_filters_by_cursor() {
        let t1 = UNIX_EPOCH + Duration::from_secs(200);
        let t2 = UNIX_EPOCH + Duration::from_secs(100);

        let items = vec![item(3, t1), item(1, t1), item(2, t2), item(4, t2)];

        let cursor = PaginationCursor::new(t1, TestId(1));
        let out = cursor.paginate(
            items,
            Some(2),
            sort_test_items,
            compare_test_item_cursor,
            cursor_for_test_item,
        );

        let ids: Vec<_> = out.items.into_iter().map(|m| m.id.0).collect();
        assert_eq!(ids, vec![3, 2]);
    }

    #[test]
    fn paginate_returns_none_next_token_when_page_not_full_or_limit_missing_or_invalid() {
        let t = UNIX_EPOCH + Duration::from_secs(100);
        let items = vec![item(1, t)];
        let cursor = PaginationCursor::new(SystemTime::now(), TestId(u32::MAX));

        let paginate = |limit| {
            cursor.paginate(
                items.clone(),
                limit,
                sort_test_items,
                compare_test_item_cursor,
                cursor_for_test_item,
            )
        };

        assert_eq!(paginate(Some(2)).next_token, None);
        assert_eq!(paginate(Some(3)).next_token, None);
        assert_eq!(paginate(Some(0)).next_token, None);
        assert_eq!(paginate(None).next_token, None);
    }

    #[test]
    fn paginate_returns_next_token_when_more_items_than_limit() {
        let t1 = UNIX_EPOCH + Duration::from_secs(200);
        let t2 = UNIX_EPOCH + Duration::from_secs(100);
        let t3 = UNIX_EPOCH + Duration::from_secs(50);
        let out = PaginationCursor::new(SystemTime::now(), TestId(u32::MAX)).paginate(
            vec![item(3, t1), item(2, t2), item(1, t3)],
            Some(2),
            sort_test_items,
            compare_test_item_cursor,
            cursor_for_test_item,
        );

        assert_eq!(out.items.len(), 2);
        let decoded =
            String::from_utf8(URL_SAFE.decode(out.next_token.expect("token")).unwrap()).unwrap();
        assert_eq!(decoded, "1970-01-01T00:01:40.000000000Z__2");
    }

    #[test]
    fn paginate_returns_no_token_when_items_exactly_fill_page() {
        let t1 = UNIX_EPOCH + Duration::from_secs(200);
        let t2 = UNIX_EPOCH + Duration::from_secs(100);
        let out = PaginationCursor::new(SystemTime::now(), TestId(u32::MAX)).paginate(
            vec![item(3, t1), item(2, t2)],
            Some(2),
            sort_test_items,
            compare_test_item_cursor,
            cursor_for_test_item,
        );

        assert_eq!(out.items.len(), 2);
        assert_eq!(out.next_token, None);
    }

    fn snapshot_id(text: &str) -> SnapshotId {
        SnapshotId::parse(text).expect("a fixed snapshot id")
    }

    #[test]
    fn the_public_token_is_base64url_of_an_rfc3339_instant_and_the_id() {
        let id = snapshot_id("0198f0a1-0000-7000-8000-0000000c0ffe");
        let token = snapshot_next_token(&SnapshotCursor::new(1_767_225_600_123, id.clone()));

        let decoded = String::from_utf8(URL_SAFE.decode(&token).expect("base64url")).unwrap();
        assert_eq!(
            decoded,
            "2026-01-01T00:00:00.123000000Z__0198f0a1-0000-7000-8000-0000000c0ffe"
        );
    }

    #[test]
    fn a_token_this_service_minted_round_trips_to_the_same_position() {
        let cursor = SnapshotCursor::new(
            1_767_225_600_123,
            snapshot_id("0198f0a1-0000-7000-8000-0000000c0ffe"),
        );

        let round_tripped =
            snapshot_cursor_from_token(&snapshot_next_token(&cursor)).expect("it should parse");

        assert_eq!(round_tripped, cursor);
    }

    #[test]
    fn a_token_minted_before_the_paging_moved_still_decodes() {
        let token =
            URL_SAFE.encode("2026-01-01T00:00:00.123000000Z__0198f0a1-0000-7000-8000-0000000c0ffe");

        let cursor = snapshot_cursor_from_token(&token).expect("an older token must still parse");

        assert_eq!(cursor.created_at_unix_ms, 1_767_225_600_123);
        assert_eq!(
            cursor.snapshot_id.to_string(),
            "0198f0a1-0000-7000-8000-0000000c0ffe"
        );
    }

    #[test]
    fn a_token_that_is_not_a_cursor_is_reported_rather_than_guessed_at() {
        assert!(matches!(
            snapshot_cursor_from_token("not-base64"),
            Err(PaginationError::DecodeCursor(_))
        ));
        assert_eq!(
            snapshot_cursor_from_token(&URL_SAFE.encode("2026-01-01T00:00:00Z-only")),
            Err(PaginationError::InvalidCursorFormat)
        );
        assert!(matches!(
            snapshot_cursor_from_token(&URL_SAFE.encode("2026-01-01T00:00:00Z__not-a-uuid")),
            Err(PaginationError::InvalidCursorValue(_))
        ));
    }

    #[test]
    fn a_sub_millisecond_instant_rounds_up_so_no_row_is_skipped() {
        let exact = UNIX_EPOCH + Duration::from_millis(1_000);
        assert_eq!(unix_ms_ceil(exact), 1_000, "a whole millisecond is exact");

        let finer = UNIX_EPOCH + Duration::from_nanos(1_000_000_001);
        assert_eq!(unix_ms_ceil(finer), 1_001);

        let before_epoch = UNIX_EPOCH - Duration::from_nanos(1_000_700_000);
        assert_eq!(
            unix_ms_ceil(before_epoch),
            -1_000,
            "toward zero is already the ceiling on the far side of the epoch"
        );
    }

    #[test]
    fn a_sub_millisecond_token_does_not_drop_the_rows_it_ties_with() {
        let token =
            URL_SAFE.encode("2026-01-01T00:00:00.123400000Z__ffffffff-ffff-ffff-ffff-ffffffffffff");

        let cursor = snapshot_cursor_from_token(&token).expect("it should parse");

        assert_eq!(
            cursor.created_at_unix_ms, 1_767_225_600_124,
            "a cursor rounded down to …123 would tie with every row in that millisecond and, \
             losing to the maximum uuid, exclude all of them"
        );
    }

    #[test]
    fn compare_desc_orders_by_time_desc_then_value_asc() {
        let t1 = UNIX_EPOCH + Duration::from_secs(100);
        let t2 = UNIX_EPOCH + Duration::from_secs(200);
        let id1 = TestId(1);
        let id2 = TestId(2);

        assert_eq!(
            PaginationCursor::compare_desc(t2, &id1, t1, &id2),
            Ordering::Less
        );
        assert_eq!(
            PaginationCursor::compare_desc(t1, &id1, t1, &id2),
            Ordering::Less
        );
    }
}
