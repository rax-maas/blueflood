namespace cpp telescope.thrift
namespace java telescope.thrift


enum UnitEnum {
  UNKNOWN,
  OTHER,
  BITS,
  BYTES,
  KILOBYTES,
  MEGABYTES,
  MILLISECONDS,
  SECONDS,
  TIMESTAMP_MILLISECONDS,
  TIMESTAMP_SECONDS,
  PERCENT,
}

enum FlapEnum {
  UNKNOWN,
  FLAPPING,
  NOFLAP,
}

enum AlarmState {
  UNKNOWN = 0,
  OK = 1,
  WARNING = 2,
  CRITICAL = 6,
  DISABLED = 7,
}

enum Result
{
  FAILED,
  OK,
}

enum CollectorState {
  UP,
  DOWN,
}

enum VerificationModel {
  ONE,
  QUORUM,
  ALL,
}

enum Resolution {
  FULL,
  MIN5,
  MIN20,
  MIN60,
  MIN240,
  MIN1440
}

exception InvalidQueryException {
  1: string why,
}

exception AlarmNotFoundException {
  1: string why,
}

exception InvalidRegexException {
  1: string why,
}

struct Metric
{
  1: byte metricType,
  2: optional double valueDbl,
  3: optional i64 valueI64,
  4: optional i32 valueI32,
  5: optional string valueStr,
  6: optional UnitEnum unitEnum,
  7: optional string unitOtherStr,
  8: optional bool valueBool
}

struct RollupMetric
{
  1: i64 numPoints,
  2: optional Metric rawSample,
  3: optional Metric average,
  4: optional Metric variance,
  5: optional Metric min,
  6: optional Metric max,
  7: i64 timestamp,
}

struct RollupMetrics
{
  1: list<RollupMetric> metrics,
  2: string unit,
}

struct MetricInfo
{
  1: string name,
  2: string unit,
}

struct CorrelatedStatus
{
  1: string id,
  2: string collector,
  3: AlarmState criteriaState,
  4: string status,
  5: CollectorState state,
  6: optional string monitoringZoneId,
  7: optional i64 timestamp,
}

struct Telescope
{
  1: string id,
  2: string checkId,
  3: string acctId,
  4: string checkModule,
  5: string entityId,
  6: string target,
  7: i64 timestamp,
  8: i32 consecutiveTrigger = 1,
  9: VerificationModel verifyModel,
  10: optional string analyzedByMonitoringZoneId,
  11: optional map<string, string> dimensions,
  12: optional map<string, Metric> metrics,
  14: optional string dimensionKey;
  15: optional string collector;
  16: optional FlapEnum flapEnum = FlapEnum.UNKNOWN
  17: optional AlarmState criteriaState
  18: optional AlarmState computedState
  19: optional string alarmId
  20: optional byte availability
  21: optional byte state
  22: optional string status
  23: optional list<CorrelatedStatus> correlatedStatuses
  24: optional string monitoringZoneId
  25: optional string txnId
  26: optional string checkType
  27: optional AlarmState previousKnownState
  28: optional list<string> collectorKeys
  29: optional i32 period = 0
  30: optional i32 timeout = 0
  31: optional map<string, i32> metricsTtls
  32: optional list<string> resourceNames
}

struct RepeatEvent
{
  1: string alarmId,
}

struct CollectorEvent
{
  1: string collector,
  2: i64 timestamp,
  3: CollectorState availability,
}

struct ConsecutiveTriggerEvent
{
  1: string alarmId,
  2: string checkId,
  3: string dimensionKey,
  4: string monitoringZoneId,
  5: i64 timestamp,
  6: AlarmState criteriaState,
  7: i32 consecutiveEvents = 0,
}

struct CollectorTimeout
{
  1: i64 timeout,
}

/* Remove the boundcheck 'id' from checks */
struct RemoveBoundCheck
{
  1: string checkId,
  2: string monitoringZoneId
}

struct AlarmDisableStateUpdate {
  1: string acctId,
  2: string entityId,
  3: string alarmId,
  4: bool disabled,
}

struct MinimalState
{
  1: string alarmId,
  2: string dimensionKey,
  3: string checkId,
  4: AlarmState computedState,
  5: i64 timestamp,
}

struct FlapState
{
  1: string alarmId,
  2: string dimensionKey,
  3: string checkId,
  4: double flapCalc,
  5: FlapEnum flapEnum,
}

struct ConditionState
{
  1: AlarmState state,
  2: string message,
}

/* Made as a struct with optional fields.  Because Thrift unions in 2011
   is about as uncertain as C++ Templates in 1999. */
/* This represents a telescope or a removed boundcheck so that you can pass
   the CEP a list of indeterminate types from the stratcon */
struct TelescopeOrRemove
{
  1: optional Telescope telescope,
  2: optional RemoveBoundCheck bc,
}

struct AgentTelescope {
  1: optional string error,
  2: optional list<TelescopeOrRemove> ts,
}

service TelescopeServer
{
  /* Publish a list of Telescope structs to the EventEngine. */
  void Publish(1: list<Telescope> messages);

  /* Publish a list of RepeatEvents (which cause the last event with the
     specified alarmId to be repeated) to the EventEngine. */
  void RepeatEvents(1: list<RepeatEvent> repeatEvents);

  /* Publish a list of RemoveBoundChecks (which cause the bound check with
     the specified id to be removed) to the EventEngine. */
  void RemoveBoundChecks(1: list<RemoveBoundCheck> removeBoundChecks);

  /* Publish a CollectorTimeout (which changes the amount of time before a
     collector is marked inactive) to the EventEngine. */
  void UpdateCollectorTimeout(1: CollectorTimeout timeout);

  /* Test compile an EPL query. The transaction ID is used for error logging.
     If a compilation error occurs an InvalidQueryException is thrown. */
  void TestCompileAlarm(1: string txnId, 2: string query) throws (1: InvalidQueryException iqe);

  /* Add an EPL query for the specified alarmId. If a compilation error occurs
     an InvalidQueryException is thrown.*/
  void AddAlarm(1: string alarmId, 2: string query) throws (1: InvalidQueryException iqe);

  /* Remove the query that was added for the specified alarmId. If no such
     query is registered an AlarmNotFoundException is thrown. */
  void RemoveAlarm(1: string alarmId) throws (1: AlarmNotFoundException qnfe);

  /* Test that a given regex pattern can be compiled. If it cannot compile
       the regex, an InvalidRegexException is thrown. */
  void TestCompileRegexes(1: string txnId, 2: list<string> regexes) throws (1: InvalidRegexException ire);

  /* Retrieve the query that was added for the specified alarmId. If no such
     query is registered an AlarmNotFoundException is thrown. */
  string GetAlarm(1: string alarmId) throws (1: AlarmNotFoundException qnfe);

  /* Retrieve the latest state of an alarm for a given alarmId. If no query is registered
     for the alarmId, an AlarmNotFoundException is thrown. */
  string GetAlarmState(1: string alarmId) throws (1: AlarmNotFoundException qnfe);

  /* Set an alarm's disabled/enabled state. */
  void UpdateAlarmDisabledState(1: string acctId, 2: string entityId, 3: string alarmId, 4: bool disabled);
}

service TelescopeTestServer
{
  /* Test compile run an alarm against a list of data. The transaction ID is used for error logging.
     If a compilation error occurs an InvalidQueryException is thrown. */
  list<Telescope> TestRunAlarm(1: string txnId, 2: string query, 3: list<Telescope> messages) throws (1: InvalidQueryException iqe);
}

service RollupServer
{
  RollupMetrics GetRollupByPoints(
      1: string metricName,
      2: i64 from,
      3: i64 to,
      4: i32 points);
  RollupMetrics GetRollupByResolution(
      1: string metricName,
      2: i64 from,
      3: i64 to,
      4: Resolution resolution);
  list<MetricInfo> GetMetricsForCheck(
      1: string acctId,
      2: string entityId,
      3: string checkId);
}

const map<string, map<string, map<string, string>>> METRIC_UNITS_MAP = {
  "remote.dns": {
    "answer": {"unit": "ips", "description": ""},
    "rtt": {"unit": "milliseconds", "description": ""},
    "ttl": {"unit": "milliseconds", "description": ""}
  },
  "remote.tcp": {
    "banner": {"unit": "unknown", "description": ""},
    "banner_match": {"unit": "unknown", "description": ""},
    "duration": {"unit": "milliseconds", "description": ""},
    "tt_connect": {"unit": "milliseconds", "description": ""},
    "tt_firstbyte": {"unit": "milliseconds", "description": ""}
  },
  "remote.ftp-banner": {
    "banner": {"unit": "unknown", "description": ""},
    "banner_match": {"unit": "unknown", "description": ""},
    "duration": {"unit": "milliseconds", "description": ""},
    "tt_connect": {"unit": "milliseconds", "description": ""},
    "tt_firstbyte": {"unit": "milliseconds", "description": ""}
  },
  "remote.imap-banner": {
    "banner": {"unit": "unknown", "description": ""},
    "banner_match": {"unit": "unknown", "description": ""},
    "body_match": {"unit": "unknown", "description": ""},
    "duration": {"unit": "milliseconds", "description": ""},
    "tt_body": {"unit": "milliseconds", "description": ""},
    "tt_connect": {"unit": "milliseconds", "description": ""},
    "tt_firstbyte": {"unit": "milliseconds", "description": ""}
  },
  "remote.mssql-banner": {
    "duration": {"unit": "milliseconds", "description": ""},
    "tt_connect": {"unit": "milliseconds", "description": ""},
    "tt_firstbyte": {"unit": "milliseconds", "description": ""}
  },
  "remote.mysql-banner": {
    "duration": {"unit": "milliseconds", "description": ""},
    "tt_connect": {"unit": "milliseconds", "description": ""},
    "tt_firstbyte": {"unit": "milliseconds", "description": ""}
  },
  "remote.pop3-banner": {
    "duration": {"unit": "milliseconds", "description": ""},
    "tt_connect": {"unit": "milliseconds", "description": ""},
    "tt_firstbyte": {"unit": "milliseconds", "description": ""}
  },
  "remote.postgresql-banner": {
    "duration": {"unit": "milliseconds", "description": ""},
    "tt_connect": {"unit": "milliseconds", "description": ""},
    "tt_firstbyte": {"unit": "milliseconds", "description": ""}
  },
  "remote.telnet-banner": {
    "duration": {"unit": "milliseconds", "description": ""},
    "tt_connect": {"unit": "milliseconds", "description": ""},
    "tt_firstbyte": {"unit": "milliseconds", "description": ""}
  },
  "remote.http": {
    "bytes": {"unit": "bytes", "description": ""},
    "cert_end": {"unit": "timestamp_seconds", "description": ""},
    "cert_end_in": {"unit": "seconds", "description": ""},
    "cert_start": {"unit": "timestamp_seconds", "description": ""},
    "duration": {"unit": "milliseconds", "description": ""},
    "truncated": {"unit": "bytes", "description": ""},
    "tt_connect": {"unit": "milliseconds", "description": ""},
    "tt_firstbyte": {"unit": "milliseconds", "description": ""}
  },
  "remote.smtp-banner": {
    "bytes": {"unit": "bytes", "description": ""},
    "cert_end": {"unit": "timestamp_seconds", "description": ""},
    "cert_end_in": {"unit": "seconds", "description": ""},
    "cert_start": {"unit": "timestamp_seconds", "description": ""},
    "duration": {"unit": "milliseconds", "description": ""},
    "truncated": {"unit": "bytes", "description": ""},
    "tt_connect": {"unit": "milliseconds", "description": ""},
    "tt_firstbyte": {"unit": "milliseconds", "description": ""}
  },
  "remote.ping": {
    "available": {"unit": "percent", "description": ""},
    "average": {"unit": "seconds", "description": ""},
    "maximum": {"unit": "seconds", "description": ""},
    "minimum": {"unit": "seconds", "description": ""},
    "count": {"unit": "other", "description": ""}
  },
  "remote.ssh": {
    "duration": {"unit": "milliseconds", "description": ""}
  }
}

const map<string, UnitEnum> UNIT_ENUM_REVERSE_MAP = {
  "unknown": UnitEnum.UNKNOWN,
  "bits": UnitEnum.BITS,
  "bytes": UnitEnum.BYTES,
  "kilobytes": UnitEnum.KILOBYTES,
  "megabytes": UnitEnum.MEGABYTES,
  "milliseconds": UnitEnum.MILLISECONDS,
  "seconds": UnitEnum.SECONDS,
  "timestamp_milliseconds": UnitEnum.TIMESTAMP_MILLISECONDS,
  "timestamp_seconds": UnitEnum.TIMESTAMP_SECONDS,
  "percent": UnitEnum.PERCENT,
}
