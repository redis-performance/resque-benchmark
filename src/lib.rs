//! Library surface for `resque-bench`, split out from the binary so
//! integration tests under `tests/` can drive real workers/producers against
//! a real Redis instance without shelling out to the compiled binary.

pub mod job;
pub mod metrics;
pub mod producer;
pub mod report;
pub mod worker;
