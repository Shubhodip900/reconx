/// grpc/proto.rs — generated proto bindings included via tonic's include_proto! macro.
///
/// The actual Rust source is emitted by tonic-build into $OUT_DIR during
/// the build.rs compilation step. This module simply pulls it in.

/// `reconx.engine` package — ReconciliationEngine service, StateRequest,
/// StateResponse, and MatchDetail messages.
pub mod engine {
    tonic::include_proto!("reconx.engine");
}

/// `reconx.common` package — ReconStatus enum, Metadata and ErrorResponse messages.
pub mod common {
    tonic::include_proto!("reconx.common");
}

// Re-export the server trait + service builder at the top of the grpc module.
pub use engine::reconciliation_engine_server::{
    ReconciliationEngine, ReconciliationEngineServer,
};
pub use engine::{MatchDetail, StateRequest, StateResponse};
pub use common::ReconStatus;
