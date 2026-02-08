//! Crowsong - Canary Views gRPC Client Library
//!
//! This crate provides a Rust client for interacting with the Canary Views API service.

pub mod canary {
    pub mod views {
        pub mod grpc {
            pub mod common {
                tonic::include_proto!("canary.views.grpc.common");
            }
            pub mod api {
                tonic::include_proto!("canary.views.grpc.api");
            }
        }
    }
    pub mod store_and_forward2 {
        pub mod grpc {
            pub mod api {
                tonic::include_proto!("canary.store_and_forward2.grpc.api");
            }
        }
    }
    pub mod utility {
        pub mod protobuf_shared_types {
            tonic::include_proto!("canary.utility.protobuf_shared_types");
        }
    }
    pub mod calculations {
        pub mod grpc {
            pub mod common {
                tonic::include_proto!("canary.calculations.grpc.common");
            }
        }
        pub mod batch {
            pub mod grpc {
                pub mod common {
                    tonic::include_proto!("canary.calculations.batch.grpc.common");
                }
            }
        }
    }
}

pub mod views_client;
#[cfg(feature = "extension-module")]
pub mod python;

pub use views_client::ViewsClient;
