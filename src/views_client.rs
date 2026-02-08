use http::Uri;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioIo;
use rustls::ClientConfig;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto;
use std::sync::Arc;
use tokio_rustls::TlsConnector;
use tonic::service::Interceptor;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Channel, Endpoint};
use tower::Service;
use tower::service_fn;

use crate::canary::views::grpc::api::canary_views_api_service_client::CanaryViewsApiServiceClient;
use crate::canary::views::grpc::api::*;

#[derive(Debug)]
struct AcceptAnyCert;

impl ServerCertVerifier for AcceptAnyCert {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

trait TonicIo: hyper::rt::Read + hyper::rt::Write {}
impl<T> TonicIo for T where T: hyper::rt::Read + hyper::rt::Write {}

#[derive(Clone)]
pub struct ApiKeyInterceptor {
    api_key: tonic::metadata::MetadataValue<tonic::metadata::Ascii>,
}

impl Interceptor for ApiKeyInterceptor {
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> Result<tonic::Request<()>, tonic::Status> {
        request
            .metadata_mut()
            .insert("canary-api-token", self.api_key.clone());
        Ok(request)
    }
}

pub struct ViewsClient {
    inner: CanaryViewsApiServiceClient<InterceptedService<Channel, ApiKeyInterceptor>>,
    cci: i32,
}

impl ViewsClient {
    /// Connect to a Canary Views service and acquire a client connection ID.
    pub async fn connect(
        endpoint: impl Into<String>,
        api_key: impl Into<String>,
        app: impl Into<String>,
        user_id: impl Into<String>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        if crypto::CryptoProvider::get_default().is_none() {
            let _ = crypto::ring::default_provider().install_default();
        }

        let verifier = Arc::new(AcceptAnyCert);

        let mut config = ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(verifier)
            .with_no_client_auth();

        config.alpn_protocols.push(b"h2".to_vec());

        let tls = TlsConnector::from(Arc::new(config));

        let mut http = HttpConnector::new();
        http.enforce_http(false);

        type BoxedIo = Box<dyn TonicIo + Send + Unpin>;

        let connector = service_fn(move |uri: Uri| {
            let tls = tls.clone();
            let mut http = http.clone();
            async move {
                let tcp = http.call(uri.clone()).await?;
                let tcp = tcp.into_inner();
                if uri.scheme_str() == Some("https") {
                    let host = uri
                        .host()
                        .ok_or_else(|| {
                            std::io::Error::new(std::io::ErrorKind::InvalidInput, "missing host")
                        })?
                        .to_string();
                    let server_name =
                        rustls::pki_types::ServerName::try_from(host).map_err(|_| {
                            std::io::Error::new(
                                std::io::ErrorKind::InvalidInput,
                                "invalid server name",
                            )
                        })?;
                    let tls_stream = tls.connect(server_name, tcp).await?;
                    Ok::<BoxedIo, Box<dyn std::error::Error + Send + Sync>>(Box::new(TokioIo::new(
                        tls_stream,
                    )))
                } else {
                    Ok::<BoxedIo, Box<dyn std::error::Error + Send + Sync>>(Box::new(TokioIo::new(
                        tcp,
                    )))
                }
            }
        });

        let endpoint = Endpoint::from_shared(endpoint.into())?;
        let channel = Channel::new(connector, endpoint);

        let api_key: tonic::metadata::MetadataValue<_> = api_key.into().parse()?;
        let interceptor = ApiKeyInterceptor { api_key };
        let mut inner = CanaryViewsApiServiceClient::with_interceptor(channel, interceptor);

        let resp = inner
            .get_client_connection_id(GetClientConnectionIdRequest {
                app: app.into(),
                user_id: user_id.into(),
            })
            .await?
            .into_inner();

        Ok(Self {
            inner,
            cci: resp.cci,
        })
    }

    /// Release the client connection ID.
    pub async fn disconnect(&mut self) -> Result<(), tonic::Status> {
        self.inner
            .release_client_connection_id(ReleaseClientConnectionIdRequest { cci: self.cci })
            .await?;
        Ok(())
    }

    /// Send a keepalive for the client connection.
    pub async fn keepalive(&mut self) -> Result<(), tonic::Status> {
        self.inner
            .keepalive_client_connection_id(KeepaliveClientConnectionIdRequest { cci: self.cci })
            .await?;
        Ok(())
    }

    /// Test the gRPC connection.
    pub async fn test(&mut self) -> Result<(), tonic::Status> {
        self.inner.test(()).await?;
        Ok(())
    }

    /// Get the service version.
    pub async fn get_version(&mut self) -> Result<GetWebServiceVersionResponse, tonic::Status> {
        Ok(self.inner.get_web_service_version(()).await?.into_inner())
    }

    /// Get the list of views accessible to this connection.
    pub async fn get_views(&mut self) -> Result<GetViewsResponse, tonic::Status> {
        Ok(self
            .inner
            .get_views(GetViewsRequest { cci: self.cci })
            .await?
            .into_inner())
    }

    /// Get the datasets for a view.
    pub async fn get_dataset_list(
        &mut self,
        view: impl Into<String>,
        include_hidden: bool,
    ) -> Result<GetDataSetListResponse, tonic::Status> {
        Ok(self
            .inner
            .get_data_set_list(GetDataSetListRequest {
                view: view.into(),
                include_hidden,
                cci: self.cci,
            })
            .await?
            .into_inner())
    }

    /// Get dataset info.
    pub async fn get_dataset_info(
        &mut self,
        view: impl Into<String>,
        dataset_name: impl Into<String>,
    ) -> Result<GetDatasetInfoResponse, tonic::Status> {
        Ok(self
            .inner
            .get_dataset_info(GetDatasetInfoRequest {
                view: view.into(),
                dataset_name: dataset_name.into(),
                cci: self.cci,
            })
            .await?
            .into_inner())
    }

    /// Get the tag list for a dataset.
    pub async fn get_tag_list(
        &mut self,
        view: impl Into<String>,
        dataset_name: impl Into<String>,
        starting_offset: i32,
        max_count: i32,
    ) -> Result<GetTagListResponse, tonic::Status> {
        Ok(self
            .inner
            .get_tag_list(GetTagListRequest {
                view: view.into(),
                dataset_name: dataset_name.into(),
                starting_offset,
                max_count,
                cci: self.cci,
            })
            .await?
            .into_inner())
    }

    /// Get tag info for the specified tags.
    pub async fn get_tag_info(
        &mut self,
        view: impl Into<String>,
        tag_names: Vec<String>,
    ) -> Result<GetTagInfoResponse, tonic::Status> {
        Ok(self
            .inner
            .get_tag_info(GetTagInfoRequest {
                view: view.into(),
                tag_names,
                cci: self.cci,
            })
            .await?
            .into_inner())
    }

    /// Get tag data context (temporal bounds) for specified tags.
    pub async fn get_tag_data_context(
        &mut self,
        view: impl Into<String>,
        tag_names: Vec<String>,
    ) -> Result<GetTagDataContextResponse, tonic::Status> {
        Ok(self
            .inner
            .get_tag_data_context(GetTagDataContextRequest {
                view: view.into(),
                tag_names,
                cci: self.cci,
            })
            .await?
            .into_inner())
    }

    /// Get the current value of specified tags.
    pub async fn get_tag_current_value(
        &mut self,
        request: GetTagCurrentValueRequest,
    ) -> Result<GetTagCurrentValueResponse, tonic::Status> {
        Ok(self
            .inner
            .get_tag_current_value(GetTagCurrentValueRequest {
                cci: self.cci,
                ..request
            })
            .await?
            .into_inner())
    }

    /// Get raw data for tags within a time range.
    pub async fn get_raw_data(
        &mut self,
        request: GetRawDataRequest,
    ) -> Result<GetRawDataResponse, tonic::Status> {
        Ok(self
            .inner
            .get_raw_data(GetRawDataRequest {
                cci: self.cci,
                ..request
            })
            .await?
            .into_inner())
    }

    /// Get aggregate data for tags.
    pub async fn get_aggregate_data(
        &mut self,
        request: GetAggregateDataRequest,
    ) -> Result<GetAggregateDataResponse, tonic::Status> {
        Ok(self
            .inner
            .get_aggregate_data(GetAggregateDataRequest {
                cci: self.cci,
                ..request
            })
            .await?
            .into_inner())
    }

    /// Get tag statistics.
    pub async fn get_tag_statistics(
        &mut self,
        request: GetTagStatisticsRequest,
    ) -> Result<GetTagStatisticsResponse, tonic::Status> {
        Ok(self
            .inner
            .get_tag_statistics(GetTagStatisticsRequest {
                cci: self.cci,
                ..request
            })
            .await?
            .into_inner())
    }

    /// Get the list of available aggregates.
    pub async fn get_aggregate_list(&mut self) -> Result<GetAggregateListResponse, tonic::Status> {
        Ok(self.inner.get_aggregate_list(()).await?.into_inner())
    }

    /// Subscribe to live data updates. Returns a streaming response.
    pub async fn subscribe_to_live_data(
        &mut self,
        request: SubscribeToLiveDataRequest,
    ) -> Result<tonic::Streaming<SubscribeToLiveDataResponse>, tonic::Status> {
        Ok(self
            .inner
            .subscribe_to_live_data(SubscribeToLiveDataRequest {
                cci: self.cci,
                ..request
            })
            .await?
            .into_inner())
    }

    /// Browse the views tree by node ID.
    pub async fn browse(
        &mut self,
        node_id_path: impl Into<String>,
        force_reload: bool,
    ) -> Result<BrowseResponse, tonic::Status> {
        Ok(self
            .inner
            .browse(BrowseRequest {
                node_id_path: node_id_path.into(),
                force_reload,
            })
            .await?
            .into_inner())
    }

    /// Browse tags at a specified node.
    pub async fn browse_tags(
        &mut self,
        request: BrowseTagsRequest,
    ) -> Result<BrowseTagsResponse, tonic::Status> {
        Ok(self.inner.browse_tags(request).await?.into_inner())
    }

    /// Search for tags matching criteria.
    pub async fn search_tags(
        &mut self,
        request: SearchTagsRequest,
    ) -> Result<SearchTagsResponse, tonic::Status> {
        Ok(self.inner.search_tags(request).await?.into_inner())
    }

    /// Browse by tree path.
    pub async fn browse_path(
        &mut self,
        tree_path: Vec<String>,
    ) -> Result<BrowsePathResponse, tonic::Status> {
        Ok(self
            .inner
            .browse_path(BrowsePathRequest { tree_path })
            .await?
            .into_inner())
    }

    /// Get the client connection ID.
    pub fn cci(&self) -> i32 {
        self.cci
    }

    /// Get a mutable reference to the underlying tonic client for direct RPC access.
    pub fn inner_mut(
        &mut self,
    ) -> &mut CanaryViewsApiServiceClient<InterceptedService<Channel, ApiKeyInterceptor>> {
        &mut self.inner
    }
}
