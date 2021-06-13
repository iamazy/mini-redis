#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetRequest {
    #[prost(string, tag = "1")]
    pub key: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetResponse {
    #[prost(bool, tag = "1")]
    pub ok: bool,
    #[prost(string, tag = "2")]
    pub key: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub value: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RaftMessage {
    #[prost(string, tag = "1")]
    pub data: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub error: ::prost::alloc::string::String,
}
#[doc = r" Generated client implementations."]
pub mod mini_redis_service_client {
    #![allow(unused_variables, dead_code, missing_docs)]
    use tonic::codegen::*;
    pub struct MiniRedisServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl MiniRedisServiceClient<tonic::transport::Channel> {
        #[doc = r" Attempt to create a new client by connecting to a given endpoint."]
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> MiniRedisServiceClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::ResponseBody: Body + HttpBody + Send + 'static,
        T::Error: Into<StdError>,
        <T::ResponseBody as HttpBody>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor(inner: T, interceptor: impl Into<tonic::Interceptor>) -> Self {
            let inner = tonic::client::Grpc::with_interceptor(inner, interceptor);
            Self { inner }
        }
        pub async fn write(
            &mut self,
            request: impl tonic::IntoRequest<super::RaftMessage>,
        ) -> Result<tonic::Response<super::RaftMessage>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/mini_redis.MiniRedisService/Write");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn get(
            &mut self,
            request: impl tonic::IntoRequest<super::GetRequest>,
        ) -> Result<tonic::Response<super::GetResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/mini_redis.MiniRedisService/Get");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn append_entries(
            &mut self,
            request: impl tonic::IntoRequest<super::RaftMessage>,
        ) -> Result<tonic::Response<super::RaftMessage>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/mini_redis.MiniRedisService/AppendEntries");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn install_snapshot(
            &mut self,
            request: impl tonic::IntoRequest<super::RaftMessage>,
        ) -> Result<tonic::Response<super::RaftMessage>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/mini_redis.MiniRedisService/InstallSnapshot",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn vote(
            &mut self,
            request: impl tonic::IntoRequest<super::RaftMessage>,
        ) -> Result<tonic::Response<super::RaftMessage>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/mini_redis.MiniRedisService/Vote");
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
    impl<T: Clone> Clone for MiniRedisServiceClient<T> {
        fn clone(&self) -> Self {
            Self {
                inner: self.inner.clone(),
            }
        }
    }
    impl<T> std::fmt::Debug for MiniRedisServiceClient<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "MiniRedisServiceClient {{ ... }}")
        }
    }
}
#[doc = r" Generated server implementations."]
pub mod mini_redis_service_server {
    #![allow(unused_variables, dead_code, missing_docs)]
    use tonic::codegen::*;
    #[doc = "Generated trait containing gRPC methods that should be implemented for use with MiniRedisServiceServer."]
    #[async_trait]
    pub trait MiniRedisService: Send + Sync + 'static {
        async fn write(
            &self,
            request: tonic::Request<super::RaftMessage>,
        ) -> Result<tonic::Response<super::RaftMessage>, tonic::Status>;
        async fn get(
            &self,
            request: tonic::Request<super::GetRequest>,
        ) -> Result<tonic::Response<super::GetResponse>, tonic::Status>;
        async fn append_entries(
            &self,
            request: tonic::Request<super::RaftMessage>,
        ) -> Result<tonic::Response<super::RaftMessage>, tonic::Status>;
        async fn install_snapshot(
            &self,
            request: tonic::Request<super::RaftMessage>,
        ) -> Result<tonic::Response<super::RaftMessage>, tonic::Status>;
        async fn vote(
            &self,
            request: tonic::Request<super::RaftMessage>,
        ) -> Result<tonic::Response<super::RaftMessage>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct MiniRedisServiceServer<T: MiniRedisService> {
        inner: _Inner<T>,
    }
    struct _Inner<T>(Arc<T>, Option<tonic::Interceptor>);
    impl<T: MiniRedisService> MiniRedisServiceServer<T> {
        pub fn new(inner: T) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner, None);
            Self { inner }
        }
        pub fn with_interceptor(inner: T, interceptor: impl Into<tonic::Interceptor>) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner, Some(interceptor.into()));
            Self { inner }
        }
    }
    impl<T, B> Service<http::Request<B>> for MiniRedisServiceServer<T>
    where
        T: MiniRedisService,
        B: HttpBody + Send + Sync + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = Never;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/mini_redis.MiniRedisService/Write" => {
                    #[allow(non_camel_case_types)]
                    struct WriteSvc<T: MiniRedisService>(pub Arc<T>);
                    impl<T: MiniRedisService> tonic::server::UnaryService<super::RaftMessage> for WriteSvc<T> {
                        type Response = super::RaftMessage;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::RaftMessage>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).write(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = WriteSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/mini_redis.MiniRedisService/Get" => {
                    #[allow(non_camel_case_types)]
                    struct GetSvc<T: MiniRedisService>(pub Arc<T>);
                    impl<T: MiniRedisService> tonic::server::UnaryService<super::GetRequest> for GetSvc<T> {
                        type Response = super::GetResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GetRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).get(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = GetSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/mini_redis.MiniRedisService/AppendEntries" => {
                    #[allow(non_camel_case_types)]
                    struct AppendEntriesSvc<T: MiniRedisService>(pub Arc<T>);
                    impl<T: MiniRedisService> tonic::server::UnaryService<super::RaftMessage> for AppendEntriesSvc<T> {
                        type Response = super::RaftMessage;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::RaftMessage>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).append_entries(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = AppendEntriesSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/mini_redis.MiniRedisService/InstallSnapshot" => {
                    #[allow(non_camel_case_types)]
                    struct InstallSnapshotSvc<T: MiniRedisService>(pub Arc<T>);
                    impl<T: MiniRedisService> tonic::server::UnaryService<super::RaftMessage>
                        for InstallSnapshotSvc<T>
                    {
                        type Response = super::RaftMessage;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::RaftMessage>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).install_snapshot(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = InstallSnapshotSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/mini_redis.MiniRedisService/Vote" => {
                    #[allow(non_camel_case_types)]
                    struct VoteSvc<T: MiniRedisService>(pub Arc<T>);
                    impl<T: MiniRedisService> tonic::server::UnaryService<super::RaftMessage> for VoteSvc<T> {
                        type Response = super::RaftMessage;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::RaftMessage>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).vote(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = VoteSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => Box::pin(async move {
                    Ok(http::Response::builder()
                        .status(200)
                        .header("grpc-status", "12")
                        .header("content-type", "application/grpc")
                        .body(tonic::body::BoxBody::empty())
                        .unwrap())
                }),
            }
        }
    }
    impl<T: MiniRedisService> Clone for MiniRedisServiceServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self { inner }
        }
    }
    impl<T: MiniRedisService> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone(), self.1.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: MiniRedisService> tonic::transport::NamedService for MiniRedisServiceServer<T> {
        const NAME: &'static str = "mini_redis.MiniRedisService";
    }
}
