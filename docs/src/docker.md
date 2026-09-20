# Docker images and TLS

When you design your Dockerfile make sure you either installed Root CA certificates or use base images that already
include them.
If you don't have certs installed you usually observe the errors such as:

```text
SystemError(FirestoreSystemError { public: FirestoreErrorPublicGenericDetails { code: "GrpcStatus(tonic::transport::Error(Transport, hyper::Error(Connect, Custom { kind: InvalidData, error: InvalidCertificateData(\"invalid peer certificate: UnknownIssuer\") })))" }, message: "GCloud system error: Tonic/gRPC error: transport error" })
```

For example for Debian based images, this usually can be fixed using this package:

```text
RUN apt-get install -y ca-certificates
```

Also, I recommend considering using [Google Distroless images](https://github.com/GoogleContainerTools/distroless) since
they are secure, already include Root CA certs, and are optimised for size.

## TLS features
Cargo provides support for different TLS features for dependencies:
- `tls-roots`: default feature to support native TLS roots
- `tls-webpki-roots`: feature to switch to webpki crate roots
