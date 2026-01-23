# Stupid Simple S3

Stupid Simple S3 is a simple S3 service designed to be efficient and performant on a single server with a single drive. No redundancy. No replication. Just operations to write, read and delete objects.

- All configuration is done using environment variables.
- AWSv4 signatures are supported from the client.
- Multi part uploads are supported.


Scope:
- Only the most basic elements of the S3 specification is supported.
- Implementation is in Golang.
- HTTPS is not supported, as it is provided by Varnish that will sit in front of the service.
- Use few external dependencies.
