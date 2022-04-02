module github.com/y-tag/libhdfs5

go 1.18

require (
	github.com/colinmarc/hdfs/v2 v2.1.1
	gopkg.in/jcmturner/gokrb5.v7 v7.3.0
)

require (
	github.com/golang/protobuf v1.1.0 // indirect
	github.com/hashicorp/go-uuid v0.0.0-20180228145832-27454136f036 // indirect
	github.com/jcmturner/gofork v0.0.0-20180107083740-2aebee971930 // indirect
	golang.org/x/crypto v0.0.0-20180723164146-c126467f60eb // indirect
	gopkg.in/jcmturner/aescts.v1 v1.0.1 // indirect
	gopkg.in/jcmturner/dnsutils.v1 v1.0.1 // indirect
	gopkg.in/jcmturner/goidentity.v3 v3.0.0 // indirect
	gopkg.in/jcmturner/rpc.v1 v1.1.0 // indirect
)

replace github.com/colinmarc/hdfs/v2 => github.com/y-tag/hdfs/v2 v2.1.2-0.20200705015456-04cd17acc693
