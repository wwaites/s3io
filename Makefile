include $(GOROOT)/src/Make.inc

TARG=github.com/wwaites/s3io
GOFILES=s3io.go

include $(GOROOT)/src/Make.pkg

format:
	gofmt -w *.go

