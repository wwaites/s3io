package main

import (
	"launchpad.net/goamz/aws"
	"launchpad.net/goamz/s3"
	"github.com/wwaites/s3io"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
)

var inbucket, outbucket, inpath, outdir, region string
var regions map[string]aws.Region

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "\n")
		fmt.Fprintf(os.Stderr, "Usage: %s [args] command [cmdargs...]\n\n",
			os.Args[0])
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\n")
		fmt.Fprintf(os.Stderr, "Regions:")
		for k, _ := range regions {
			fmt.Fprintf(os.Stderr, " %s", k)
		}
		fmt.Fprintf(os.Stderr, "\n")
	}
	flag.StringVar(&inbucket, "ib", "", "S3 bucket for standard input")
	flag.StringVar(&inpath, "ip", "", "S3 path for standard input")
	flag.StringVar(&outbucket, "ob", "", "S3 bucket for output (required)")
	flag.StringVar(&outdir, "od", "", "S3 directory within the bucket (required)")
	flag.StringVar(&region, "r", "EUWest", "S3 region")
	regions = map[string]aws.Region {
		"APNortheast": aws.APNortheast,
		"APNorthwest": aws.APSoutheast,
		"EUWest": aws.EUWest,
		"USEast": aws.USEast,
		"USWest": aws.USWest,
	}
}

func main() {
	flag.Parse()
	if len(outbucket) == 0 || len(outdir) == 0 {
		flag.Usage()
		os.Exit(255)
	}

	r, ok := regions[region]
	if !ok {
		flag.Usage()
		fmt.Fprintf(os.Stderr, "Unrecognised region: %s\n\n", region)
		os.Exit(255)
	}

	cmd := flag.Arg(0)
	if len(cmd) == 0 {
		flag.Usage()
		os.Exit(255)
	}
	args := flag.Args()[1:]

	log.SetFlags(log.LstdFlags | log.Lshortfile)

	auth, err := aws.EnvAuth()
	if err != nil {
		log.Fatal(err)
	}

	s3 := s3.New(auth, r)
	outb := s3.Bucket(outbucket)
	s3io, err := s3io.New(outb, outdir, cmd, args...)
	if err != nil {
		log.Fatal(err)
	}

	if len(inbucket) != 0 {
		if len(inpath) == 0 {
			flag.Usage()
			fmt.Fprintf(os.Stderr, "Need input path with input bucket\n\n")
			os.Exit(255)
		}
		stdin, err := s3io.StdinPipe()
		if err != nil {
			log.Fatal(err)
		}
		go func() {
			inb := s3.Bucket(inbucket)
			input, err := inb.GetReader(inpath)
			if err != nil {
				log.Fatal(err)
			}
			if _, err := io.Copy(stdin, input); err != nil {
				log.Fatal(err)
			}
			input.Close()
		}()
	} else {
		s3io.SetStdin(os.Stdin)
	}

	err = s3io.Run()
	if err != nil {
		log.Fatal(err)
	}
}