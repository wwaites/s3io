package s3io

import (
	"launchpad.net/goamz/s3"
	"bufio"
	"exec"
	"io"
	"io/ioutil"
	"json"
	"log"
	"os"
	"path"
	"time"
)

type S3IO struct {
	bucket *s3.Bucket
	s3path string
	command *exec.Cmd
	log *log.Logger
	Meta string `json:"uri"`
	Program string `json:"program"`
	Arguments []string `json:"arguments"`
	Start string `json:"start"`
	Finish string `json:"finish"`
	Stdout string `json:"stdout"`
	Stderr string `json:"stderr"`
}

func New(bucket *s3.Bucket, s3path, prog string, args... string) (s3io *S3IO, err os.Error) {
	s := S3IO{bucket: bucket, s3path: s3path, Program: prog, Arguments: args}
	s.command = exec.Command(prog, args...)
	s3io = &s
	return
}

func (s3io *S3IO) StdinPipe() (stdin io.WriteCloser, err os.Error) {
	stdin, err = s3io.command.StdinPipe()
	return
}

func (s3io *S3IO) SetStdin(stdin io.Reader) {
	s3io.command.Stdin = stdin
}

func (s3io *S3IO) vtemp() (tempfile *os.File, err os.Error) {
	tempfile, err = ioutil.TempFile(os.TempDir(), "s3io.")
	if err != nil {
		return
	}
	if err = os.RemoveAll(tempfile.Name()); err != nil {
		tempfile.Close()
	}
	return
}

func (s3io *S3IO) Run() (meta []byte, err os.Error) {
	s3io.Start = time.UTC().Format(time.RFC3339)
	defer func() {
		s3io.Finish = time.UTC().Format(time.RFC3339)
		s3io.Meta = s3io.bucket.Region.S3Endpoint + "/" + s3io.bucket.Name + s3io.s3path
		meta, err = json.Marshal(s3io)
		if err != nil {
			log.Print(err)
			return
		}
		err = s3io.bucket.Put(s3io.s3path, meta, "text/javascript", s3.PublicRead)
		if err != nil {
			log.Print(err)
		}
	}()

	stdout, err := s3io.command.StdoutPipe()
	if err != nil {
		return
	}
	stderr, err := s3io.command.StderrPipe()
	if err != nil { 
		return
	}
	tmpout, err := s3io.vtemp()
	if err != nil {
		return
	}
	tmperr, err := s3io.vtemp()
	if err != nil {
		return
	}

	doneout := make(chan bool)
	doneerr := make(chan bool)

	go func() {
		if _, err := io.Copy(tmpout, stdout); err != nil {
			log.Print(err)
		}
		//tmpout.Flush()
		doneout <-true
	}()

	journal, stamped := io.Pipe()
	go func() {
		if err := Stamp(stamped, stderr); err != nil {
			log.Print(err)
		}
	}()
	go func() {
		if _, err := io.Copy(tmperr, journal); err != nil {
			log.Print(err)
		}
		//tmperr.Flush()
		doneerr <-true
	}()

	childErr := s3io.command.Run()

	_ =<-doneout
	_ =<-doneerr

	go func() {
		outpath := path.Join(s3io.s3path, "stdout")
		s3io.Stdout = s3io.bucket.Region.S3Endpoint + "/" + s3io.bucket.Name + outpath
		err := s3io.Save(tmpout, outpath)
		if err != nil {
			log.Print(err)
		}
		doneout<-true
	}()
	go func() {
		errpath := path.Join(s3io.s3path, "stderr")
		s3io.Stderr = s3io.bucket.Region.S3Endpoint + "/" + s3io.bucket.Name + errpath
		err := s3io.Save(tmperr, errpath)
		if err != nil {
			log.Print(err)
		}
		doneerr<-true
	}()

	_ =<-doneout
	_ =<-doneerr

	err = childErr
	return
}

func (s3io *S3IO) Save(file *os.File, path string) (err os.Error) {
	size, err := file.Seek(0, 1)
	if size == 0 || err != nil { // aws doesn't support empty files
		return
	}
	_, err = file.Seek(0, 0)
	if err != nil {
		return
	}
	err = s3io.bucket.PutReader(path, file, size,
		"text/plain", s3.PublicRead) //s3.BucketOwnerFull)
	return
}

func Stamp(output io.WriteCloser, input io.Reader) (err os.Error) {
	lines := bufio.NewReader(input)
	line := make([]byte, 0, 256)
	for {
		data, pfx, err := lines.ReadLine()
		if err != nil {
			break
		}
		line = append(line, data...)
		if pfx {
			continue
		}
		now := time.UTC()
		_, err = output.Write([]byte(now.Format(time.RFC3339) + " "))
		if err != nil {
			break
		}
		if _, err = output.Write(line); err != nil {
			break
		}
		if _, err = output.Write([]byte("\n")); err != nil {
			break
		}
		line = make([]byte, 0, 256)
	}
	output.Close()
	return
}
