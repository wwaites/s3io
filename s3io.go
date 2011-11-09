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

func (s3io *S3IO) Run() (err os.Error) {
	s3io.Start = time.UTC().Format(time.RFC3339)
	defer func() {
		s3io.Finish = time.UTC().Format(time.RFC3339)
		meta, err := json.Marshal(s3io)
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
		log.Print("copying stdout...")
		if _, err := io.Copy(tmpout, stdout); err != nil {
			log.Print(err)
		}
		//tmpout.Flush()
		doneout <-true
		log.Print("done copying stdout...")
	}()

	journal, stamped := io.Pipe()
	go func() {
		log.Print("stamping stderr...")
		if err := Stamp(stamped, stderr); err != nil {
			log.Print(err)
		}
		log.Print("done stamping...")
	}()
	go func() {
		log.Print("copying stderr")
		if _, err := io.Copy(tmperr, journal); err != nil {
			log.Print(err)
		}
		//tmperr.Flush()
		doneerr <-true
		log.Print("done copying stderr")
	}()

	log.Print("running command")
	childErr := s3io.command.Run()
	log.Print("command returned")

	log.Print("waiting on capture of output...")
	_ =<-doneout
	log.Print("now waiting on capture of error...")
	_ =<-doneerr
	log.Print("output captured")

	go func() {
		log.Print("saving output")
		outpath := path.Join(s3io.s3path, "stdout")
		s3io.Stdout = s3io.bucket.Region.S3Endpoint + "/" + s3io.bucket.Name + outpath
		err := s3io.Save(tmpout, outpath)
		if err != nil {
			log.Print(err)
		}
		doneout<-true
		log.Print("done saving output")
	}()
	go func() {
		log.Print("saving error")
		errpath := path.Join(s3io.s3path, "stderr")
		s3io.Stderr = s3io.bucket.Region.S3Endpoint + "/" + s3io.bucket.Name + errpath
		err := s3io.Save(tmperr, errpath)
		if err != nil {
			log.Print(err)
		}
		doneerr<-true
		log.Print("done saving error")
	}()

	log.Print("waiting on save of output")
	_ =<-doneout
	log.Print("now waiting on save of error")
	_ =<-doneerr
	log.Print("done waiting")

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
