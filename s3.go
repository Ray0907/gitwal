package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// Store is the write-ahead log: one S3 bucket, one index object per repo.
type Store struct {
	c      *minio.Client
	bucket string
}

// Entry is one accepted push. Refs lines are "old new refname" as git gave them.
type Entry struct {
	ID   string   `json:"id"`
	Pack bool     `json:"pack"`
	Refs []string `json:"refs"`
}

// Base is a compacted repository snapshot.
type Base struct {
	ID   string            `json:"id"`
	Refs map[string]string `json:"refs"`
}

// Index is the repo's linearized history. Every push appends one Entry via CAS.
type Index struct {
	Base    Base    `json:"base"`
	Entries []Entry `json:"entries"`
}

var (
	errConflict = errors.New("gitwal: concurrent push, retry")
	errLost     = errors.New("gitwal: lost index CAS repeatedly, retry")
)

func newStore(ctx context.Context) (*Store, error) {
	env := func(k, d string) string {
		if v := os.Getenv(k); v != "" {
			return v
		}
		return d
	}
	c, err := minio.New(env("S3_ENDPOINT", "localhost:9000"), &minio.Options{
		Creds:  credentials.NewStaticV4(env("S3_KEY", "minioadmin"), env("S3_SECRET", "minioadmin"), ""),
		Secure: env("S3_SSL", "") == "1",
		Region: "us-east-1",
	})
	if err != nil {
		return nil, err
	}
	s := &Store{c: c, bucket: env("GITWAL_BUCKET", "gitwal")}
	ok, err := c.BucketExists(ctx, s.bucket)
	if err != nil {
		return nil, fmt.Errorf("s3 %s: %w", s.bucket, err)
	}
	if !ok {
		if err := c.MakeBucket(ctx, s.bucket, minio.MakeBucketOptions{}); err != nil {
			// Another node may have created it after BucketExists.
			if ok, _ := c.BucketExists(ctx, s.bucket); !ok {
				return nil, err
			}
		}
	}
	return s, nil
}

func indexKey(repo string) string    { return repo + "/index.json" }
func packKey(repo, id string) string { return repo + "/wal/" + id + ".pack" }
func baseKey(repo, id string) string { return repo + "/base/" + id + ".pack" }

// getIndex does a conditional GET. changed=false means the caller's etag is still current.
// A missing index reads as empty with etag "".
func (s *Store) getIndex(ctx context.Context, repo, etag string) (idx Index, newETag string, changed bool, err error) {
	opts := minio.GetObjectOptions{}
	if etag != "" {
		if err = opts.SetMatchETagExcept(etag); err != nil {
			return
		}
	}
	obj, err := s.c.GetObject(ctx, s.bucket, indexKey(repo), opts)
	if err != nil {
		return
	}
	defer obj.Close()
	data, err := io.ReadAll(obj)
	if err != nil {
		resp := minio.ToErrorResponse(err)
		switch {
		case resp.StatusCode == 304:
			return idx, etag, false, nil
		case resp.Code == "NoSuchKey":
			return Index{}, "", true, nil
		}
		return
	}
	st, err := obj.Stat()
	if err != nil {
		return
	}
	err = json.Unmarshal(data, &idx)
	return idx, st.ETag, true, err
}

// putIndex is the CAS: If-Match on the etag we read, or If-None-Match:* when creating.
func (s *Store) putIndex(ctx context.Context, repo string, idx Index, etag string) (string, error) {
	data, _ := json.Marshal(idx)
	opts := minio.PutObjectOptions{ContentType: "application/json"}
	if etag == "" {
		opts.SetMatchETagExcept("*")
	} else {
		opts.SetMatchETag(etag)
	}
	info, err := s.c.PutObject(ctx, s.bucket, indexKey(repo), bytes.NewReader(data), int64(len(data)), opts)
	if err != nil {
		if minio.ToErrorResponse(err).StatusCode == 412 {
			return "", errConflict
		}
		return "", err
	}
	return info.ETag, nil
}

func (s *Store) putPack(ctx context.Context, repo, id, path string) error {
	_, err := s.c.FPutObject(ctx, s.bucket, packKey(repo, id), path, minio.PutObjectOptions{})
	return err
}

func (s *Store) getPack(ctx context.Context, repo, id string) (io.ReadCloser, error) {
	return s.c.GetObject(ctx, s.bucket, packKey(repo, id), minio.GetObjectOptions{})
}

func (s *Store) putBase(ctx context.Context, repo, id, path string) error {
	_, err := s.c.FPutObject(ctx, s.bucket, baseKey(repo, id), path, minio.PutObjectOptions{})
	return err
}

func (s *Store) getBase(ctx context.Context, repo, id string) (io.ReadCloser, error) {
	return s.c.GetObject(ctx, s.bucket, baseKey(repo, id), minio.GetObjectOptions{})
}

func (s *Store) remove(ctx context.Context, key string) error {
	return s.c.RemoveObject(ctx, s.bucket, key, minio.RemoveObjectOptions{})
}
