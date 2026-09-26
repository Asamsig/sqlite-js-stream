// Command golden generates LTX fixture files using Go's `superfly/ltx`
// implementation.
//
// The generated files in `testdata/go/` are committed and decoded by
// `test/ltx/go-interop.test.ts` to verify that our TypeScript decoder is
// byte-compatible with the canonical Go LTX writer (in particular the LZ4 block
// codec, and the header/page-index/trailer layout).
//
// Because `lz4js` produces valid-but-not-byte-identical LZ4 blocks to Go's
// `pierrec/lz4/v4`, the *encoding* bytes differ; this generator only proves the
// *decoding* direction. CI regenerates these files and fails if they drift.
//
// The logical content here mirrors `test/ltx/golden.test.ts` so the TS side can
// cross-check the header, pages and trailer.
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"github.com/superfly/ltx"
)

const pageSize = 512

func makePage(seed uint32) []byte {
	b := make([]byte, pageSize)
	for i := 0; i < pageSize; i++ {
		b[i] = byte((seed + uint32(i%7)) & 0xff)
	}
	return b
}

// postApplyChecksum reproduces the rolling post-apply checksum our decoder (and
// litestream) derives from the pages: a CRC-64 over each page, XORed together and
// masked with ChecksumFlag. Must be set on snapshot files so the decoder can
// validate it.
func postApplyChecksum(pgnos []uint32) ltx.Checksum {
	chk := ltx.ChecksumFlag
	for _, pgno := range pgnos {
		chk = ltx.ChecksumFlag | (chk ^ ltx.ChecksumPage(pgno, makePage(pgno)))
	}
	return chk
}

func writeLTX(path string, hdr ltx.Header, pgnos []uint32) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	enc, err := ltx.NewEncoder(f)
	if err != nil {
		return err
	}
	if err := enc.EncodeHeader(hdr); err != nil {
		return err
	}
	for _, pgno := range pgnos {
		if err := enc.EncodePage(ltx.PageHeader{Pgno: pgno}, makePage(pgno)); err != nil {
			return err
		}
	}
	enc.SetPostApplyChecksum(postApplyChecksum(pgnos))
	return enc.Close()
}

func main() {
	// Resolve the repo root from this file's location (tools/golden -> repo root)
	// so the output path is stable regardless of the working directory.
	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(thisFile)))
	dir := filepath.Join(repoRoot, "testdata", "go")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, "mkdir:", err)
		os.Exit(1)
	}

	// Sparse, non-snapshot incremental file (pages are not contiguous).
	sparse := ltx.Header{
		Version:          3,
		PageSize:         pageSize,
		Commit:           16,
		MinTXID:          2,
		MaxTXID:          3,
		Timestamp:        1000,
		PreApplyChecksum: ltx.ChecksumFlag | 1,
	}
	if err := writeLTX(filepath.Join(dir, "sparse-non-snapshot.ltx"), sparse, []uint32{2, 5, 9, 16}); err != nil {
		fmt.Fprintln(os.Stderr, "sparse:", err)
		os.Exit(1)
	}

	// Full snapshot (contiguous pages 1..N, min=1).
	snapshot := ltx.Header{
		Version:          3,
		PageSize:         pageSize,
		Commit:           6,
		MinTXID:          1,
		MaxTXID:          4,
		Timestamp:        1000,
		PreApplyChecksum: 0,
	}
	if err := writeLTX(filepath.Join(dir, "snapshot.ltx"), snapshot, []uint32{1, 2, 3, 4, 5, 6}); err != nil {
		fmt.Fprintln(os.Stderr, "snapshot:", err)
		os.Exit(1)
	}

	fmt.Println("wrote", filepath.Join(dir, "sparse-non-snapshot.ltx"), "and", filepath.Join(dir, "snapshot.ltx"))
}
