//go:build !windows

package clientmigrate

import "os"

// replaceFile installs tmp as dst, replacing an existing dst. os.Rename is
// atomic and replaces the destination on Unix-like hosts.
func replaceFile(tmp, dst string) error {
	//nolint:gosec // G703: tmp is an os.CreateTemp file in dst's directory and dst is a fixed infohash name under --qbit-dir
	return os.Rename(tmp, dst)
}
