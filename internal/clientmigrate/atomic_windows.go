package clientmigrate

import (
	"github.com/pkg/errors"
	"golang.org/x/sys/windows"
)

// replaceFile installs tmp as dst, replacing an existing dst. os.Rename
// refuses to overwrite on Windows, so use MoveFileEx with the replace flag.
func replaceFile(tmp, dst string) error {
	tmpPtr, err := windows.UTF16PtrFromString(tmp)
	if err != nil {
		return err
	}
	dstPtr, err := windows.UTF16PtrFromString(dst)
	if err != nil {
		return err
	}
	if err := windows.MoveFileEx(tmpPtr, dstPtr, windows.MOVEFILE_REPLACE_EXISTING); err != nil {
		return errors.WithStack(err)
	}
	return nil
}
