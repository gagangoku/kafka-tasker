package tasker

import (
	"fmt"
)

func WrapError(err error, wrap string) error {
	if err != nil {
		return fmt.Errorf("%s: %s", wrap, err)
	}
	return nil
}

func noopFn(x ...any) {
	// Does nothing
}
