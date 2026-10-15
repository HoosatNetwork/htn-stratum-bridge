package gostratum

import (
	"bytes"
	"net"
	"os"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"
)

func spawnClientListener(ctx *StratumContext, connection net.Conn, s *StratumListener) error {
	defer ctx.Disconnect()

	var pending []byte // start of a line whose newline has not arrived yet
	for {
		err := readFromConnection(connection, &pending, func(line string) error {
			event, err := UnmarshalEvent(line)
			if err != nil {
				ctx.Logger.Error("error unmarshalling event", zap.String("raw", line))
				return err
			}
			return s.HandleEvent(ctx, event)
		})
		if errors.Is(err, os.ErrDeadlineExceeded) {
			continue // expected timeout
		}
		if ctx.Err() != nil {
			return ctx.Err() // context cancelled
		}
		if ctx.parentContext.Err() != nil {
			return ctx.parentContext.Err() // parent context cancelled
		}
		if err != nil { // actual error
			ctx.Logger.Debug("error reading from socket", zap.Error(err))
			return err
		}
	}
}

type LineCallback func(line string) error

// maxLineLength is the longest accepted stratum line in bytes, counting its "\r\n" or "\n"
// terminator (after NUL bytes are stripped). It equals the read buffer size, which was the
// longest line that could be parsed before lines could span reads.
const maxLineLength = 8096 * 2

var errLineTooLong = errors.New("stratum line exceeds maximum length")

func readFromConnection(connection net.Conn, pending *[]byte, cb LineCallback) error {
	deadline := time.Now().Add(5 * time.Second).UTC()
	if err := connection.SetReadDeadline(deadline); err != nil {
		return err
	}

	buffer := make([]byte, maxLineLength)
	n, err := connection.Read(buffer)
	if err != nil {
		return errors.Wrapf(err, "error reading from connection")
	}
	// a line can arrive in several reads: data starts with the unterminated part of the previous reads
	data := append(*pending, bytes.ReplaceAll(buffer[:n], []byte("\x00"), nil)...)
	for {
		end := bytes.IndexByte(data, '\n')
		if end < 0 {
			break
		}
		if end+1 > maxLineLength {
			return errLineTooLong
		}
		line := bytes.TrimSuffix(data[:end], []byte("\r"))
		data = data[end+1:]
		if err := cb(string(line)); err != nil {
			return err
		}
	}
	if len(data) >= maxLineLength { // cannot become a line of at most maxLineLength bytes
		return errLineTooLong
	}
	*pending = append([]byte(nil), data...)
	return nil
}

