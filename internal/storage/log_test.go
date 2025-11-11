package storage

import (
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLog(t *testing.T) {
	file, err := os.OpenFile("TestLog.log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0755)
	require.NoError(t, err)
	defer file.Close()

	logger, err := NewFileTransctionLogger(file.Name())
	require.NoError(t, err)
	logger.Run()

	logTest := make(map[int]string)

	for i := 0; i <= 3; i++ {
		logTest[i] = "HeyYou"
		logger.WritePut(strconv.Itoa(i), logTest[i])
	}

	time.Sleep(100 * time.Millisecond)

	logger.Close()

	file, err = os.Open(file.Name())
	require.NoError(t, err)
	defer file.Close()

	readerLogger, err := NewFileTransctionLogger(file.Name())
	require.NoError(t, err)
	defer readerLogger.Close()

	events, errs := readerLogger.ReadEvents()
	for i := 0; i <= 3; i++ {
		select {
		case e, ok := <-events:
			require.True(t, ok)
			require.Equal(t, EventPut, e.EventType)
			require.Equal(t, strconv.Itoa(i), e.Key)
			require.Equal(t, "HeyYou", e.Value)
			require.Equal(t, uint64(i+1), e.Sequence)
		case <-errs:
			t.Fatalf("Unexpected Error")
			//fmt.Print(err)
		}
	}

}
