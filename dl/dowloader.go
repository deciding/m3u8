package dl

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/oopsguy/m3u8/parse"
	"github.com/oopsguy/m3u8/tool"
)

const (
	tsExt            = ".ts"
	tsFolderName     = "ts"
	mergeTSFilename  = "main.ts"
	tsTempFileSuffix = "_tmp"
	progressWidth    = 40
)

type Downloader struct {
	lock     sync.Mutex
	queue    []int
	folder   string
	tsFolder string
	finish   int32
	segLen   int

	result *parse.Result
}

// NewTask returns a Task instance
func NewTask(output string, url string) (*Downloader, error) {
	//m3u8_key_re := regexp.MustCompile(`"(.*)/(.*?\.key)"`)
	//m3u8_key_re := regexp.MustCompile(`"(.*\.key)"`)
	fileID := strings.TrimSuffix(filepath.Base(url), filepath.Ext(url)) + String(10)
	result, lines, err := parse.FromURL(url)
	if err != nil {
		return nil, err
	}
	var folder string
	// If no output folder specified, use current directory
	if output == "" {
		current, err := tool.CurrentDir()
		if err != nil {
			return nil, err
		}
		folder = filepath.Join(current, output)
	} else {
		folder = output
	}
	m3u8_file := filepath.Join(folder, fmt.Sprintf("%s.m3u8", fileID))
	m3u8_org_file := filepath.Join(folder, fmt.Sprintf("%s_org.m3u8", fileID))
	//ts_folder := filepath.Join(folder, fileID)
	m3u8_key_file := filepath.Join(folder, fmt.Sprintf("%s.key", fileID))
	m3u8_f, err := os.Create(m3u8_file)
	if err != nil {
		return nil, err
	}
	m3u8_fo, err := os.Create(m3u8_org_file)
	if err != nil {
		return nil, err
	}
	m3u8_k, err := os.Create(m3u8_key_file)
	if err != nil {
		return nil, err
	}
	defer m3u8_f.Close()
	defer m3u8_fo.Close()
	defer m3u8_k.Close()
	for _, line := range lines {
		_, err := m3u8_fo.WriteString(fmt.Sprintf("%s\n", line))
		if err != nil {
			return nil, err
		}
		if strings.HasSuffix(line, ".ts") {
			base := filepath.Base(line)
			//ts_abs_path, err := filepath.Abs(ts_folder)
			//if err != nil {
			//	return nil, err
			//}
			//new_ts_path := filepath.Join(ts_abs_path, base)
			new_ts_path := filepath.Join(fileID, base)
			line = new_ts_path
		}
		if strings.HasSuffix(line, ".key\"") {
			//abs_dir, err := filepath.Abs(folder)
			//if err != nil {
			//	return nil, err
			//}
			//line = m3u8_key_re.ReplaceAllString(line, fmt.Sprintf("\"%s/%s.key\"", abs_dir, fileID))
			//line = m3u8_key_re.ReplaceAllString(line, fmt.Sprintf("\"%s.key\"", fileID))
			continue
		}
		_, err = m3u8_f.WriteString(fmt.Sprintf("%s\n", line))
		if err != nil {
			return nil, err
		}
	}
	_, err = m3u8_k.WriteString(fmt.Sprintf("%s\n", result.Keys[1]))
	if err != nil {
		return nil, err
	}
	m3u8_f.Sync()
	m3u8_k.Sync()

	if err := os.MkdirAll(folder, os.ModePerm); err != nil {
		return nil, fmt.Errorf("create storage folder failed: %s", err.Error())
	}
	//tsFolder := filepath.Join(folder, tsFolderName)
	tsFolder := filepath.Join(folder, fileID)
	if err := os.MkdirAll(tsFolder, os.ModePerm); err != nil {
		return nil, fmt.Errorf("create ts folder '[%s]' failed: %s", tsFolder, err.Error())
	}
	d := &Downloader{
		folder:   folder,
		tsFolder: tsFolder,
		result:   result,
	}
	d.segLen = len(result.M3u8.Segments)
	d.queue = genSlice(d.segLen)
	return d, nil
}

// Start runs downloader
func (d *Downloader) Start(concurrency int) error {
	var wg sync.WaitGroup
	// struct{} zero size
	limitChan := make(chan struct{}, concurrency)
	for {
		tsIdx, end, err := d.next()
		if err != nil {
			if end {
				break
			}
			continue
		}
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			if err := d.download(idx); err != nil {
				// Back into the queue, retry request
				fmt.Printf("[failed] %s\n", err.Error())
				if err := d.back(idx); err != nil {
					fmt.Printf(err.Error())
				}
			}
			<-limitChan
		}(tsIdx)
		limitChan <- struct{}{}
	}
	wg.Wait()
	// We don't need to merge and delete the ts, just provide these segments to the client
	//if err := d.merge(); err != nil {
	//	return err
	//}
	return nil
}

func (d *Downloader) download(segIndex int) error {
	//tsFilename := tsFilename(segIndex)
	tsUrl := d.tsURL(segIndex)
	tsFilename := filepath.Base(tsUrl)
	b, e := tool.Get(tsUrl)
	if e != nil {
		return fmt.Errorf("request %s, %s", tsUrl, e.Error())
	}
	//noinspection GoUnhandledErrorResult
	defer b.Close()
	fPath := filepath.Join(d.tsFolder, tsFilename)
	fTemp := fPath + tsTempFileSuffix
	f, err := os.Create(fTemp)
	if err != nil {
		return fmt.Errorf("create file: %s, %s", tsFilename, err.Error())
	}
	bytes, err := ioutil.ReadAll(b)
	if err != nil {
		return fmt.Errorf("read bytes: %s, %s", tsUrl, err.Error())
	}
	sf := d.result.M3u8.Segments[segIndex]
	if sf == nil {
		return fmt.Errorf("invalid segment index: %d", segIndex)
	}
	key, ok := d.result.Keys[sf.KeyIndex]
	if ok && key != "" {
		bytes, err = tool.AES128Decrypt(bytes, []byte(key),
			[]byte(d.result.M3u8.Keys[sf.KeyIndex].IV))
		if err != nil {
			return fmt.Errorf("decryt: %s, %s", tsUrl, err.Error())
		}
	}
	// https://en.wikipedia.org/wiki/MPEG_transport_stream
	// Some TS files do not start with SyncByte 0x47, they can not be played after merging,
	// Need to remove the bytes before the SyncByte 0x47(71).
	syncByte := uint8(71) //0x47
	bLen := len(bytes)
	for j := 0; j < bLen; j++ {
		if bytes[j] == syncByte {
			bytes = bytes[j:]
			break
		}
	}
	w := bufio.NewWriter(f)
	if _, err := w.Write(bytes); err != nil {
		return fmt.Errorf("write to %s: %s", fTemp, err.Error())
	}
	// Release file resource to rename file
	_ = f.Close()
	if err = os.Rename(fTemp, fPath); err != nil {
		return err
	}
	// Maybe it will be safer in this way...
	atomic.AddInt32(&d.finish, 1)
	//tool.DrawProgressBar("Downloading", float32(d.finish)/float32(d.segLen), progressWidth)
	fmt.Printf("[download %6.2f%%] %s\n", float32(d.finish)/float32(d.segLen)*100, tsUrl)
	return nil
}

func (d *Downloader) next() (segIndex int, end bool, err error) {
	d.lock.Lock()
	defer d.lock.Unlock()
	if len(d.queue) == 0 {
		err = fmt.Errorf("queue empty")
		if d.finish == int32(d.segLen) {
			end = true
			return
		}
		// Some segment indexes are still running.
		end = false
		return
	}
	segIndex = d.queue[0]
	d.queue = d.queue[1:]
	return
}

func (d *Downloader) back(segIndex int) error {
	d.lock.Lock()
	defer d.lock.Unlock()
	if sf := d.result.M3u8.Segments[segIndex]; sf == nil {
		return fmt.Errorf("invalid segment index: %d", segIndex)
	}
	d.queue = append(d.queue, segIndex)
	return nil
}

func (d *Downloader) merge() error {
	// In fact, the number of downloaded segments should be equal to number of m3u8 segments
	missingCount := 0
	for idx := 0; idx < d.segLen; idx++ {
		tsFilename := tsFilename(idx)
		f := filepath.Join(d.tsFolder, tsFilename)
		if _, err := os.Stat(f); err != nil {
			missingCount++
		}
	}
	if missingCount > 0 {
		fmt.Printf("[warning] %d files missing\n", missingCount)
	}

	// Create a TS file for merging, all segment files will be written to this file.
	mFilePath := filepath.Join(d.folder, mergeTSFilename)
	mFile, err := os.Create(mFilePath)
	if err != nil {
		return fmt.Errorf("create main TS file failed：%s", err.Error())
	}
	//noinspection GoUnhandledErrorResult
	defer mFile.Close()

	writer := bufio.NewWriter(mFile)
	mergedCount := 0
	for segIndex := 0; segIndex < d.segLen; segIndex++ {
		tsFilename := tsFilename(segIndex)
		bytes, err := ioutil.ReadFile(filepath.Join(d.tsFolder, tsFilename))
		_, err = writer.Write(bytes)
		if err != nil {
			continue
		}
		mergedCount++
		tool.DrawProgressBar("merge",
			float32(mergedCount)/float32(d.segLen), progressWidth)
	}
	_ = writer.Flush()
	// Remove `ts` folder
	_ = os.RemoveAll(d.tsFolder)

	if mergedCount != d.segLen {
		fmt.Printf("[warning] \n%d files merge failed", d.segLen-mergedCount)
	}

	fmt.Printf("\n[output] %s\n", mFilePath)

	return nil
}

func (d *Downloader) tsURL(segIndex int) string {
	seg := d.result.M3u8.Segments[segIndex]
	return tool.ResolveURL(d.result.URL, seg.URI)
}

func tsFilename(ts int) string {
	return strconv.Itoa(ts) + tsExt
}

func genSlice(len int) []int {
	s := make([]int, 0)
	for i := 0; i < len; i++ {
		s = append(s, i)
	}
	return s
}
