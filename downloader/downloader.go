package downloader

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"sync"
)

var mutex sync.Mutex

func DownloadFile(url string) error {
	// Send a Http Head Request
	fileName := "new.parquet"
	response, err := http.Head(url)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	cLen := response.ContentLength
	srcMD5 := response.Header.Get("Etag")
	// Create a empty file with the cLen provided
	err = createEmptyFile("new.parquet", cLen)
	if err != nil {
		log.Printf("create new file failed with : %s \n", err)
		return err
	}
	// Spin GoRoutines to FETCH and send to a channel
	// Collect each channel data and write to the newFile
	var wg sync.WaitGroup
	nGr := int64(10)
	wg.Add(int(nGr))
	chunk_size := cLen / nGr
	for i := int64(0); i < nGr; i++ {
		offset := chunk_size * i
		if i == nGr-1 {
			go downloadAndWrite(url, offset, cLen-offset, fileName, &wg)
		} else {
			go downloadAndWrite(url, offset, chunk_size, fileName, &wg)
		}
	}
	wg.Wait()

	// Once Done calculate the signature of new file and compare with old and return
	destMD5, err := calculateMD5(fileName)
	log.Printf("SRC MD5: %s \n", srcMD5)
	log.Printf("Dest MD5: %s \n", destMD5)
	if destMD5 != srcMD5 {
		log.Fatalf("Mismatch occured exiting")
	}
	if err != nil {
		log.Fatalf("Error in calculating MD5: %s \n", err)
	}

	log.Printf("%#v \n", srcMD5)
	return nil
}

func createEmptyFile(path string, size int64) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	file.Seek(size-1, os.SEEK_SET)
	file.Write([]byte{0})
	return nil
}

func downloadAndWrite(url string, offset int64, chunk_size int64, file string, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Printf("################Downloading################")
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	req.Header.Add("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+chunk_size))
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatalf("%#v \n", err)
	}
	log.Printf("%#v \n", res.Body)
	err = writeToFile(res.Body, file, offset)
	if err != nil {
		log.Fatalf("Erroring writing from go routine. \n %s \n", err)
	}
	log.Printf("################Done downloading chunk from (%d - %d)################", offset, offset+chunk_size)
}

func writeToFile(data io.ReadCloser, filename string, offset int64) error {
	mutex.Lock()
	defer mutex.Unlock()
	fs, err := os.OpenFile(filename, os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("error opening file : \n %s \n", err)
	}
	defer fs.Close()
	bData, err := ioutil.ReadAll(data)
	if err != nil {
		return fmt.Errorf("error Reading the data :  \n %s \n", err)
	}
	_, err = fs.WriteAt(bData, offset)
	if err != nil {
		return fmt.Errorf("error Writing the data : \n %s \n", err)
	}
	return nil
}

func calculateMD5(filename string) (string, error) {
	fs, err := os.Open(filename)
	if err != nil {
		return "", fmt.Errorf("Error Opening File %s \n", err)
	}
	h := md5.New()
	io.Copy(h, fs)
	md5Val := h.Sum(nil)
	return hex.EncodeToString(md5Val), nil
}
