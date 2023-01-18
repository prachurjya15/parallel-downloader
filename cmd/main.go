package main

import (
	"flag"
	"log"
	"time"

	"github.com/prachurjya15/parallel-downloader/downloader"
)

func main() {
	start := time.Now()
	ngR := flag.Int64("ngr", 10, "Specify number of go routines to spin.Maximum allowed is 50")
	nChunkSize := flag.Int("chunk_size", 0, "Specify chunk size( in bytes ) to download per go routine. Default value will be fileSize/Number of Go Routines")
	flag.Parse()
	err := downloader.DownloadFile("https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2018-05.parquet", *ngR, *nChunkSize)
	duration := time.Since(start)
	if err != nil {
		log.Fatalf("Download Failed with error: %s \n time taken: %s \n", err, duration)
	} else {
		log.Printf("Successfully Downloaded File in %s!", duration)
	}
}
