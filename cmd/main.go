package main

import (
	"net/http"

	"github.com/prachurjya15/parallel-download/downloader"
)

func main() {
	// Define a server
	handler := http.NewServeMux()
	// Define the route
	handler.HandleFunc("/download", handleDownloadFile)
	http.ListenAndServe(":8080", handler)
}

func handleDownloadFile(w http.ResponseWriter, r *http.Request) {
	downloader.DownloadFile("https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2018-05.parquet")
}
