package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"
)

// func processLine(lockLock *sync.RWMutex, fileLocks map[string]*sync.Mutex, jobs <-chan []string) {

// 	for line := range jobs {

// 		csvFileOutFile, _ := os.OpenFile("out/"+line[0]+".csv", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

// 		writer := csv.NewWriter(csvFileOutFile)

// 		err := writer.Write(line)
// 		checkError("Cannot write to file", err)
// 		writer.Flush()
// 		csvFileOutFile.Close()

// 	}

// }

func main() {

	channels := map[string]chan []string{}
	// uniquevessels := map[string]string{}

	start := time.Now().Local()

	// for w := 1; w <= 10; w++ {
	// 	go processLine(lockLock, fileLocks, jobs)
	// }

	csvFile, _ := os.Open("input/VD_2013_01.txt")

	defer csvFile.Close()
	reader := csv.NewReader(bufio.NewReader(csvFile))
	reader.Comma = '\t'

	doneCount := 0
	intervalCount := 1000000

	doneWriting := make(chan string)

	for {
		line, error := reader.Read()
		if error == io.EOF {
			break
		} else if error != nil {
			log.Fatal(error)
		}

		if _, ok := channels[line[0]]; ok == false {
			channels[line[0]] = make(chan []string)

			go func(channel chan []string) {
				// currentTime := time.Now().Local()

				// hostname, err := os.Hostname()

				f, err := os.Create("out/" + line[0] + ".csv")
				checkError("Failed to open file", err)

				w := bufio.NewWriter(f)

				for {
					j, more := <-channel

					if more {
						w.WriteString(strings.Join(j[:], ",") + "\n")

						err = w.Flush() // Don't forget to flush!
						if err != nil {
							log.Fatal(err)
						}

						doneCount = doneCount + 1

						if doneCount%intervalCount == 0 {
							fmt.Println(doneCount)
							fmt.Println(time.Since(start))
						}
					} else {
						f.Close()
						doneWriting <- "done"
					}
				}

			}(channels[line[0]])

		}

		channels[line[0]] <- line

	}

	<-doneWriting

}

func checkError(message string, err error) {
	if err != nil {
		log.Fatal(message, err)
	}
}
