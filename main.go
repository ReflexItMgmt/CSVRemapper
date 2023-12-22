package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
)

type MapFile struct {
	Name    string            `json:"name"`
	Records [][]string        `json:"generated_records,omitempty"`      // generated at runtime
	Columns map[string]string `json:"columns"`                          // [original col][new col]
	PosMap  map[string]int    `json:"generated_pos_original,omitempty"` // [original pos][new pos] generated at runtime
}

type Mappings struct {
	Name    string         `json:"name"`
	Files   []MapFile      `json:"files,omitempty"`               // mappings for old col to new col
	Records [][]string     `json:"generated_records,omitempty"`   // generated at runtime
	PosMap  map[string]int `json:"generated_pos_final,omitempty"` // [final col][pos] generated at runtime
	PosMax  int            `json:"generated_pos_max,omitempty"`   // [max len for col] generated at runtime
}

func main() {
	m := Mappings{
		Name: "combined.csv",
		Files: []MapFile{{
			Name: "gsuite-tenant-users.csv",
			Columns: map[string]string{
				"Firstname": "GS_FIRST",
				"Lastname":  "GS_LAST",
				"Email":     "GS_EMAIL",
			},
		}},
	}

	m.Records = readCsvRecords(m.Name)
	// generate final positions
	m.PosMap = make(map[string]int)
	for i, r := range m.Records[0] {
		m.PosMap[r] = i // [col name][new pos]
	}

	// generate mappings for each file to the final file, and their positions
	for _, mapFile := range m.Files {
		mapFile.PosMap = make(map[string]int)

		mapFile.Records = readCsvRecords(mapFile.Name)
		for ogColPos, ogCol := range mapFile.Records[0] {
			if newCol, ok := mapFile.Columns[ogCol]; ok {
				if newColPos, ok := m.PosMap[newCol]; ok {
					log.Printf("Mapped %s -> %s (%v -> %v)\n", ogCol, newCol, ogColPos, newColPos)
					mapFile.PosMap[strconv.Itoa(ogColPos)] = newColPos

					// generate maximum number of columns required for new file
					if newColPos > m.PosMax {
						m.PosMax = newColPos
					}
				}
			}
		}
		//log.Printf("%s\n", file)

		// map rows 66 to 105 as an example

		for ogRowPos, ogRow := range mapFile.Records {
			// Print header for this row
			if ogRowPos == 0 {
				log.Printf("%s\n", strings.Join(ogRow, ", "))
			}

			if ogRowPos != 66 {
				continue
			}

			// Print current row, ask for new location to insert
			log.Printf("%s\n", strings.Join(ogRow, ", "))

			newRowPos := askChoiceAllowNull("new row location (enter to skip)")
			if newRowPos == -1 {
				continue
			}

			if newRowPos >= len(m.Records) {
				log.Printf("%v is greater than the allowed %v!\n", newRowPos, len(m.Records))
				continue
			}

			log.Printf("%s\n", strings.Join(m.Records[newRowPos], ", "))
			if !askConfirm("confirm insert?", true) {
				continue
			}

			// Before inserting new cols, ensure this row is big enough
			newRow := make([]string, m.PosMax+1)
			for ogRecordPos, ogRecord := range m.Records[newRowPos] {
				newRow[ogRecordPos] = ogRecord
			}

			// insert new columns
			for ogColPos, ogCol := range mapFile.Records[ogRowPos] {
				newColPosMap, ok := mapFile.PosMap[strconv.Itoa(ogColPos)]

				if ok {
					log.Printf("Found matching row: %v / %v / %s / %v\n", newColPosMap, ok, ogCol, ogColPos)
					newRow[newColPosMap] = ogCol  // set [new row][new col] to old record
					m.Records[newRowPos] = newRow // set records[new pos] to new modified row

					log.Printf("m.Records[newRowPos][newColPosMap]: %s\n", m.Records[newRowPos][newColPosMap])
				}
			}
			log.Printf("m.Records[newRowPos]: %s\n", m.Records[newRowPos])

			//for oldColPos, newColPos := range mapFile.PosMap {
			//	log.Printf("oldColPos: %s / newColPos: %v\n", oldColPos, newColPos)
			//	log.Printf("len(mapFile.Records[newRowPos]): %v\n", len(mapFile.Records[newRowPos]))
			//	mapFile.Records[newColPos][newRowPos] = ogRow[ogRowPos][oldColPos]
			//	log.Printf("mapFile.Records[newRowPos][newColPos]: %s\n", mapFile.Records[newColPos][newRowPos])
			//}
			//log.Printf()
		}
	}
	//log.Printf("%s\n", m)

	//chooseAndPrintColumns()

	//for _, r := range records {
	//	fmt.Printf("%s\n", r[0]) // print all from first column
	//}

}

//func insertValue(records [][]string, column, row int) [][]string {
//	if len(records) < column {
//
//	}
//}

func chooseFile() (string, [][]string) {
	files, err := ioutil.ReadDir("./csv/")
	if err != nil {
		log.Println("oops, couldn't read the directory :(")
		return "", nil
	}

	csvFiles := make([]os.FileInfo, 0)
	log.Println("here are the files:")
	csvFileIndex := 0

	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".csv") {
			csvFiles = append(csvFiles, file)
			log.Printf("%d: %s\n", csvFileIndex, file.Name())
			csvFileIndex++
		}
	}

	inputInt := askChoice("pick a file")
	records := readCsvRecords(csvFiles[inputInt].Name())

	return csvFiles[inputInt].Name(), records
}

func readCsvRecords(name string) [][]string {
	file, err := os.Open("./csv/" + name)
	if err != nil {
		log.Println("oops, couldn't open the file :(")
		return nil
	}

	csvReader := csv.NewReader(file)
	csvReader.FieldsPerRecord = -1    // https://stackoverflow.com/a/61337003
	records, _ := csvReader.ReadAll() // [row][column]record

	return records
}

func chooseAndPrintColumns() {
	file0, records0 := chooseFile()
	file1, records1 := chooseFile()
	if records0 == nil || records1 == nil {
		log.Panicf("records were nil")
	}

	for i, columnName := range records0[0] {
		log.Printf("%s %d: %s\n", file0, i, columnName)
	}

	for i, columnName := range records1[0] {
		log.Printf("%s %d: %s\n", file1, i, columnName)
	}
}

func askChoice(msg string) int {
	log.Print(msg + ": ")
	reader := bufio.NewReader(os.Stdin)
	input, _ := reader.ReadString('\n')
	inputInt, _ := strconv.Atoi(strings.Split(input, "\n")[0])
	return inputInt
}

func askChoiceAllowNull(msg string) int {
	log.Print(msg + ": ")
	reader := bufio.NewReader(os.Stdin)
	input, _ := reader.ReadString('\n')
	if len(strings.Split(input, "\n")[0]) == 0 {
		return -1
	}

	if inputInt, err := strconv.Atoi(strings.Split(input, "\n")[0]); err != nil {
		return -1
	} else {
		return inputInt
	}
}

func askConfirm(msg string, yesDefault bool) bool {
	defaultMsg := "confirm"
	if len(msg) != 0 {
		defaultMsg = msg
	}

	confirmMsg := fmt.Sprintf("%s [y/N]: ", defaultMsg)
	if yesDefault {
		confirmMsg = fmt.Sprintf("%s [Y/n]: ", defaultMsg)
	}
	log.Print(confirmMsg)

	reader := bufio.NewReader(os.Stdin)
	input, _ := reader.ReadString('\n')
	input = strings.ToLower(strings.Split(input, "\n")[0])[:len(input)-1]
	// split to get first line of input, trim to first character, lowercase it

	switch input {
	case "n":
		return false
	case "y":
		return true
	default:
		return yesDefault
	}
}
