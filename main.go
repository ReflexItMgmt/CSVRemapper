package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/lithammer/fuzzysearch/fuzzy"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	fileMode = os.FileMode(0644)
)

type MapFile struct {
	Name        string            `json:"name"`
	Records     [][]string        `json:"generated_records,omitempty"`      // generated at runtime
	Columns     map[string]string `json:"columns"`                          // [original col][new col]
	FuzzColumns map[string]string `json:"fuzz_columns,omitempty"`           // [original col][new col] try to suggest matches
	FuzzPosMap  map[string]int    `json:"fuzz_pos_map,omitempty"`           // [original pos][new pos] generated at runtime
	PosMap      map[string]int    `json:"generated_pos_original,omitempty"` // [original pos][new pos] generated at runtime
}

type Mappings struct {
	Name      string                    `json:"name"`
	Files     []MapFile                 `json:"files,omitempty"`                // mappings for old col to new col
	Records   [][]string                `json:"generated_records,omitempty"`    // generated at runtime
	RecordMap map[string]map[string]int `json:"generated_record_map,omitempty"` // [map file][old row][new row] generated at runtime
	PosMap    map[string]int            `json:"generated_pos_final,omitempty"`  // [final col][pos] generated at runtime
	PosMax    int                       `json:"generated_pos_max,omitempty"`    // [max len for col] generated at runtime
}

func main() {
	m := *readMappings()

	// load initial records from csv, we rely on the json from here on out
	if m.Records == nil || len(m.Records) == 0 {
		m.Records = readCsvRecords(m.Name)
	}

	if m.RecordMap == nil {
		m.RecordMap = make(map[string]map[string]int)
	}

	// generate final positions
	m.PosMap = make(map[string]int)
	for i, r := range m.Records[0] {
		m.PosMap[r] = i // [col name][new pos]
	}

	// generate mappings for each file to the final file, and their positions
	for _, mapFile := range m.Files {
		if m.RecordMap[mapFile.Name] == nil {
			m.RecordMap[mapFile.Name] = make(map[string]int)
		}

		// generate mapped positions for columns
		mapFile.PosMap = make(map[string]int)
		mapFile.Records = readCsvRecords(mapFile.Name)
		for ogColPos, ogCol := range mapFile.Records[0] {
			if newCol, ok := mapFile.Columns[ogCol]; ok {
				if newColPos, ok := m.PosMap[newCol]; ok {
					log.Printf("%s: Mapped %s -> %s (%v -> %v)\n", mapFile.Name, ogCol, newCol, ogColPos, newColPos)
					mapFile.PosMap[strconv.Itoa(ogColPos)] = newColPos

					// generate maximum number of columns required for new file
					if newColPos > m.PosMax {
						m.PosMax = newColPos
					}
				}
			}
		}

		// generate mapped positions for fuzzy suggest
		mapFile.FuzzPosMap = make(map[string]int)
		for ogColPos, ogCol := range mapFile.Records[0] {
			if newCol, ok := mapFile.FuzzColumns[ogCol]; ok {
				if newColPos, ok := m.PosMap[newCol]; ok {
					log.Printf("%s: Mapped fuzzy suggest %s -> %s (%v -> %v)", mapFile.Name, ogCol, newCol, ogColPos, newColPos)
					mapFile.FuzzPosMap[strconv.Itoa(ogColPos)] = newColPos
				}
			}
		}

		log.Printf("INFO: When choosing a row to insert, you can press enter to skip, -2 to go back, -3 for new row, -4 to skip rest and save file")

		// show progress, ask to skip this file
		log.Printf("%.2f%% records from %s have been imported, there are %v records missing",
			100*float64(len(m.RecordMap[mapFile.Name]))/float64(len(mapFile.Records)-1),
			mapFile.Name,
			len(mapFile.Records)-len(m.RecordMap[mapFile.Name])-1,
		)

		if askConfirm("print missing records?", false) {
			for oldRowPos, oldRow := range mapFile.Records {
				if oldRowPos == 0 {
					continue
				}

				if _, ok := m.RecordMap[mapFile.Name][strconv.Itoa(oldRowPos)]; !ok {
					log.Printf("MISSING: %s\n", strings.Join(oldRow, ", "))
				}
			}
		}

		if askConfirm("skip this file?", false) {
			continue
		}

		skipModified := askConfirm("skip already added records?", true)

		// Loop through all records and ask to import each
		for i := 0; i < len(mapFile.Records); i++ {
			// Get row
			ogRowPos := i
			ogRow := mapFile.Records[ogRowPos]

			// Print header for this row and skip
			if ogRowPos == 0 {
				log.Printf("HEADER:   %s\n", strings.Join(ogRow, ", "))
				continue
			}

			// Show message if skipping row
			if skipModified {
				if _, ok := m.RecordMap[mapFile.Name][strconv.Itoa(ogRowPos)]; ok {
					log.Printf("SKIPPED:  %s\n", strings.Join(ogRow, ", "))
					continue
				}
			}

			// Print current row, ask for new location to insert
			if ogRowPos != 0 {
				log.Printf("EDITING:  %s\n", strings.Join(ogRow, ", "))
			}

			// generate fuzzy suggestions and print them
			for ogColFuzz, newColFuzz := range mapFile.FuzzPosMap {
				suggestions := make(fuzzy.Ranks, 0)
				fuzzEntries := make([]string, 0)
				fuzzEntriesLookup := make(map[string]int)

				for newRowFuzzPos, newRowFuzz := range m.Records {
					fuzzEntries = append(fuzzEntries, newRowFuzz[newColFuzz])
					fuzzEntriesLookup[m.Records[newRowFuzzPos][newColFuzz]] = newRowFuzzPos
				}

				ogColFuzzPos, _ := strconv.Atoi(ogColFuzz)
				suggestions = append(suggestions, fuzzy.RankFind(ogRow[ogColFuzzPos], fuzzEntries)...)

				if len(suggestions) > 0 {
					for _, choice := range suggestions {
						log.Printf("SUGGEST:  Row %v for %s, %s", choice.OriginalIndex+1, choice.Source, strings.Join(m.Records[choice.OriginalIndex], ", "))
					}
				}
			}

			// choose row, -1 (default) skips, -2 goes back, -3 inserts new row, -4 exits and saves
			newRowPos := askChoiceAllowNull("Choose row to insert into")
			if newRowPos == -1 {
				continue
			} else if newRowPos == -2 {
				i -= 2
				continue
			} else if newRowPos == -4 {
				break
			}

			// If inserting a new row, resize m.Records
			if newRowPos == -3 {
				newRecords := make([][]string, len(m.Records)+1)
				for n0, r0 := range m.Records {
					newRecords[n0] = r0
				}

				newRowPos = len(newRecords)
				m.Records = newRecords
			}

			if newRowPos > len(m.Records) {
				log.Printf("%v is greater than the allowed %v!\n", newRowPos, len(m.Records))
				i--
				continue
			}

			// normalize
			newRowPos--

			log.Printf("CONFIRM:  %s\n", strings.Join(m.Records[newRowPos], ", "))
			if !askConfirm("Confirm insert?", true) {
				i--
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
					newRow[newColPosMap] = ogCol                                      // set [new row][new col] to old record
					m.Records[newRowPos] = newRow                                     // set records[new pos] to new modified row
					m.RecordMap[mapFile.Name][strconv.Itoa(ogRowPos)] = newRowPos + 1 // save recordMap[old row][new row] for history
				}
			}

			log.Printf("INSERTED: %s\n\n", strings.Join(m.Records[newRowPos], ", "))
		}
	}

	saveRemapped(m)
	saveAllAsCsv(m)
}

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
	case "":
		return yesDefault
	default:
		return false
	}
}

func saveRemapped(a any) {
	j, err := json.MarshalIndent(a, "", "    ")
	if err != nil {
		log.Fatalf("failed to marshal: %v\n", err)
	}

	saveFile("remapped/data.json", j)
	saveFile(fmt.Sprintf("remapped/data-%s.json", time.Now().Format(time.RFC3339)), j)
}

func saveFile(file string, j []byte) {
	err := os.WriteFile(file, j, fileMode)
	if err != nil {
		log.Printf("failed to save %s: %v\n", file, err)
	} else {
		log.Printf("SAVED: %s\n", file)
	}
}

func readMappings() *Mappings {
	d, err := os.ReadFile("remapped/data.json")
	if err != nil {
		log.Fatalf("Failed to open remapped/data.json: %v\n", err)
	}

	var m *Mappings
	if err := json.Unmarshal(d, &m); err != nil {
		log.Fatalf("Failed to unmarshal remapped/data.json: %v\n", err)
	} else {
		return m
	}

	return nil
}

func saveAllAsCsv(m Mappings) {
	saveAsCsv("remapped/combined.csv", m)
	saveAsCsv(fmt.Sprintf("remapped/combined-%s.csv", time.Now().Format(time.RFC3339)), m)
}

func saveAsCsv(name string, m Mappings) {
	file, err := os.Create(name)
	if err != nil {
		fmt.Println("oops, couldn't create the file :(")
		return
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	err = writer.WriteAll(m.Records)
	if err != nil {
		log.Printf("Failed to convert to csv: %v\n", err)
		return
	}

	if err := writer.Error(); err != nil {
		fmt.Println("oops, there was an error writing to the csv file :(")
		return
	}

	log.Printf("SAVED: %s\n", name)
}
