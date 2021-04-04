// cobagorx project main.go
package main

import (
	"context"
	"encoding/csv"
	"fmt"

	"io"
	"log"

	"bytes"
	"os"
	"reflect"
	"time"

	"github.com/jszwec/csvutil"
	rxgo "github.com/reactivex/rxgo/v2"
)

type Row struct {
	RowNumber          string
	EAN                string
	ProductName        string
	ProductDescription string
	StoreName          string
	ProductTrademark   string
	Price              string
	DiscountedPrice    string
	Discount           string
	VariationName      string
	VariationValue     string
	Stock              string
	ProductCategory1   string
	ProductCategory2   string
	ProductCategory3   string
}

type StoreDTO struct {
	StoreName string
	Rows      []Row
}

func (storeDTOPtr *StoreDTO) appendRow(row Row) {
	storeDTOPtr.Rows = append(storeDTOPtr.Rows, row)
}

func newStoreDTO(storeName string) StoreDTO {
	instance := StoreDTO{}
	instance.StoreName = storeName
	instance.Rows = make([]Row, 0, 20)

	return instance
}

var (
	rowFields = getRowFields()
)

func getRowFields() []string {
	e := reflect.ValueOf(&Row{}).Elem()

	rowFields := make([]string, 0, 20)

	for i := 0; i < e.NumField(); i++ {
		rowFields = append(rowFields, e.Type().Field(i).Name)
	}

	return rowFields
}

type FileConfiguration struct {
	ColumnMappings      []ColumnMapping
	IgnoreUnmappedField bool
}

func (fileConfiguration FileConfiguration) ColumnMappingsByFieldName() map[string]ColumnMapping {
	columnMappingsByFieldName := make(map[string]ColumnMapping)

	for _, columnMapping := range fileConfiguration.ColumnMappings {
		columnMappingsByFieldName[columnMapping.FieldName] = columnMapping
	}

	return columnMappingsByFieldName
}

type ColumnMapping struct {
	ColumnName      string
	FieldName       string
	NoisyCharacters string
}

type RecordHolder struct {
	Values map[string]string
}

func newRecordHolder() RecordHolder {
	instance := RecordHolder{}
	instance.Values = make(map[string]string)

	return instance
}

//https://stackoverflow.com/questions/24562942/golang-how-do-i-determine-the-number-of-lines-in-a-file-efficiently
func countLines(r io.Reader) (int, error) {
	buf := make([]byte, 32*1024)
	count := 0
	lineSep := []byte{'\n'}

	for {
		c, err := r.Read(buf)
		count += bytes.Count(buf[:c], lineSep)

		switch {
		case err == io.EOF:
			return count, nil

		case err != nil:
			return count, err
		}
	}
}

func createRow(recordHolder RecordHolder, rowFields []string, fileConfiguration FileConfiguration, columnMappingsByFieldName map[string]ColumnMapping) (Row, error) {
	row := Row{}

	for _, rowField := range rowFields {
		if columnMapping, ok := columnMappingsByFieldName[rowField]; ok {
			reflect.ValueOf(&row).Elem().FieldByName(rowField).SetString(
				recordHolder.Values[columnMapping.ColumnName],
			)
		} else {
			if !fileConfiguration.IgnoreUnmappedField {
				return row, fmt.Errorf("ColumnMapping for field %s is not specified", rowField)
			}
		}
	}

	return row, nil
}

func recordsProducer(csvFilename string, fileConfiguration FileConfiguration) func(context.Context, chan<- rxgo.Item) {
	columnMappingsByFieldName := fileConfiguration.ColumnMappingsByFieldName()

	return func(context context.Context, ch chan<- rxgo.Item) {
		csvFile, err := os.Open(csvFilename)
		if err != nil {
			ch <- rxgo.Error(err)
			return
		}

		csvReader := csv.NewReader(csvFile)

		dec, err := csvutil.NewDecoder(csvReader)
		if err != nil {
			ch <- rxgo.Error(err)
			return
		}

		headers := dec.Header()
		if len(headers) != len(fileConfiguration.ColumnMappings) {
			ch <- rxgo.Error(fmt.Errorf(
				"Length of headers does not match fileConfiguration (%d vs %d)",
				len(headers), len(fileConfiguration.ColumnMappings),
			))
			return
		}

		for {
			recordHolder := newRecordHolder()

			if err := dec.Decode(&RecordHolder{}); err == io.EOF {
				return
			} else if err != nil {
				log.Fatal(err)
			}

			record := dec.Record()
			for i, header := range headers {
				recordHolder.Values[header] = record[i]
			}

			row, err := createRow(recordHolder, rowFields, fileConfiguration, columnMappingsByFieldName)

			if err != nil {
				ch <- rxgo.Error(err)
			} else {
				ch <- rxgo.Of(row)
			}

		}

	}
}

func createStoreDTOsObservable(
	fileName string, fileConfiguration FileConfiguration,
	bufferSize int, bufferTimeout time.Duration,
) rxgo.Observable {
	observable := rxgo.Defer([]rxgo.Producer{recordsProducer(fileName, fileConfiguration)})

	return observable.BufferWithTimeOrCount(rxgo.WithDuration(bufferTimeout), bufferSize).FlatMap(
		func(item rxgo.Item) rxgo.Observable {
			rows := item.V
			//fmt.Printf("Callback of BufferWithTimeOrCount, rows: %v\n", rows)

			return rxgo.Just(rows)().GroupByDynamic(func(item rxgo.Item) string {
				row, _ := item.V.(Row)
				//fmt.Printf("Callback of GroupByDynamic, RowNumber: %s, StoreName: %s\n", row.RowNumber, row.StoreName)

				return row.StoreName
			}, rxgo.WithBufferedChannel(bufferSize)).FlatMap(func(item rxgo.Item) rxgo.Observable {
				groupedObservable := item.V.(rxgo.GroupedObservable)
				//fmt.Printf("New observable: %s\n", groupedObservable.Key)

				storeDTOItem, err := groupedObservable.Reduce(func(_ context.Context, acc interface{}, elem interface{}) (interface{}, error) {
					var storeDTO StoreDTO
					row := elem.(Row)

					if acc == nil {
						storeDTO = newStoreDTO(row.StoreName)
					} else {
						storeDTO = acc.(StoreDTO)
					}

					storeDTO.appendRow(row)

					return storeDTO, nil
				}).Get()

				if err != nil {
					return rxgo.Thrown(err)
				}

				return rxgo.Just(storeDTOItem.V)()
			})
		},
	)
}

func sendStoreDTOs(storeDTOsObservable rxgo.Observable) rxgo.Disposed {
	return storeDTOsObservable.ForEach(func(v interface{}) {
		fmt.Printf("Sending: %v\n", v)
	}, func(err error) {
		fmt.Printf("error: %e\n", err)
	}, func() {
		fmt.Println(".... Observable is closed")
	})
}

func readFileConfiguration(partnerName string) FileConfiguration {
	fmt.Println("Reading file configuration for partner", partnerName)

	return FileConfiguration{
		ColumnMappings: []ColumnMapping{
			ColumnMapping{ColumnName: "Row_Num", FieldName: "RowNumber"},
			ColumnMapping{ColumnName: "EAN", FieldName: "EAN"},
			ColumnMapping{ColumnName: "Product_Name", FieldName: "ProductName"},
			ColumnMapping{ColumnName: "Product_Description", FieldName: "ProductDescription"},
			ColumnMapping{ColumnName: "Store_Name", FieldName: "StoreName"},
			ColumnMapping{ColumnName: "Product_Trademark", FieldName: "ProductTrademark"},
			ColumnMapping{ColumnName: "Price", FieldName: "Price"},
			ColumnMapping{ColumnName: "Discounted_Price", FieldName: "DiscountedPrice"},
			ColumnMapping{ColumnName: "Discount", FieldName: "Discount"},
			ColumnMapping{ColumnName: "Variation_Name", FieldName: "VariationName"},
			ColumnMapping{ColumnName: "Variation_Value", FieldName: "VariationValue"},
			ColumnMapping{ColumnName: "Stock", FieldName: "Stock"},
			ColumnMapping{ColumnName: "Product_Category_1", FieldName: "ProductCategory1"},
			ColumnMapping{ColumnName: "Product_Category_2", FieldName: "ProductCategory2"},
			ColumnMapping{ColumnName: "Product_Category_3", FieldName: "ProductCategory3"},
		},
	}
}

func main() {
	fileConfiguration := readFileConfiguration("PARTNER_1_MX")

	storeDTOsObservable := createStoreDTOsObservable("input.csv", fileConfiguration, 4, 3*time.Second)

	<-sendStoreDTOs(storeDTOsObservable)
}
