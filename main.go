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
	RowNumber   int
	SKU         string
	ProductName string
	StoreName   string
	Price       string
	Stock       string
}

type ProductAvailabilities struct {
	StoreName     string
	ProductStocks []ProductStock
}

type ProductStock struct {
	SKU         string
	ProductName string
	Price       string
	Stock       string
}

func (productAvailabilitiesPtr *ProductAvailabilities) addProductStock(productStock ProductStock) {
	productAvailabilitiesPtr.ProductStocks = append(
		productAvailabilitiesPtr.ProductStocks, productStock,
	)
}

func newProductStock(row Row) ProductStock {
	return ProductStock{
		SKU:         row.SKU,
		ProductName: row.ProductName,
		Price:       row.Price,
		Stock:       row.Stock,
	}
}

func newProductAvailabilities(storeName string) ProductAvailabilities {
	instance := ProductAvailabilities{}

	instance.StoreName = storeName
	instance.ProductStocks = make([]ProductStock, 0, 20)

	return instance
}

var (
	rowFields = getRowFields()
)

func getRowFields() []string {
	e := reflect.ValueOf(&Row{}).Elem()

	rowFields := make([]string, 0, 20)

	for i := 0; i < e.NumField(); i++ {
		fieldName := e.Type().Field(i).Name
		if fieldName != "RowNumber" {
			rowFields = append(rowFields, e.Type().Field(i).Name)
		}
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

func createRow(recordHolder RecordHolder, rowFields []string, fileConfiguration FileConfiguration, columnMappingsByFieldName map[string]ColumnMapping, rowNumber int) (Row, error) {
	row := Row{
		RowNumber: rowNumber,
	}

	for _, rowField := range rowFields {
		columnMapping := columnMappingsByFieldName[rowField]

		reflect.ValueOf(&row).Elem().FieldByName(rowField).SetString(
			recordHolder.Values[columnMapping.ColumnName],
		)
	}

	return row, nil
}

func recordsProducer(csvFilename string, fileConfiguration FileConfiguration) func(context.Context, chan<- rxgo.Item) {
	return func(context context.Context, ch chan<- rxgo.Item) {
		defer func() {
			if err := recover(); err != nil {
				ch <- rxgo.Error(fmt.Errorf("Uncaught error in recordsProducer %v", err))
			}
		}()

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
		var headersAsMap = make(map[string]bool, len(headers))
		for _, header := range headers {
			headersAsMap[header] = true
		}

		for _, columnMapping := range fileConfiguration.ColumnMappings {
			if _, ok := headersAsMap[columnMapping.ColumnName]; !(ok || fileConfiguration.IgnoreUnmappedField) {
				ch <- rxgo.Error(fmt.Errorf(
					"Mapping for field %s (column %s) is not found in the input",
					columnMapping.FieldName, columnMapping.ColumnName,
				))

				return
			}
		}

		rowNumber := 2
		columnMappingsByFieldName := fileConfiguration.ColumnMappingsByFieldName()
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

			row, err := createRow(
				recordHolder, rowFields, fileConfiguration,
				columnMappingsByFieldName, rowNumber,
			)

			rowNumber++

			if err != nil {
				ch <- rxgo.Error(err)
			} else {
				ch <- rxgo.Of(row)
			}
		}
	}
}

func createProductAvailabilitiesObservable(
	fileName string, fileConfiguration FileConfiguration,
	bufferSize int, bufferTimeout time.Duration,
) rxgo.Observable {
	observable := rxgo.Defer([]rxgo.Producer{recordsProducer(fileName, fileConfiguration)})

	return observable.BufferWithTimeOrCount(rxgo.WithDuration(bufferTimeout), bufferSize).FlatMap(
		func(item rxgo.Item) rxgo.Observable {
			if item.Error() {
				return rxgo.Thrown(item.E)
			}

			rows := item.V
			//fmt.Printf("Callback of BufferWithTimeOrCount, rows: %v\n", rows)

			return rxgo.Just(rows)().GroupByDynamic(func(item rxgo.Item) string {
				row, _ := item.V.(Row)
				//fmt.Printf("Callback of GroupByDynamic, RowNumber: %d, StoreName: %s\n", row.RowNumber, row.StoreName)

				return row.StoreName
			}, rxgo.WithBufferedChannel(bufferSize)).FlatMap(func(item rxgo.Item) rxgo.Observable {
				groupedObservable := item.V.(rxgo.GroupedObservable)
				//fmt.Printf("New observable: %s\n", groupedObservable.Key)

				productAvailabilitiesItem, err := groupedObservable.Reduce(func(_ context.Context, acc interface{}, elem interface{}) (interface{}, error) {
					var productAvailabilities ProductAvailabilities
					row := elem.(Row)

					if acc == nil {
						productAvailabilities = newProductAvailabilities(row.StoreName)
					} else {
						productAvailabilities = acc.(ProductAvailabilities)
					}

					productAvailabilities.addProductStock(newProductStock(row))

					return productAvailabilities, nil
				}).Get()

				if err != nil {
					return rxgo.Thrown(err)
				}

				return rxgo.Just(productAvailabilitiesItem.V)()
			})
		},
	)
}

func sendProductAvailabilities(productAvailabilitiesObservable rxgo.Observable) rxgo.Disposed {
	return productAvailabilitiesObservable.ForEach(func(v interface{}) {
		fmt.Printf("Sending: %v\n", v)
	}, func(err error) {
		fmt.Printf("Caught error in sendProductAvailabilities: %e\n", err)
	}, func() {
		fmt.Println(".... Observable is closed")
	})
}

func readFileConfiguration(partnerName string) FileConfiguration {
	fmt.Println("Reading file configuration for partner", partnerName)

	return FileConfiguration{
		ColumnMappings: []ColumnMapping{
			ColumnMapping{ColumnName: "SKU", FieldName: "SKU"},
			ColumnMapping{ColumnName: "Product_Name", FieldName: "ProductName"},
			ColumnMapping{ColumnName: "Store_Name", FieldName: "StoreName"},
			ColumnMapping{ColumnName: "Price", FieldName: "Price"},
			ColumnMapping{ColumnName: "Stock", FieldName: "Stock"},
		},
	}
}

func main() {
	fileConfiguration := readFileConfiguration("PARTNER_1_MX")

	productAvailabilitiesObservable := createProductAvailabilitiesObservable("input.csv", fileConfiguration, 4, 3*time.Second)

	<-sendProductAvailabilities(productAvailabilitiesObservable)
}
