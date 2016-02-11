package main

import (
    "fmt"
    "time"
    "strconv"
    "errors"
    "reflect"
    "strings"
    "bufio"
    "os"
    "github.com/xwb1989/sqlparser"
    "github.com/influxdb/influxdb/client/v2"
)

const (
    MyDB = "y"
    username = ""
    password = ""

    debug = false
)

type ExtractType map[string]map[string]string




func insertSplitFunc(data []byte, atEOF bool) (advance int, token []byte, err error) {

    // Return nothing if at end of file and no data passed
    if atEOF && len(data) == 0 {
        return 0, nil, nil
    }

    // Find the index of the input of a newline followed by a
    // pound sign.
    if i := strings.Index(string(data), "/*!*/;\nINSERT"); i >= 0 {
        return i + 7, data[0:i], nil
    }

    // If at end of file with data return the data
    if atEOF {
        return len(data), data, nil
    }

    return
}

func main() {

    extract := ExtractType{
        "data": {
            "tags": "col1",
            "fields": "col2",
            "time": "timecol",
        },
        "datapoint": {
            "tags": "name",
            "fields": "coherent_si_value",
            "time": "time_stamp",
        },
    }

    c, _ := client.NewHTTPClient(client.HTTPConfig{
        Addr: "http://localhost:8086",
        Username: username,
        Password: password,
    })


    // Create a new point batch
    bp_config := client.BatchPointsConfig{
        Database:  MyDB,
        Precision: "s",
    }

    scanner := bufio.NewScanner(bufio.NewReader(os.Stdin))
    scanner.Split(insertSplitFunc)

    insert_cnt := 0
    for bp_cnt := 1; bp_cnt > 0; {
        bp, _ := client.NewBatchPoints(bp_config)
        for bp_cnt = 0; scanner.Scan() && bp_cnt < 1000; {
            insert_cnt ++
            e := strings.Index(scanner.Text(),"/*!*/;")
            if e > 0 {
                sqlInsert := scanner.Text()[:e]
                err := analyzeInsert(sqlInsert, extract, bp)
                if err != nil {
                    fmt.Println(err)
                } else {
                    bp_cnt ++
                }
            }
        }
        if bp_cnt > 0 {
            err := c.Write(bp)
            if err != nil {
                fmt.Println(err)
                return
            }
            fmt.Printf("%+v\n", bp.Points()[0])
        }
    }

}


func analyzeInsert(sql string, extract ExtractType, bp client.BatchPoints) error {
    // parse SQL string
    t, err := sqlparser.Parse(sql)

    if(debug) {fmt.Printf("%#v\n", t)}

    if err != nil {
        return err
    }

    // interpret parse tree
    switch t := t.(type) {
    default:
        fmt.Printf("Not an insert statment (%v)\n", reflect.TypeOf(t))
    case *sqlparser.Insert:
        table := string(t.Table.Name)
        if _, ok := extract[table]; ! ok {
            return nil
        }

        cols := make([]string, len(t.Columns))
        for i, c := range t.Columns {
            cols[i] = sqlparser.String(c)
        }

        if(debug) {fmt.Printf("%#v\n", cols)}
        rows, ok := t.Rows.(sqlparser.Values)
        if ! ok {
            return errors.New("Rows are no Values")
        }

        tags := make(map[string]string)
        fields := make(map[string]interface{})
        var timestamp time.Time

        for _, r := range rows {
            row, ok := r.(sqlparser.ValTuple)
            if ! ok {
                return errors.New("Row is no ValTuple")
            }

            if(debug) {fmt.Printf("hallo\n")}

            for i, v := range row {
                if cols[i] == extract[table]["time"] {
                    if(debug) {fmt.Println("time")}
                    ve, ok := v.(sqlparser.StrVal)
                    if ok {
                        timestamp, _ = time.Parse(time.RFC3339, string(ve)+"Z")
                    } else {
                        fmt.Println("Time column is no StrVal")
                    }
                } else if cols[i] == extract[table]["tags"] {
                    ve, ok := v.(sqlparser.StrVal)
                    if(debug) {fmt.Printf("tags: %s, %t\n", ve, ok)}
                    if ok {
                        tags[cols[i]] = string(ve)
                    } else {
                        fmt.Println("Tag column is no StrVal")
                    }
                } else if cols[i] == extract[table]["fields"] {
                    ve, ok := v.(sqlparser.NumVal)
                    if(debug) {fmt.Printf("field: %s, %t\n", ve, ok)}
                    if ok {
                        val, err := strconv.ParseFloat(string(ve), 64)
                        if(debug) {fmt.Printf("ParseFloat %4.2f\n", val)}
                        if err != nil {
                            fmt.Println("Field column doesn't convert to float")
                        } else {
                            fields[cols[i]] = val
                        }
                    } else {
                        fmt.Println("Field column is no StrVal")
                    }
                }
            }
            if(debug) {fmt.Println("NewPoint")}
            p, err := client.NewPoint(table, tags, fields, timestamp)
            if err != nil {
                return err
            }
            bp.AddPoint(p)
        }
    }

    return nil
}
