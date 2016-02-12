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
    "path/filepath"
    "encoding/json"
    "github.com/xwb1989/sqlparser"
    "github.com/influxdb/influxdb/client/v2"
)

const (
    LINE_TAIL = "/*!*/;\n"
    LINE_HEAD = "INSERT"
    debug = false
)

type ExtractType map[string]struct{
    Tags map[string]string
    Fields map[string]string
    Time string
}

type ConfigType struct{
    Address string
    Database string
    Username string
    Password string
    Precision string
    Debug bool
    Extract ExtractType
}

func insertSplitFunc(data []byte, atEOF bool) (advance int, token []byte, err error) {
    // Return nothing if at end of file and no data passed
    if atEOF && len(data) == 0 {
        return 0, nil, nil
    }
    // Find a INSERT statement
    if i := strings.Index(string(data), LINE_TAIL + LINE_HEAD); i >= 0 {
        return i + 7, data[0:i], nil
    }
    // If at end of file with data return the data
    if atEOF {
        return len(data), data, nil
    }

    return
}

func main() {
    dir, _ := filepath.Abs(filepath.Dir(os.Args[0]))
    file, _ := os.Open(filepath.Join(dir, "conf.json"))
    decoder := json.NewDecoder(file)
    config := ConfigType{}

    if err := decoder.Decode(&config); err != nil {
      fmt.Println("error decoding config: ", err)
    } else {
        fmt.Printf("Config: %+v\n", config)
    }

    c, _ := client.NewHTTPClient(client.HTTPConfig{
        Addr: config.Address,
        Username: config.Username,
        Password: config.Password,
    })

    // Create a new point batch
    bp_config := client.BatchPointsConfig{
        Database:  config.Database,
        Precision: config.Precision,
    }

    scanner := bufio.NewScanner(bufio.NewReader(os.Stdin))
    scanner.Split(insertSplitFunc)

    insert_cnt := 0
    for bp_cnt := 1; bp_cnt > 0; {
        bp, _ := client.NewBatchPoints(bp_config)
        for bp_cnt = 0; scanner.Scan() && bp_cnt < 100000; {
            if  strings.Index(scanner.Text(), LINE_HEAD) != 0 {
                continue // there is no INSERT at the start of the file
            }
            if e := strings.Index(scanner.Text(), LINE_TAIL);  e > 0 {
                sqlInsert := scanner.Text()[:e]
                err := analyzeInsert(sqlInsert, config.Extract, bp)
                if err != nil {
                    fmt.Printf("analyzeInsert failed for line \n%s\n%s\n", sqlInsert, err)
                } else {
                    insert_cnt ++
                    bp_cnt += len(bp.Points())
                }
            }
        }
        if bp_cnt > 0 {
            err := c.Write(bp)
            if err != nil {
                fmt.Println(err)
                return
            }
            // fmt.Printf("%+v\n", bp.Points()[0])
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

                if cols[i] == extract[table].Time {
                    if(debug) {fmt.Println("time")}
                    ve, ok := v.(sqlparser.StrVal)
                    if ok {
                        timestamp, _ = time.Parse( // place T as date time seperator and add UTC Z
                            time.RFC3339, strings.Replace(string(ve)+"Z", " ", "T", 1))
                    } else {
                        fmt.Println("Time column is no StrVal")
                    }
                } else if _, present := extract[table].Tags[cols[i]]; present {
                    ve, ok := v.(sqlparser.StrVal)
                    if(debug) {fmt.Printf("tags: %s, %t\n", ve, ok)}
                    if ok {
                        tags["tag_" + cols[i]] = string(ve)
                    } else {
                        fmt.Println("Tag column is no StrVal")
                    }
                } else if _, present := extract[table].Fields[cols[i]]; present {
                    switch v := v.(type) {
                    case sqlparser.NumVal:
                        // if(debug) {fmt.Printf("field: %s, %t\n", ve, ok)}
                        val, err := strconv.ParseFloat(string(v), 64)
                        if(debug) {fmt.Printf("ParseFloat %4.2f\n", val)}
                        if err != nil {
                            fmt.Println("Field column doesn't convert to float")
                        } else {
                            fields[cols[i]] = val
                        }
                    case sqlparser.StrVal:
                        // if(debug) {fmt.Printf("tags: %s, %t\n", ve, ok)}
                        fields[cols[i]] = string(v)
                    }
                }
            }
            if(debug) {fmt.Println("NewPoint")}
            if ! timestamp.IsZero() && len(fields) > 0 {
                p, err := client.NewPoint(table, tags, fields, timestamp)
                if err != nil {
                    return err
                }
                bp.AddPoint(p)
            }
        }
    }

    return nil
}
