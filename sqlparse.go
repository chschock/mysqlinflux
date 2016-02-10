package main

import (
    "fmt"
    "time"
    "strconv"
    "errors"
    "reflect"
    "strings"
    // "regexp"
    "bufio"
    "io"
    "log"
    "os"
    "github.com/xwb1989/sqlparser"
    "github.com/influxdb/influxdb/client/v2"
)

const (
    MyDB = "y"
    username = ""
    password = ""
)

type ExtractType map[string]map[string]string

func main2() {
    nBytes, nChunks := int64(0), int64(0)
    r := bufio.NewReader(os.Stdin)
    buf := make([]byte, 0, 4*1024)
    for {
        n, err := r.Read(buf[:cap(buf)])
        buf = buf[:n]
        if n == 0 {
            if err == nil {
                continue
            }
            if err == io.EOF {
                break
            }
            log.Fatal(err)
        }
        nChunks++
        nBytes += int64(len(buf))
        // process buf
        if err != nil && err != io.EOF {
            log.Fatal(err)
        }
    }
    log.Println("Bytes:", nBytes, "Chunks:", nChunks)
}

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
    bp, _ := client.NewBatchPoints(bp_config)

    // regex := regexp.MustCompile("into")
    // sql := "insert into data (col1, col2) values ('mytag',1.4142)"
    // var _ = strings.NewReader(sql)
    // fmt.Printf("found at pos %v\n", regex.FindReaderIndex(scanner))
    // err := analyzeInsert(sql, extract, bp)

    scanner := bufio.NewScanner(bufio.NewReader(os.Stdin))
    scanner.Split(insertSplitFunc)

    insert_cnt := 0
    for scanner.Scan() {
        insert_cnt ++
        e := strings.Index(scanner.Text(),"/*!*/;")

        if e > 0 {
            sqlInsert := scanner.Text()[:e]
            err := analyzeInsert(sqlInsert, extract, bp)
            // fmt.Printf("-> %100s\n", sqlInsert)
            if err != nil {
                fmt.Println(err)
            }
        }
        if (insert_cnt % 1000 == 0) {
            err := c.Write(bp)
            if err != nil {
                fmt.Println(err)
                return
            }
            fmt.Printf("%+v\n", bp.Points()[0])
            bp, _ = client.NewBatchPoints(bp_config)
        }
    }

    c.Write(bp)

}

const(
    debug = false
)

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

func test() {
    var sql string
    sql = "insert into data (col1, col2) values ('asdaf123a','1a\\\nmsd,2,3,4')"
    // sql = "INSERT INTO t3 VALUES (8, 10, 'baz')"
    pt, err := sqlparser.Parse(sql)
    fmt.Printf("%v  [%v]\n", sql, err)

    fmt.Printf("%v\n", sqlparser.String(pt))

    fmt.Printf("%#v\n%s\n", pt, reflect.TypeOf(pt))
    // var t interface{}

    fmt.Println("type switching")
    t := pt
    switch t := t.(type) {
    default:
        fmt.Printf("stupid: %v\n", reflect.TypeOf(t))
    case *sqlparser.Insert:
        fmt.Printf("nice: \n%+v\n%s\n\n", t, t.Table.Name)
        for i, c := range t.Columns {
            fmt.Printf("%d: %s %s\n", i, sqlparser.String(c), reflect.TypeOf(c))
        }
        fmt.Printf("%#v\n", t.Rows)
        rows, ok := t.Rows.(sqlparser.Values)
        fmt.Printf("ok: %t, %#v\n", ok, rows)
        for i, r := range rows {
            fmt.Printf("row %d: \n", i)
            vt, _ := r.(sqlparser.ValTuple)
            for _, v := range vt {
                ve, _ := v.(sqlparser.StrVal)
                fmt.Printf("%s  [%#v]\n", string(ve), v)
            }
            fmt.Println()
        }
    }

}

