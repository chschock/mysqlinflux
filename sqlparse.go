package main

import (
    "time"
    "strconv"
    "errors"
    "reflect"
    "strings"
    "bytes"
    "regexp"
    "bufio"
    "os"
    "path/filepath"
    "encoding/json"
    "github.com/xwb1989/sqlparser"
    "github.com/influxdb/influxdb/client/v2"
    log "github.com/Sirupsen/logrus"
    "github.com/davecheney/profile"
)

var LINE_SEP = []byte("/*!*/;\n")
var INSERT_HEAD = []byte("INSERT")

const (
    gen_profile_data = false
)

type ExtractType map[string]struct{
    Tags map[string]string
    Fields map[string]string
    Time string
}

type DbType struct{
    Address string
    Database string
    Username string
    Password string
    Precision string
}

type BinlogType struct{
    MinTime string
    MaxTime string
}

type ConfigType struct{
    Db DbType
    Binlog BinlogType
    Debug bool
    Extract ExtractType
}

func sqlSplitFunc(data []byte, atEOF bool) (advance int, token []byte, err error) {
    // Return nothing if at end of file and no data passed
    if atEOF && len(data) == 0 {
        return 0, nil, nil
    }
    // Find a INSERT statement
    if i := bytes.Index(data, LINE_SEP); i >= 0 {
        return i + len(LINE_SEP), data[0:i], nil
    }
    // If at end of file with data return the data
    if atEOF {
        return len(data), data, nil
    }
    // get more data
    return 0, nil, nil
}

func main() {
    if gen_profile_data {
        defer profile.Start(profile.CPUProfile).Stop()
    }
    startTime := time.Now()

    config := getConfig()

    c, _ := client.NewHTTPClient(client.HTTPConfig{
        Addr: config.Db.Address,
        Username: config.Db.Username,
        Password: config.Db.Password,
    })

    // Create a new point batch
    bp_config := client.BatchPointsConfig{
        Database:  config.Db.Database,
        Precision: config.Db.Precision,
    }

    scanner := bufio.NewScanner(bufio.NewReader(os.Stdin))
    scanner.Split(sqlSplitFunc)

    scanner.Scan(); scanner.Scan()
    in_range, err := checkBinlogTime(scanner.Text(), config)
    if err != nil {
        log.Fatal("Error checking binlog time: ", err)
    }
    if ! in_range {
        return
    }

    insert_cnt := 0
    for bp_cnt := 1; bp_cnt > 0; {
        bp, _ := client.NewBatchPoints(bp_config)
        for bp_cnt = 0; scanner.Scan() && bp_cnt < 100000; {
            line, ih := scanner.Bytes(), INSERT_HEAD
            if len(line) > len(ih) && bytes.Compare(line[0:len(ih)], ih) == 0 {
                err := analyzeInsert(string(line), &config.Extract, &bp)
                if err != nil {
                    log.Warnf("analyzeInsert failed for line \n%s\n%s\n", string(line), err)
                } else {
                    insert_cnt ++
                    bp_cnt += len(bp.Points())
                }
            }
        }
        if bp_cnt > 0 {
            err := c.Write(bp)
            if err != nil {
                log.Fatalln(err)
                return
            }
            log.Debugf("%+v\n", bp.Points()[0])
        }
    }
    log.Infof("Processing time: %s\n", time.Since(startTime))
}


func analyzeInsert(sql string, extractPtr *ExtractType, bpPtr *client.BatchPoints) error {
    extract, bp := *extractPtr, *bpPtr
    // parse SQL string
    t, err := sqlparser.Parse(sql)

    log.Debugf("%#v\n", t)

    if err != nil {
        return err
    }

    // interpret parse tree
    switch t := t.(type) {
    default:
        log.Warnf("Not an insert statment (%v)\n", reflect.TypeOf(t))
    case *sqlparser.Insert:
        table := string(t.Table.Name)
        if _, ok := extract[table]; ! ok {
            return nil
        }

        cols := make([]string, len(t.Columns))
        for i, c := range t.Columns {
            cols[i] = sqlparser.String(c)
        }
        log.Debugf("%#v\n", cols)

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

            for i, v := range row {

                if cols[i] == extract[table].Time {
                    log.Debug("time")
                    ve, ok := v.(sqlparser.StrVal)
                    if ok {
                        timestamp, _ = time.Parse( // place T as date time seperator and add UTC Z
                            time.RFC3339, strings.Replace(string(ve)+"Z", " ", "T", 1))
                    } else {
                        log.Info("Time column is no StrVal")
                    }
                } else if _, present := extract[table].Tags[cols[i]]; present {
                    ve, ok := v.(sqlparser.StrVal)
                    log.Debugf("tags: %s, %t\n", ve, ok)
                    if ok {
                        tags["tag_" + cols[i]] = string(ve)
                    } else {
                        log.Info("Tag column is no StrVal")
                    }
                } else if _, present := extract[table].Fields[cols[i]]; present {
                    switch v := v.(type) {
                    case sqlparser.NumVal:
                        log.Debugf("field: %s, %t\n", v, ok)
                        val, err := strconv.ParseFloat(string(v), 64)
                        log.Debugf("ParseFloat %4.2f\n", val)
                        if err != nil {
                            log.Info("Field column doesn't convert to float")
                        } else {
                            fields[cols[i]] = val
                        }
                    case sqlparser.StrVal:
                        log.Debugf("tags: %s, %t\n", v, ok)
                        fields[cols[i]] = string(v)
                    }
                }
            }
            log.Debug("NewPoint")
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

func checkBinlogTime(head string, config ConfigType) (bool, error) {
    regex := regexp.MustCompile("#(.*?) server")
    match := regex.FindStringSubmatch(head)[1]
    t, err := time.Parse("060102 15:04:05", match)
    if err != nil {
        log.Warnf("Unable to parse binlog time. Will import it. Matched %s. Head of file \n%s\n",
            match, head)
        return true, nil
    }
    t_min, err_min := time.Parse(time.RFC3339, config.Binlog.MinTime)
    if err_min == nil {
        log.Debugf("min_binlog_time %v", t_min)
        if t.Before(t_min) {
            log.Info("binlog date < min_binlog_time.")
            return false, nil
        }
    } else if config.Binlog.MinTime != "" {
        return false, err_min
    }
    t_max, err_max := time.Parse(time.RFC3339, config.Binlog.MaxTime)
    if err_max == nil {
        log.Debugf("max_binlog_time %v", t_max)
        if ! t.Before(t_max) {
            log.Info("binlog date >= max_binlog_time.")
            return false, nil
        }
    } else if config.Binlog.MaxTime != "" {
        return false, err_max
    }
    return true, nil
}

func getConfig() ConfigType{
    dir, _ := filepath.Abs(filepath.Dir(os.Args[0]))
    file, _ := os.Open(filepath.Join(dir, "conf.json"))
    decoder := json.NewDecoder(file)
    config := ConfigType{}

    if err := decoder.Decode(&config); err != nil {
      log.Fatalln("error decoding config: ", err)
    } else {
        log.Debugf("Config: %#v\n", config)
    }

    if config.Debug {
        log.SetLevel(log.DebugLevel)
    }
    return config
}