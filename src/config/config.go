/**
* @Author:zhoutao
* @Date:2021/3/2 上午11:04
* @Desc:
 */

package config

import (
	"bufio"
	"github.com/ztaoing/GoRedisDemo/src/lib/logger"
	"log"
	"os"
	"reflect"
	"strconv"
	"strings"
)

type PropertyHolder struct {
	Bind           string   `cfg:"bind"`
	Port           int      `cfg:"port"`
	AppendOnly     bool     `cfg:"appendOnly"`
	AppendFilename string   `cfg:"appendFileName"`
	MaxClients     int      `cfg:"maxClients"`
	Peers          []string `cfg:"peers"`
	Self           string   `cfg:"self"`
}

var Properties *PropertyHolder

func init() {
	Properties = &PropertyHolder{
		Bind:       "127.0.0.1",
		Port:       6379,
		AppendOnly: false,
	}

}

func SetupConfig(configFileName string) {
	Properties = LoadConfig(configFileName)
}

func LoadConfig(configFileName string) *PropertyHolder {
	config := Properties

	file, err := os.Open(configFileName)
	if err != nil {
		log.Print(err)
		return config
	}
	defer file.Close()

	//read file
	rawMap := make(map[string]string)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) > 0 && line[0] == '#' {
			continue
		}

		pivot := strings.IndexAny(line, " ")

		if pivot > 0 && pivot < len(line)-1 {

			key := line[0:pivot]
			value := strings.Trim(line[pivot+1:], " ")
			rawMap[strings.ToLower(key)] = value
		}
	}

	if err := scanner.Err(); err != nil {
		logger.Fatal(err)
	}

	//parse
	t := reflect.TypeOf(config)
	v := reflect.ValueOf(config)
	n := t.Elem().NumField()
	//解析每一行参数
	for i := 0; i < n; i++ {
		field := t.Elem().Field(i)
		fieldVal := v.Elem().Field(i)

		key, ok := field.Tag.Lookup("cfg")
		if !ok {
			key = field.Name
		}
		value, ok := rawMap[strings.ToLower(key)]
		if ok {
			switch field.Type.Kind() {
			case reflect.String:
				fieldVal.SetString(value)
			case reflect.Int:
				intVal, err := strconv.ParseInt(value, 10, 64)
				if err == nil {
					fieldVal.SetInt(intVal)
				}
			case reflect.Bool:
				boolVal := "yes" == value
				fieldVal.SetBool(boolVal)
			case reflect.Slice:
				if field.Type.Elem().Kind() == reflect.String {
					slice := strings.Split(value, ",")
					fieldVal.Set(reflect.ValueOf(slice))
				}
			}
		}
	}
	return config
}
