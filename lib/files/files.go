/**
* @Author:zhoutao
* @Date:2021/2/10 上午9:58
* @Desc:文件处理
 */

package files

import (
	"fmt"
	"io/ioutil"
	"mime/multipart"
	"os"
	"path"
)

func GetSize(f multipart.File) (int, error) {
	content, err := ioutil.ReadAll(f)
	return len(content), err
}

func GetExt(fileName string) string {
	return path.Ext(fileName)
}

func CheckNotExist(src string) bool {
	_, err := os.Stat(src)
	return os.IsNotExist(err)
}

func CheckPermission(src string) bool {
	_, err := os.Stat(src)
	return os.IsPermission(err)
}

func NotExistMkdir(src string) error {
	if notExist := CheckNotExist(src); notExist == true {
		if err := Mkdir(src); err != nil {
			return err
		}
	}
	return nil
}

func Mkdir(src string) error {
	err := os.MkdirAll(src, os.ModePerm)
	if err != nil {
		return err
	}
	return nil
}

func Open(name string, flag int, perm os.FileMode) (*os.File, error) {
	f, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func MustOpen(filename string, dir string) (*os.File, error) {
	perm := CheckPermission(dir)
	if perm == true {
		return nil, fmt.Errorf("permission deny dir:%s", dir)
	}

	err := NotExistMkdir(dir)
	if err != nil {
		return nil, fmt.Errorf("make dir:%s error ocur,err:%s", dir, err)
	}

	f, err := Open(dir+string(os.PathSeparator)+filename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("fail to open file,err:%s", err)
	}
	return f, nil
}
