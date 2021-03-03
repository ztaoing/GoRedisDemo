/**
* @Author:zhoutao
* @Date:2021/3/3 上午10:33
* @Desc:
 */

package gob

import (
	"bytes"
	"encoding/gob"
)

func Marshal(obj interface{}) ([]byte, error) {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	err := enc.Encode(obj)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func UnMarshal(data []byte, obj interface{}) error {
	dec := gob.NewDecoder(bytes.NewBuffer(data))
	return dec.Decode(obj)
}
