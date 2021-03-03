/**
* @Author:zhoutao
* @Date:2021/2/28 上午9:13
* @Desc: ScoreBorder 是 'ZRANGEBYSCORE' 命令的最大值和最小值范围的strcut
 */

package sortedset

import (
	"errors"
	"strconv"
)

const (
	negativeInf int8 = -1
	positiveInf int8 = 1
)

type ScoreBorder struct {
	Inf     int8
	Value   float64
	Exclude bool //排除
}

func (b *ScoreBorder) greater(value float64) bool {
	if b.Inf == negativeInf {
		return false
	} else if b.Inf == positiveInf {
		return true
	}

	if b.Exclude {
		return b.Value > value
	} else {
		return b.Value >= value
	}
}

func (b *ScoreBorder) less(value float64) bool {
	if b.Inf == negativeInf {
		return true
	} else if b.Inf == positiveInf {
		return false
	}

	if b.Exclude {
		return b.Value < value
	} else {
		return b.Value <= value
	}
}

var positiveInfBorder = &ScoreBorder{
	Inf: positiveInf,
}

var negativeInfBorder = &ScoreBorder{
	Inf: negativeInf,
}

func ParseScoreBorder(s string) (*ScoreBorder, error) {
	if s == "inf" || s == "+inf" {
		return positiveInfBorder, nil
	}

	if s == "-inf" {
		return negativeInfBorder, nil
	}

	if s[0] == '(' {
		value, err := strconv.ParseFloat(s[1:], 64)
		if err != nil {
			return nil, errors.New("ERR : min or max value does  not a float")
		}
		return &ScoreBorder{
			Inf:     0,
			Value:   value,
			Exclude: true, //true
		}, nil
	} else {
		value, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return nil, errors.New("ERR : min or max value does not a float ")
		}
		return &ScoreBorder{
			Inf:     0,
			Value:   value,
			Exclude: false, //false
		}, nil
	}
}
