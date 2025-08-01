/*
 * Copyright 2023 CloudWeGo Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * MIT License
 *
 * Copyright (c) 2019-present Fenny and Contributors
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 * This file may have been modified by CloudWeGo authors. All CloudWeGo
 * Modifications are Copyright 2023 CloudWeGo Authors
 */

package decoder

import (
	"fmt"
	"reflect"

	"github.com/cloudwego/hertz/pkg/protocol"
	"github.com/cloudwego/hertz/pkg/route/param"
)

type fieldInfo struct {
	index       int
	parentIndex []int
	fieldName   string
	tagInfos    []TagInfo
	fieldType   reflect.Type
	config      *DecodeConfig
}

type baseTypeFieldTextDecoder struct {
	fieldInfo
	decoder TextDecoder
}

func (d *baseTypeFieldTextDecoder) Decode(req *protocol.Request, params param.Params, reqValue reflect.Value) error {
	var err error
	var text string
	var exist bool
	var defaultValue string
	for _, tagInfo := range d.tagInfos {
		if tagInfo.Skip || tagInfo.Key == jsonTag || tagInfo.Key == fileNameTag {
			if tagInfo.Key == jsonTag {
				defaultValue = tagInfo.Default
				found := checkRequireJSON(req, tagInfo)
				if found {
					err = nil
				} else {
					err = fmt.Errorf("'%s' field is a 'required' parameter, but the request body does not have this parameter '%s'", tagInfo.Value, tagInfo.JSONName)
				}
				if len(tagInfo.Default) != 0 && keyExist(req, tagInfo) {
					defaultValue = ""
				}
			}
			continue
		}
		text, exist = tagInfo.Getter(req, params, tagInfo.Value)
		defaultValue = tagInfo.Default
		if exist {
			err = nil
			break
		}
		if tagInfo.Required {
			err = fmt.Errorf("'%s' field is a 'required' parameter, but the request does not have this parameter", tagInfo.Value)
		}
	}
	if err != nil {
		return err
	}
	if len(text) == 0 && len(defaultValue) != 0 {
		text = toDefaultValue(d.fieldType, defaultValue)
	}
	if !exist && len(text) == 0 {
		return nil
	}

	// get the non-nil value for the parent field
	reqValue = GetFieldValue(reqValue, d.parentIndex)
	field := reqValue.Field(d.index)
	if field.Kind() == reflect.Ptr {
		t := field.Type()
		var ptrDepth int
		for t.Kind() == reflect.Ptr {
			t = t.Elem()
			ptrDepth++
		}
		var vv reflect.Value
		vv, err := stringToValue(t, text, req, params, d.config)
		if err != nil {
			return err
		}
		field.Set(ReferenceValue(vv, ptrDepth))
		return nil
	}

	// Non-pointer elems
	if field.CanAddr() {
		if tryTextUnmarshaler(field.Addr(), text) {
			return nil
		}
	}
	err = d.decoder.UnmarshalString(text, field, d.config.LooseZeroMode)
	if err != nil {
		return fmt.Errorf("unable to decode '%s' as %s: %w", text, d.fieldType.Name(), err)
	}

	return nil
}

func getBaseTypeTextDecoder(field reflect.StructField, index int, tagInfos []TagInfo, parentIdx []int, config *DecodeConfig) ([]fieldDecoder, error) {
	for idx, tagInfo := range tagInfos {
		switch tagInfo.Key {
		case pathTag:
			tagInfos[idx].SliceGetter = pathSlice
			tagInfos[idx].Getter = path
		case formTag:
			tagInfos[idx].SliceGetter = postFormSlice
			tagInfos[idx].Getter = postForm
		case queryTag:
			tagInfos[idx].SliceGetter = querySlice
			tagInfos[idx].Getter = query
		case cookieTag:
			tagInfos[idx].SliceGetter = cookieSlice
			tagInfos[idx].Getter = cookie
		case headerTag:
			tagInfos[idx].SliceGetter = headerSlice
			tagInfos[idx].Getter = header
		case jsonTag:
			// do nothing
		case rawBodyTag:
			tagInfos[idx].SliceGetter = rawBodySlice
			tagInfos[idx].Getter = rawBody
		case fileNameTag:
			// do nothing
		default:
		}
	}

	fieldType := field.Type
	for field.Type.Kind() == reflect.Ptr {
		fieldType = field.Type.Elem()
	}

	textDecoder, err := SelectTextDecoder(fieldType)
	if err != nil {
		return nil, err
	}

	return []fieldDecoder{&baseTypeFieldTextDecoder{
		fieldInfo: fieldInfo{
			index:       index,
			parentIndex: parentIdx,
			fieldName:   field.Name,
			tagInfos:    tagInfos,
			fieldType:   fieldType,
			config:      config,
		},
		decoder: textDecoder,
	}}, nil
}
