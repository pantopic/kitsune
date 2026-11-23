package main

import (
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/pantopic/kitsune/module/pkg/globals"
	"github.com/pantopic/kitsune/module/pkg/pb"
)

var (
	ErrMessageAttributesInvalid = errors.New("Message attributes are invalid")
	attributeNamePattern        = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)
	numberPattern               = regexp.MustCompile(`^[+-]?([0-9]+(\.[0-9]*)?|\.[0-9]+)([eE][+-]?[0-9]+)?$`)
	traceRootPattern            = regexp.MustCompile(`^1-[0-9a-f]{8}-[0-9a-f]{24}$`)
	traceParentPattern          = regexp.MustCompile(`^[0-9a-f]{16}$`)
)

func validateMessageAttributes(attributes map[string]*pb.MessageAttribute, system bool) (uint64, error) {
	if !system && uint64(len(attributes)) > globals.MAX_MESSAGE_ATTRIBUTES() {
		return 0, ErrMessageAttributesInvalid
	}
	var size uint64
	for name, attr := range attributes {
		if attr == nil || attr.Invalid {
			return 0, ErrMessageAttributesInvalid
		}
		if system {
			if name != "AWSTraceHeader" || attr.DataType != "String" || !attr.StringValueSet || attr.BinaryValueSet || !validTraceHeader(attr.StringValue) {
				return 0, ErrMessageAttributesInvalid
			}
			continue
		}
		lowerName := strings.ToLower(name)
		if len(name) == 0 || len(name) > 256 || !attributeNamePattern.MatchString(name) ||
			strings.HasPrefix(name, ".") || strings.HasSuffix(name, ".") || strings.Contains(name, "..") ||
			strings.HasPrefix(lowerName, "aws.") || strings.HasPrefix(lowerName, "amazon.") {
			return 0, ErrMessageAttributesInvalid
		}
		if len(attr.DataType) == 0 || len(attr.DataType) > 256 || !validMessageBody(attr.DataType) {
			return 0, ErrMessageAttributesInvalid
		}
		base, suffix, custom := strings.Cut(attr.DataType, ".")
		if custom && suffix == "" {
			return 0, ErrMessageAttributesInvalid
		}
		switch base {
		case "String", "Number":
			if !attr.StringValueSet || attr.BinaryValueSet || !validMessageBody(attr.StringValue) || (base == "Number" && !validAttributeNumber(attr.StringValue)) {
				return 0, ErrMessageAttributesInvalid
			}
			size += uint64(len(attr.StringValue))
		case "Binary":
			if !attr.BinaryValueSet || attr.StringValueSet || len(attr.BinaryValue) == 0 {
				return 0, ErrMessageAttributesInvalid
			}
			size += uint64(len(attr.BinaryValue))
		default:
			return 0, ErrMessageAttributesInvalid
		}
		size += uint64(len(name) + len(attr.DataType))
	}
	return size, nil
}

func messageAttributeSize(attributes map[string]*pb.MessageAttribute) uint64 {
	var size uint64
	for name, attr := range attributes {
		if attr != nil {
			size += uint64(len(name) + len(attr.DataType) + len(attr.StringValue) + len(attr.BinaryValue))
		}
	}
	return size
}

func validAttributeNumber(value string) bool {
	if !numberPattern.MatchString(value) {
		return false
	}
	mantissa, exponentText, hasExponent := strings.Cut(strings.ToLower(value), "e")
	exponent := 0
	if hasExponent {
		parsed, err := strconv.ParseInt(exponentText, 10, 32)
		if err != nil {
			return false
		}
		exponent = int(parsed)
	}
	mantissa = strings.TrimLeft(mantissa, "+-")
	whole, fraction, _ := strings.Cut(mantissa, ".")
	digits := whole + fraction
	first := strings.IndexFunc(digits, func(r rune) bool { return r != '0' })
	if first < 0 {
		return true
	}
	significant := strings.TrimRight(digits[first:], "0")
	order := len(whole) - first - 1 + exponent
	return len(significant) <= 38 && order >= -128 && order <= 126 && (order != 126 || significant == "1")
}

func validTraceHeader(value string) bool {
	if !validMessageBody(value) {
		return false
	}
	fields := make(map[string]bool)
	for _, field := range strings.Split(value, ";") {
		key, content, ok := strings.Cut(field, "=")
		if !ok || key == "" || content == "" || fields[key] {
			return false
		}
		fields[key] = true
		switch key {
		case "Root":
			if !traceRootPattern.MatchString(content) {
				return false
			}
		case "Parent":
			if !traceParentPattern.MatchString(content) {
				return false
			}
		case "Sampled":
			if content != "0" && content != "1" && content != "?" {
				return false
			}
		}
	}
	return fields["Root"]
}

func normalizedMessageAttributes(attributes map[string]*pb.MessageAttribute) (map[string]*pb.MessageAttribute, error) {
	result := make(map[string]*pb.MessageAttribute, len(attributes))
	for name, attr := range attributes {
		value := *attr
		if value.DataType == "Number" || strings.HasPrefix(value.DataType, "Number.") {
			var err error
			value.StringValue, err = normalizeAttributeNumber(value.StringValue)
			if err != nil {
				return nil, err
			}
		}
		result[name] = &value
	}
	return result, nil
}

func normalizeAttributeNumber(value string) (string, error) {
	negative := strings.HasPrefix(value, "-")
	mantissa, exponentText, hasExponent := strings.Cut(strings.ToLower(value), "e")
	exponent := 0
	if hasExponent {
		parsed, err := strconv.ParseInt(exponentText, 10, 32)
		if err != nil {
			return "", ErrMessageAttributesInvalid
		}
		exponent = int(parsed)
	}
	mantissa = strings.TrimLeft(mantissa, "+-")
	whole, fraction, _ := strings.Cut(mantissa, ".")
	digits := whole + fraction
	first := strings.IndexFunc(digits, func(r rune) bool { return r != '0' })
	if first < 0 {
		return "0", nil
	}
	position := len(whole) - first + exponent
	digits = strings.TrimRight(digits[first:], "0")
	switch {
	case position <= 0:
		value = "0." + strings.Repeat("0", -position) + digits
	case position >= len(digits):
		value = digits + strings.Repeat("0", position-len(digits))
	default:
		value = digits[:position] + "." + digits[position:]
	}
	if negative {
		return "-" + value, nil
	}
	return value, nil
}

func attributeDigest(attributes map[string]*pb.MessageAttribute) string {
	if len(attributes) == 0 {
		return ""
	}
	names := make([]string, 0, len(attributes))
	for name := range attributes {
		names = append(names, name)
	}
	sort.Strings(names)
	var data []byte
	appendField := func(value []byte) {
		data = binary.BigEndian.AppendUint32(data, uint32(len(value)))
		data = append(data, value...)
	}
	for _, name := range names {
		attr := attributes[name]
		appendField([]byte(name))
		appendField([]byte(attr.DataType))
		if attr.BinaryValueSet {
			data = append(data, 2)
			appendField(attr.BinaryValue)
		} else {
			data = append(data, 1)
			appendField([]byte(attr.StringValue))
		}
	}
	digest := md5.Sum(data)
	return hex.EncodeToString(digest[:])
}

func selectMessageAttributes(attributes map[string]*pb.MessageAttribute, names []string) map[string]*pb.MessageAttribute {
	selected := make(map[string]*pb.MessageAttribute)
	for name, attr := range attributes {
		for _, pattern := range names {
			if pattern == "All" || pattern == ".*" || pattern == name ||
				(strings.HasSuffix(pattern, ".*") && strings.HasPrefix(name, strings.TrimSuffix(pattern, "*"))) {
				selected[name] = attr
				break
			}
		}
	}
	return selected
}
