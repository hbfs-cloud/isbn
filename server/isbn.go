package main

import (
	"strings"
	"unicode"
)

func normalizeISBN(in string) string {
	var b strings.Builder
	for _, r := range in {
		if unicode.IsDigit(r) || r == 'X' || r == 'x' {
			b.WriteRune(r)
		}
	}
	s := strings.ToUpper(b.String())
	if len(s) != 10 && len(s) != 13 {
		return ""
	}
	if len(s) == 10 && !validISBN10(s) {
		return ""
	}
	if len(s) == 13 && !validISBN13(s) {
		return ""
	}
	return s
}

func validISBN10(s string) bool {
	if len(s) != 10 {
		return false
	}
	sum := 0
	for i := 0; i < 9; i++ {
		c := s[i]
		if c < '0' || c > '9' {
			return false
		}
		sum += int(c-'0') * (10 - i)
	}
	last := s[9]
	v := 0
	if last == 'X' {
		v = 10
	} else if last >= '0' && last <= '9' {
		v = int(last - '0')
	} else {
		return false
	}
	sum += v
	return sum%11 == 0
}

func validISBN13(s string) bool {
	if len(s) != 13 {
		return false
	}
	sum := 0
	for i := 0; i < 13; i++ {
		c := s[i]
		if c < '0' || c > '9' {
			return false
		}
		w := 1
		if i%2 == 1 {
			w = 3
		}
		sum += int(c-'0') * w
	}
	return sum%10 == 0
}

func isbn10To13(s10 string) string {
	if len(s10) != 10 {
		return ""
	}
	core := "978" + s10[:9]
	sum := 0
	for i, c := range core {
		w := 1
		if i%2 == 1 {
			w = 3
		}
		sum += int(c-'0') * w
	}
	check := (10 - sum%10) % 10
	return core + string('0'+rune(check))
}

func isbn13To10(s13 string) string {
	if len(s13) != 13 || !strings.HasPrefix(s13, "978") {
		return ""
	}
	core := s13[3:12]
	sum := 0
	for i := 0; i < 9; i++ {
		sum += int(core[i]-'0') * (10 - i)
	}
	check := (11 - sum%11) % 11
	last := byte('0' + check)
	if check == 10 {
		last = 'X'
	}
	return core + string(last)
}

func bothISBNForms(isbn string) (isbn10, isbn13 string) {
	if len(isbn) == 13 {
		return isbn13To10(isbn), isbn
	}
	if len(isbn) == 10 {
		return isbn, isbn10To13(isbn)
	}
	return "", ""
}
