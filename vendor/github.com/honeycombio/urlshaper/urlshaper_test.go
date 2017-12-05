package urlshaper

import (
	"fmt"
	"net/url"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"strings"
	"testing"
)

type compPat struct {
	pat   string
	rawRE string
}

var pats = []compPat{
	{"/book/abcd", "^/book/abcd$"},
	{"/book/:isbn", "^/book/(?P<isbn>[^/]+)$"},
	{"/about/:lang/books", "^/about/(?P<lang>[^/]+)/books$"},
	{"/about/:lang/books/:isbn", "^/about/(?P<lang>[^/]+)/books/(?P<isbn>[^/]+)$"},
	{"/about/:lang/books/*", "^/about/(?P<lang>[^/]+)/books/.*$"},
	{"/abo[ut/", "^/abo[ut/$"},
	{"/about/:bo]ok", "^/about/(?P<bo]ok>[^/]+)$"},
	{"/about/:bo>ok", "^/about/(?P<bo>ok>[^/]+)$"},
}

func TestPatternCompile(t *testing.T) {
	for _, pat := range pats {
		p := Pattern{}
		p.Pat = pat.pat
		p.Compile()
		re, _ := regexp.Compile(pat.rawRE)
		testEquals(t, p.rawRE, pat.rawRE)
		testEquals(t, p.re, re)
	}
}

type lineShape struct {
	line string // input to the parser
	res  Result // the result of parsing the line
}

var parsePats = []*Pattern{
	{Pat: "/about/:lang/books"},
	{Pat: "/about/:lang/books/:isbn"},
	{Pat: "/about/:lang/books/*"},
}

var lineShapes = []lineShape{
	// no match
	{"/other/path", Result{
		URI:         "/other/path",
		Path:        "/other/path",
		Query:       "",
		QueryFields: url.Values{},
		PathFields:  url.Values{},
		Shape:       "/other/path",
		PathShape:   "/other/path",
		QueryShape:  "",
	}},
	// basic path match
	{"/about/en/books", Result{
		URI:         "/about/en/books",
		Path:        "/about/en/books",
		Query:       "",
		QueryFields: url.Values{},
		PathFields:  url.Values{"lang": []string{"en"}},
		Shape:       "/about/:lang/books",
		PathShape:   "/about/:lang/books",
		QueryShape:  "",
	}},
	{"/about/fr/books", Result{
		URI:         "/about/fr/books",
		Path:        "/about/fr/books",
		Query:       "",
		QueryFields: url.Values{},
		PathFields:  url.Values{"lang": []string{"fr"}},
		Shape:       "/about/:lang/books",
		PathShape:   "/about/:lang/books",
		QueryShape:  "",
	}},
	// resty + query
	{"/about/fr/books?isbn=123", Result{
		URI:         "/about/fr/books?isbn=123",
		Path:        "/about/fr/books",
		Query:       "isbn=123",
		QueryFields: url.Values{"isbn": []string{"123"}},
		PathFields:  url.Values{"lang": []string{"fr"}},
		Shape:       "/about/:lang/books?isbn=?",
		PathShape:   "/about/:lang/books",
		QueryShape:  "isbn=?",
	}},
	// resty + query
	{"http://example.com:8080/about/fr/books?isbn=123", Result{
		URI:         "http://example.com:8080/about/fr/books?isbn=123",
		Path:        "/about/fr/books",
		Query:       "isbn=123",
		QueryFields: url.Values{"isbn": []string{"123"}},
		PathFields:  url.Values{"lang": []string{"fr"}},
		Shape:       "/about/:lang/books?isbn=?",
		PathShape:   "/about/:lang/books",
		QueryShape:  "isbn=?",
	}},
	// multiple query params
	{"/about/fr/books?isbn=123&title=aoeu", Result{
		URI:         "/about/fr/books?isbn=123&title=aoeu",
		Path:        "/about/fr/books",
		Query:       "isbn=123&title=aoeu",
		QueryFields: url.Values{"isbn": []string{"123"}, "title": []string{"aoeu"}},
		PathFields:  url.Values{"lang": []string{"fr"}},
		Shape:       "/about/:lang/books?isbn=?&title=?",
		PathShape:   "/about/:lang/books",
		QueryShape:  "isbn=?&title=?",
	}},
	// alphebatize query params
	{"/about/fr/books?title=aoeu&isbn=123", Result{
		URI:         "/about/fr/books?title=aoeu&isbn=123",
		Path:        "/about/fr/books",
		Query:       "title=aoeu&isbn=123",
		QueryFields: url.Values{"isbn": []string{"123"}, "title": []string{"aoeu"}},
		PathFields:  url.Values{"lang": []string{"fr"}},
		Shape:       "/about/:lang/books?isbn=?&title=?",
		PathShape:   "/about/:lang/books",
		QueryShape:  "isbn=?&title=?",
	}},
	// multiple resty path vars
	{"/about/en/books/123456", Result{
		URI:         "/about/en/books/123456",
		Path:        "/about/en/books/123456",
		Query:       "",
		QueryFields: url.Values{},
		PathFields:  url.Values{"lang": []string{"en"}, "isbn": []string{"123456"}},
		Shape:       "/about/:lang/books/:isbn",
		PathShape:   "/about/:lang/books/:isbn",
		QueryShape:  "",
	}},
	{"/about/en/books/abcdc", Result{
		URI:         "/about/en/books/abcdc",
		Path:        "/about/en/books/abcdc",
		Query:       "",
		QueryFields: url.Values{},
		PathFields:  url.Values{"lang": []string{"en"}, "isbn": []string{"abcdc"}},
		Shape:       "/about/:lang/books/:isbn",
		PathShape:   "/about/:lang/books/:isbn",
		QueryShape:  "",
	}},
	// test star swallows stuff
	{"/about/en/books/foo/bar", Result{
		URI:         "/about/en/books/foo/bar",
		Path:        "/about/en/books/foo/bar",
		Query:       "",
		QueryFields: url.Values{},
		PathFields:  url.Values{"lang": []string{"en"}},
		Shape:       "/about/:lang/books/*",
		PathShape:   "/about/:lang/books/*",
		QueryShape:  "",
	}},
	// shouldn't match a pattern; keep original
	{"/about/en/foo/books/bar", Result{
		URI:         "/about/en/foo/books/bar",
		Path:        "/about/en/foo/books/bar",
		Query:       "",
		QueryFields: url.Values{},
		PathFields:  url.Values{},
		Shape:       "/about/en/foo/books/bar",
		PathShape:   "/about/en/foo/books/bar",
		QueryShape:  "",
	}},
	// no pattern match + query params
	{"/about/en/foo/books/bar?foo=bar", Result{
		URI:         "/about/en/foo/books/bar?foo=bar",
		Path:        "/about/en/foo/books/bar",
		Query:       "foo=bar",
		QueryFields: url.Values{"foo": []string{"bar"}},
		PathFields:  url.Values{},
		Shape:       "/about/en/foo/books/bar?foo=?",
		PathShape:   "/about/en/foo/books/bar",
		QueryShape:  "foo=?",
	}},
	// repeat query params
	{"/about/fr/books?isbn=123&title=aoeu&isbn=456", Result{
		URI:         "/about/fr/books?isbn=123&title=aoeu&isbn=456",
		Path:        "/about/fr/books",
		Query:       "isbn=123&title=aoeu&isbn=456",
		QueryFields: url.Values{"isbn": []string{"123", "456"}, "title": []string{"aoeu"}},
		PathFields:  url.Values{"lang": []string{"fr"}},
		Shape:       "/about/:lang/books?isbn=?&isbn=?&title=?",
		PathShape:   "/about/:lang/books",
		QueryShape:  "isbn=?&isbn=?&title=?",
	}},
}

func TestParse(t *testing.T) {
	pr := Parser{}
	pr.Patterns = parsePats
	// try and parse some lines
	for _, l := range lineShapes {
		r, _ := pr.Parse(l.line)
		testEquals(t, *r, l.res)
	}
	// verify the parser automatically compiled all patterns
	testEquals(t, pr.Patterns[0].rawRE, "^/about/(?P<lang>[^/]+)/books$")
	testEquals(t, pr.Patterns[1].rawRE, "^/about/(?P<lang>[^/]+)/books/(?P<isbn>[^/]+)$")
}

// helper function
func testEquals(t testing.TB, actual, expected interface{}, msg ...string) {
	if !reflect.DeepEqual(actual, expected) {
		message := strings.Join(msg, ", ")
		_, file, line, _ := runtime.Caller(1)

		t.Errorf(
			"%s:%d: %s -- actual(%T): %v, expected(%T): %v",
			filepath.Base(file),
			line,
			message,
			testDeref(actual),
			testDeref(actual),
			testDeref(expected),
			testDeref(expected),
		)
	}
}
func testDeref(v interface{}) interface{} {
	switch t := v.(type) {
	case *string:
		return fmt.Sprintf("*(%v)", *t)
	case *int64:
		return fmt.Sprintf("*(%v)", *t)
	case *float64:
		return fmt.Sprintf("*(%v)", *t)
	case *bool:
		return fmt.Sprintf("*(%v)", *t)
	default:
		return v
	}
}
