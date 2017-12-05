package urlshaper

import "fmt"

func Example() {
	prs := Parser{}

	// Add three sample patterns to our parser.
	// Patterns are always matched in list order; first match wins
	for _, pat := range []string{
		"/about",
		"/about/:lang",
		"/about/:lang/page",
	} {
		prs.Patterns = append(prs.Patterns, &Pattern{Pat: pat})
	}

	// Parse and generate the shape for a complex URL
	urlStr := "http://example.com:8080/about/english?title=Paradise&state=California"
	result, _ := prs.Parse(urlStr)

	fmt.Printf(`Original URL: %s
URI: %s
Path: %s
Query: %s
QueryFields: %+v
PathFields: %+v
Shape: %s
PathShape: %s
QueryShape: %s
`, urlStr, result.URI, result.Path, result.Query, result.QueryFields,
		result.PathFields, result.Shape, result.PathShape, result.QueryShape)

	// Output:
	// Original URL: http://example.com:8080/about/english?title=Paradise&state=California
	// URI: http://example.com:8080/about/english?title=Paradise&state=California
	// Path: /about/english
	// Query: title=Paradise&state=California
	// QueryFields: map[title:[Paradise] state:[California]]
	// PathFields: map[lang:[english]]
	// Shape: /about/:lang?state=?&title=?
	// PathShape: /about/:lang
	// QueryShape: state=?&title=?

}
