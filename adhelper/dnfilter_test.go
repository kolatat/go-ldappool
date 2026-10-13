package adhelper_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/go-ldap/ldap/v3"
)

// The corpus below pins go-ldap v3.4.15's verdicts for the DN and filter
// shapes that matter to Active Directory callers. go-ldap tightened DN parsing
// in v3.4.14 and filter validation in v3.4.15; a later bump that changes a
// verdict fails here instead of silently breaking a consumer's strings. Only
// accept or reject is asserted, never the error text. No test in this package
// runs in parallel mode (see TestAdhelperResetPasswordOverWire).

type verdictCase struct {
	name   string
	input  string
	accept bool
}

func TestDNParsingStrictness(t *testing.T) {
	cases := []verdictCase{
		{"escaped comma", `CN=Doe\, John,OU=Users,DC=corp,DC=example,DC=com`, true},
		{"escaped backslash by hex", `CN=a\5Cb,DC=x`, true},
		{"escaped quote by hex", `CN=a\22b,DC=x`, true},
		{"utf-8 value", "CN=José Núñez,OU=Users,DC=corp", true},
		{"multi-valued RDN", `CN=a+OU=b,DC=x`, true},
		{"unescaped double quote", `CN=a"b,DC=x`, false},
		{"unescaped angle brackets", `CN=a<b>c,DC=x`, false},
		{"unescaped comma in value", `CN=Doe, John,OU=Users,DC=corp,DC=example,DC=com`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ldap.ParseDN(tc.input)
			if got := err == nil; got != tc.accept {
				t.Errorf("ParseDN(%q) accepted = %v, want %v", tc.input, got, tc.accept)
			}
		})
	}
}

func TestFilterCompileStrictness(t *testing.T) {
	cases := []verdictCase{
		{"and of two", `(&(objectClass=user)(sAMAccountName=jdoe))`, true},
		{"or", `(|(cn=a)(cn=b))`, true},
		{"not", `(!(cn=a))`, true},
		{"matching rule bit and", `(userAccountControl:1.2.840.113556.1.4.803:=2)`, true},
		{"nested group membership", `(memberOf:1.2.840.113556.1.4.1941:=CN=Doe\2C John,OU=Users,DC=corp)`, true},
		{"unbalanced paren in value", `(cn=a(b)`, true},
		{"comma and space in value", `(cn=Doe, John)`, true},
		{"trailing garbage", `(&(a=b)garbage)`, false},
		{"backslash without hex digits", `(sAMAccountName=Doe\, John)`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ldap.CompileFilter(tc.input)
			if got := err == nil; got != tc.accept {
				t.Errorf("CompileFilter(%q) accepted = %v, want %v", tc.input, got, tc.accept)
			}
		})
	}
}

var (
	// dnLiteral matches a string that starts with, or has after a comma, a
	// common DN attribute type followed by an equals sign.
	dnLiteral = regexp.MustCompile(`(?i)(^|,)\s*(cn|ou|dc|uid|sn|o)\s*=`)
	// filterLiteral matches a parenthesised comparison, or a string that opens
	// an AND, OR or NOT group.
	filterLiteral = regexp.MustCompile(`^\s*(\(.*=.*\)|\(?[&|!]\s*\()`)
)

// TestNoHandBuiltDNOrFilterLiterals proves that the module's non-test Go
// sources contain no string literal shaped like a DN or an LDAP filter, so the
// set of hand-built DNs and filters that go-ldap's stricter parsing could break
// is empty and stays checked.
func TestNoHandBuiltDNOrFilterLiterals(t *testing.T) {
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	root := filepath.Dir(wd)

	skipDirs := map[string]bool{
		"internal": true, ".git": true, ".planning": true,
		".claude": true, ".idea": true, ".github": true,
	}

	var visited int
	var sawConnector bool
	var hits []string
	fset := token.NewFileSet()

	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if path != root && skipDirs[d.Name()] {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(d.Name(), ".go") || strings.HasSuffix(d.Name(), "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		visited++
		if d.Name() == "connector.go" {
			sawConnector = true
		}
		ast.Inspect(file, func(n ast.Node) bool {
			lit, ok := n.(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				return true
			}
			s, err := strconv.Unquote(lit.Value)
			if err != nil {
				return true
			}
			if dnLiteral.MatchString(s) || filterLiteral.MatchString(s) {
				hits = append(hits, fset.Position(lit.Pos()).String()+": "+lit.Value)
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}

	if visited == 0 {
		t.Fatalf("scan visited no files under %s", root)
	}
	if !sawConnector {
		t.Fatalf("scan visited %d files but never saw connector.go under %s", visited, root)
	}
	for _, h := range hits {
		t.Errorf("DN or filter shaped literal: %s", h)
	}
}
