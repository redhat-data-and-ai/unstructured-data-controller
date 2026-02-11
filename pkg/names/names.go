package names

import (
	"regexp"
	"strings"
)

var serviceAccountSuffixPatternRegex *regexp.Regexp

const (
	serviceAccountSuffixPatternString = `_(PLATFORMTEST|SANDBOX|DEV|PREPROD|PROD)_(APPUSER)`
)

func init() {
	serviceAccountSuffixPatternRegex = regexp.MustCompile(serviceAccountSuffixPatternString)
}

func buildName(parts ...string) string {
	b := &strings.Builder{}
	if _, err := b.WriteString(parts[0]); err != nil {
		return ""
	}
	for _, part := range parts[1:] {
		if err := b.WriteByte('_'); err != nil {
			return ""
		}
		if _, err := b.WriteString(part); err != nil {
			return ""
		}
	}
	return b.String()
}

// serviceaccount name for given dp, tool and env
func ServiceAccountNameForToolEnv(name, tool, env string) string {
	return buildName(
		strings.ToUpper(strings.ReplaceAll(name, "-", "_")),
		strings.ToUpper(strings.ReplaceAll(tool, "-", "_")),
		strings.ToUpper(strings.ReplaceAll(env, "rh", "")),
		SuffixServiceAccount,
	)
}

// serviceaccount role name for given serviceaccount
func ServiceAccountRole(serviceAccountName string) string {
	name := serviceAccountSuffixPatternRegex.ReplaceAllString(strings.ToUpper(serviceAccountName), "")
	return buildName(
		name,
		SuffixRole,
	)
}
