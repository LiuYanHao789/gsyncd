package config

// A deliberately small TOML subset parser. Keeping it in-tree means gsyncd
// builds and runs with zero third party modules: a single static binary is the
// whole point of the rewrite.

import (
	"fmt"
	"strconv"
	"strings"
)

type tomlValue any

type tomlTable map[string]tomlValue

// parseTOML parses the subset: comments, [table], [[array of tables]],
// key = value with strings, ints, floats, bools, arrays and inline tables.
func parseTOML(src string) (tomlTable, error) {
	root := tomlTable{}
	cur := root
	lines := strings.Split(src, "\n")
	for ln := 0; ln < len(lines); ln++ {
		line := stripComment(lines[ln])
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		// A multi-line array is the one construct people actually need for a
		// long exclude list, and TOML allows it. Keep pulling lines until the
		// brackets balance; a one-line array is unaffected because it is
		// already balanced.
		if eq := indexUnquoted(line, '='); eq >= 0 {
			val := line[eq+1:]
			for bracketDepth(val) > 0 && ln+1 < len(lines) {
				ln++
				val += " " + strings.TrimSpace(stripComment(lines[ln]))
			}
			line = line[:eq+1] + val
		}
		if strings.HasPrefix(line, "[[") {
			if !strings.HasSuffix(line, "]]") {
				return nil, fmt.Errorf("line %d: bad table array header", ln+1)
			}
			name := strings.TrimSpace(line[2 : len(line)-2])
			// A dotted array header ([[sync.ssh]]) belongs to its parent table,
			// not to a top-level key literally named "sync.ssh".
			parts := strings.Split(name, ".")
			parent := root
			if len(parts) > 1 {
				p, err := resolveTable(root, strings.Join(parts[:len(parts)-1], "."))
				if err != nil {
					return nil, fmt.Errorf("line %d: %w", ln+1, err)
				}
				parent = p
			}
			leaf := strings.Trim(strings.TrimSpace(parts[len(parts)-1]), "\"'")
			arr, _ := parent[leaf].([]tomlTable)
			t := tomlTable{}
			parent[leaf] = append(arr, t)
			cur = t
			continue
		}
		if strings.HasPrefix(line, "[") {
			if !strings.HasSuffix(line, "]") {
				return nil, fmt.Errorf("line %d: bad table header", ln+1)
			}
			name := strings.TrimSpace(line[1 : len(line)-1])
			t, err := resolveTable(root, name)
			if err != nil {
				return nil, fmt.Errorf("line %d: %w", ln+1, err)
			}
			cur = t
			continue
		}
		eq := indexUnquoted(line, '=')
		if eq < 0 {
			return nil, fmt.Errorf("line %d: expected key = value", ln+1)
		}
		key := strings.TrimSpace(line[:eq])
		key = strings.Trim(key, "\"'")
		val, err := parseValue(strings.TrimSpace(line[eq+1:]))
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", ln+1, err)
		}
		if _, exists := cur[key]; exists {
			// TOML forbids duplicate keys. Last-one-wins silently changed
			// target/log/delete depending on file order.
			return nil, fmt.Errorf("line %d: duplicate key %q", ln+1, key)
		}
		cur[key] = val
	}
	return root, nil
}

// resolveTable walks a possibly dotted header such as "sync.ssh", creating
// intermediate tables. When a segment names an array of tables the last element
// is used, which is what "[[sync]]" followed by "[sync.ssh]" means.
func resolveTable(root tomlTable, name string) (tomlTable, error) {
	parts := strings.Split(name, ".")
	cur := root
	for i, p := range parts {
		p = strings.Trim(strings.TrimSpace(p), "\"'")
		if p == "" {
			return nil, fmt.Errorf("empty table name segment in %q", name)
		}
		switch v := cur[p].(type) {
		case tomlTable:
			cur = v
		case []tomlTable:
			if len(v) == 0 {
				return nil, fmt.Errorf("table %q has no element", p)
			}
			cur = v[len(v)-1]
		case nil:
			t := tomlTable{}
			cur[p] = t
			cur = t
		default:
			return nil, fmt.Errorf("%q is already a value, not a table", strings.Join(parts[:i+1], "."))
		}
	}
	return cur, nil
}

// bracketDepth returns the number of unclosed [ and { in s, ignoring brackets
// inside strings. It is used to fold a multi-line array back into one logical
// line before parsing.
func bracketDepth(s string) int {
	depth := 0
	inStr := byte(0)
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case inStr != 0:
			// Only a basic ("...") string has backslash escapes. A literal
			// ('...') string ends at the next quote even after a backslash.
			if inStr == '"' && c == '\\' {
				i++
			} else if c == inStr {
				inStr = 0
			}
		case c == '"' || c == '\'':
			inStr = c
		case c == '[' || c == '{':
			depth++
		case c == ']' || c == '}':
			depth--
		}
	}
	return depth
}

func stripComment(s string) string {
	inStr := byte(0)
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case inStr != 0:
			// Only a basic ("...") string has backslash escapes. A literal
			// ('...') string ends at the next quote even after a backslash.
			if inStr == '"' && c == '\\' {
				i++
			} else if c == inStr {
				inStr = 0
			}
		case c == '"' || c == '\'':
			inStr = c
		case c == '#':
			return s[:i]
		}
	}
	return s
}

func indexUnquoted(s string, target byte) int {
	depth := 0
	inStr := byte(0)
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case inStr != 0:
			// Only a basic ("...") string has backslash escapes. A literal
			// ('...') string ends at the next quote even after a backslash.
			if inStr == '"' && c == '\\' {
				i++
			} else if c == inStr {
				inStr = 0
			}
		case c == '"' || c == '\'':
			inStr = c
		case c == '[' || c == '{':
			depth++
		case c == ']' || c == '}':
			depth--
		case c == target && depth == 0:
			return i
		}
	}
	return -1
}

func parseValue(s string) (tomlValue, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, fmt.Errorf("empty value")
	}
	switch {
	case s[0] == '"':
		return unquote(s)
	case s[0] == '\'':
		if len(s) < 2 || s[len(s)-1] != '\'' {
			return nil, fmt.Errorf("unterminated string")
		}
		return s[1 : len(s)-1], nil
	case s[0] == '[':
		if s[len(s)-1] != ']' {
			return nil, fmt.Errorf("unterminated array")
		}
		body := strings.TrimSpace(s[1 : len(s)-1])
		if body == "" {
			return []tomlValue{}, nil
		}
		var out []tomlValue
		for _, part := range splitTop(body) {
			v, err := parseValue(part)
			if err != nil {
				return nil, err
			}
			out = append(out, v)
		}
		return out, nil
	case s[0] == '{':
		if s[len(s)-1] != '}' {
			return nil, fmt.Errorf("unterminated inline table")
		}
		t := tomlTable{}
		body := strings.TrimSpace(s[1 : len(s)-1])
		if body == "" {
			return t, nil
		}
		for _, part := range splitTop(body) {
			eq := indexUnquoted(part, '=')
			if eq < 0 {
				return nil, fmt.Errorf("bad inline table entry %q", part)
			}
			k := strings.Trim(strings.TrimSpace(part[:eq]), "\"'")
			v, err := parseValue(part[eq+1:])
			if err != nil {
				return nil, err
			}
			t[k] = v
		}
		return t, nil
	case s == "true":
		return true, nil
	case s == "false":
		return false, nil
	}
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return i, nil
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f, nil
	}
	// Bare words are accepted as strings for convenience.
	return s, nil
}

func unquote(s string) (string, error) {
	if len(s) < 2 || s[len(s)-1] != '"' {
		return "", fmt.Errorf("unterminated string")
	}
	var b strings.Builder
	for i := 1; i < len(s)-1; i++ {
		c := s[i]
		if c != '\\' {
			b.WriteByte(c)
			continue
		}
		i++
		if i >= len(s)-1 {
			return "", fmt.Errorf("trailing backslash")
		}
		switch s[i] {
		case 'n':
			b.WriteByte('\n')
		case 't':
			b.WriteByte('\t')
		case 'r':
			b.WriteByte('\r')
		case '"':
			b.WriteByte('"')
		case '\\':
			b.WriteByte('\\')
		default:
			b.WriteByte(s[i])
		}
	}
	return b.String(), nil
}

func splitTop(s string) []string {
	var out []string
	depth := 0
	inStr := byte(0)
	start := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case inStr != 0:
			// Only a basic ("...") string has backslash escapes. A literal
			// ('...') string ends at the next quote even after a backslash.
			if inStr == '"' && c == '\\' {
				i++
			} else if c == inStr {
				inStr = 0
			}
		case c == '"' || c == '\'':
			inStr = c
		case c == '[' || c == '{':
			depth++
		case c == ']' || c == '}':
			depth--
		case c == ',' && depth == 0:
			out = append(out, strings.TrimSpace(s[start:i]))
			start = i + 1
		}
	}
	if strings.TrimSpace(s[start:]) != "" {
		out = append(out, strings.TrimSpace(s[start:]))
	}
	return out
}
