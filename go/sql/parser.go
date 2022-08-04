/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package sql

import (
	"regexp"
	"strconv"
	"strings"
)

var (
	// Compile解析并返回一个正则表达式。如果成功返回，该Regexp就可用于匹配文本。Compile() 或者 MustCompile()创建一个编译好的正则表达式对象
	// MustCompile类似Compile但会在解析失败时panic，主要用于全局正则表达式变量的安全初始化
	sanitizeQuotesRegexp                 = regexp.MustCompile("('[^']*')")
	renameColumnRegexp                   = regexp.MustCompile(`(?i)\bchange\s+(column\s+|)([\S]+)\s+([\S]+)\s+`)
	dropColumnRegexp                     = regexp.MustCompile(`(?i)\bdrop\s+(column\s+|)([\S]+)$`)
	renameTableRegexp                    = regexp.MustCompile(`(?i)\brename\s+(to|as)\s+`)
	autoIncrementRegexp                  = regexp.MustCompile(`(?i)\bauto_increment[\s]*=[\s]*([0-9]+)`)
	alterTableExplicitSchemaTableRegexps = []*regexp.Regexp{
		// ALTER TABLE `scm`.`tbl` something
		regexp.MustCompile(`(?i)\balter\s+table\s+` + "`" + `([^` + "`" + `]+)` + "`" + `[.]` + "`" + `([^` + "`" + `]+)` + "`" + `\s+(.*$)`),
		// ALTER TABLE `scm`.tbl something
		regexp.MustCompile(`(?i)\balter\s+table\s+` + "`" + `([^` + "`" + `]+)` + "`" + `[.]([\S]+)\s+(.*$)`),
		// ALTER TABLE scm.`tbl` something
		regexp.MustCompile(`(?i)\balter\s+table\s+([\S]+)[.]` + "`" + `([^` + "`" + `]+)` + "`" + `\s+(.*$)`),
		// ALTER TABLE scm.tbl something
		regexp.MustCompile(`(?i)\balter\s+table\s+([\S]+)[.]([\S]+)\s+(.*$)`),
	}
	alterTableExplicitTableRegexps = []*regexp.Regexp{
		// ALTER TABLE `tbl` something
		regexp.MustCompile(`(?i)\balter\s+table\s+` + "`" + `([^` + "`" + `]+)` + "`" + `\s+(.*$)`),
		// ALTER TABLE tbl something
		regexp.MustCompile(`(?i)\balter\s+table\s+([\S]+)\s+(.*$)`),
	}
	enumValuesRegexp = regexp.MustCompile("^enum[(](.*)[)]$")
)

type AlterTableParser struct {
	columnRenameMap        map[string]string
	droppedColumns         map[string]bool
	isRenameTable          bool
	isAutoIncrementDefined bool

	alterStatementOptions string
	alterTokens           []string

	explicitSchema string
	explicitTable  string
}

func NewAlterTableParser() *AlterTableParser {
	// 返回AlterTableParser结构体
	return &AlterTableParser{
		// make(map[string]string - 创建key为string，value为string的map集合
		columnRenameMap: make(map[string]string),
		droppedColumns:  make(map[string]bool),
	}
}

func NewParserFromAlterStatement(alterStatement string) *AlterTableParser {
	parser := NewAlterTableParser()
	parser.ParseAlterStatement(alterStatement)
	return parser
}

func (this *AlterTableParser) tokenizeAlterStatement(alterStatement string) (tokens []string, err error) {
	// rune=int32, It's to distinguish character values from integer values
	terminatingQuote := rune(0)
	f := func(c rune) bool {
		switch {
		case c == terminatingQuote:
			terminatingQuote = rune(0)
			return false
		case terminatingQuote != rune(0):
			return false
		case c == '\'':
			terminatingQuote = c
			return false
		case c == '(':
			terminatingQuote = ')'
			return false
		default:
			return c == ','
		}
	}

	// Golang 中的 strings.FieldsFunc() 函数用于在每次运行满足 f(c)的Unicode代码点c处拆分给定的字符串str并返回str的切片数组
	// func FieldsFunc(str string, f func(rune) bool) []string
	tokens = strings.FieldsFunc(alterStatement, f)
	for i := range tokens {
		tokens[i] = strings.TrimSpace(tokens[i])
	}
	return tokens, nil
}

func (this *AlterTableParser) sanitizeQuotesFromAlterStatement(alterStatement string) (strippedStatement string) {
	strippedStatement = alterStatement
	strippedStatement = sanitizeQuotesRegexp.ReplaceAllString(strippedStatement, "''")
	return strippedStatement
}

// 解析具体的alter变更
func (this *AlterTableParser) parseAlterToken(alterToken string) (err error) {
	{
		// rename
		allStringSubmatch := renameColumnRegexp.FindAllStringSubmatch(alterToken, -1)
		for _, submatch := range allStringSubmatch {
			if unquoted, err := strconv.Unquote(submatch[2]); err == nil {
				submatch[2] = unquoted
			}
			if unquoted, err := strconv.Unquote(submatch[3]); err == nil {
				submatch[3] = unquoted
			}
			// columnRenameMap 为重命名列与原始列的对应关系
			this.columnRenameMap[submatch[2]] = submatch[3]
		}
	}
	{
		// drop
		allStringSubmatch := dropColumnRegexp.FindAllStringSubmatch(alterToken, -1)
		for _, submatch := range allStringSubmatch {
			if unquoted, err := strconv.Unquote(submatch[2]); err == nil {
				submatch[2] = unquoted
			}
			this.droppedColumns[submatch[2]] = true
		}
	}
	{
		// rename table
		if renameTableRegexp.MatchString(alterToken) {
			this.isRenameTable = true
		}
	}
	{
		// auto_increment
		if autoIncrementRegexp.MatchString(alterToken) {
			this.isAutoIncrementDefined = true
		}
	}
	return nil
}

func (this *AlterTableParser) ParseAlterStatement(alterStatement string) (err error) {

	this.alterStatementOptions = alterStatement
	for _, alterTableRegexp := range alterTableExplicitSchemaTableRegexps {
		// FindStringSubmatch() 除了返回匹配的字符串外，还会返回子表达式的匹配项。
		// if 可以包含一个初始化语句（如：给一个变量赋值）。这种写法具有固定的格式（在初始化语句后方必须加上分号）; 先赋值，再判断
		if submatch := alterTableRegexp.FindStringSubmatch(this.alterStatementOptions); len(submatch) > 0 {
			this.explicitSchema = submatch[1]
			this.explicitTable = submatch[2]
			this.alterStatementOptions = submatch[3]
			break
		}
	}
	for _, alterTableRegexp := range alterTableExplicitTableRegexps {
		if submatch := alterTableRegexp.FindStringSubmatch(this.alterStatementOptions); len(submatch) > 0 {
			this.explicitTable = submatch[1]
			this.alterStatementOptions = submatch[2]
			break
		}
	}

	// 解析具体的ddl变更项
	alterTokens, _ := this.tokenizeAlterStatement(this.alterStatementOptions)
	for _, alterToken := range alterTokens {
		alterToken = this.sanitizeQuotesFromAlterStatement(alterToken)
		this.parseAlterToken(alterToken)
		this.alterTokens = append(this.alterTokens, alterToken)
	}
	return nil
}

// GetNonTrivialRenames 返回重命名列的映射 result[column] = renamed
func (this *AlterTableParser) GetNonTrivialRenames() map[string]string {
	result := make(map[string]string)
	for column, renamed := range this.columnRenameMap {
		if column != renamed {
			result[column] = renamed
		}
	}
	return result
}

func (this *AlterTableParser) HasNonTrivialRenames() bool {
	return len(this.GetNonTrivialRenames()) > 0
}

func (this *AlterTableParser) DroppedColumnsMap() map[string]bool {
	return this.droppedColumns
}

func (this *AlterTableParser) IsRenameTable() bool {
	return this.isRenameTable
}

func (this *AlterTableParser) IsAutoIncrementDefined() bool {
	return this.isAutoIncrementDefined
}

func (this *AlterTableParser) GetExplicitSchema() string {
	return this.explicitSchema
}

func (this *AlterTableParser) HasExplicitSchema() bool {
	return this.GetExplicitSchema() != ""
}

func (this *AlterTableParser) GetExplicitTable() string {
	return this.explicitTable
}

func (this *AlterTableParser) HasExplicitTable() bool {
	return this.GetExplicitTable() != ""
}

func (this *AlterTableParser) GetAlterStatementOptions() string {
	return this.alterStatementOptions
}

func ParseEnumValues(enumColumnType string) string {
	if submatch := enumValuesRegexp.FindStringSubmatch(enumColumnType); len(submatch) > 0 {
		return submatch[1]
	}
	return enumColumnType
}
