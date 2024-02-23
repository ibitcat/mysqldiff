package main

import (
	"log"
	"os"
	"regexp"
	"sort"
	"strings"
)

var (
	// regexps
	lineRe  = regexp.MustCompile(`.*?\n`)
	likeRe  = regexp.MustCompile(`like\s+?` + "`" + `(\S+)` + "`")
	tnmRe   = regexp.MustCompile(`CREATE\s+TABLE\s+` + "`" + `(\S+?)` + "`" + "(.+)")
	fldRe   = regexp.MustCompile(`^\s*` + "`" + `(\S+)` + "`" + `\s*(.+),`)
	keyRe   = regexp.MustCompile(`^\s*(.*?(KEY|INDEX))\s*(\S*)\s*\((.+?)\)`)
	knmRe   = regexp.MustCompile("`" + `(\S+)` + "`")
	ngnRe   = regexp.MustCompile(`^\s*\)\s*?ENGINE\s*=\s*(\S+)\s*(.*);`)
	childRe = regexp.MustCompile(`UNION=\((\S+)\)`)
	tnameRe = regexp.MustCompile("`" + `(\S+)` + "`")
)

func parseTableFromFile(file string) []*MysqlTable {
	bytes, err := os.ReadFile(file)
	if err != nil {
		panic("读取sql文件失败")
	}

	re := regexp.MustCompile(`(?s)CREATE\s+?TABLE.+?;`)
	tables := re.FindAllString(string(bytes), -1)
	if len(tables) == 0 {
		panic("sql文件为空")
	}

	// 解析所有的表结构
	tblList := make([]*MysqlTable, 0, len(tables))
	for _, tbstr := range tables {
		t := parseTable(tbstr)
		tblList = append(tblList, t)
	}
	parseTableEx(tblList)
	return tblList
}

func parseTableFromDB(dbname string) []*MysqlTable {
	_, err := MysqlDB.Exec("use " + dbname)
	if err != nil {
		log.Println("不存在库：", dbname)
		return nil
	}

	var tbNames []string
	rows, _ := MysqlDB.Query("show tables")
	defer rows.Close()

	var tbName string
	for rows.Next() {
		if err := rows.Scan(&tbName); err != nil {
			panic(err.Error())
		} else {
			tbNames = append(tbNames, tbName)
		}
	}

	tblList := make([]*MysqlTable, 0, len(tbNames))
	// show create table xxx;
	var tblStr string
	for _, tbnm := range tbNames {
		ddlRows := MysqlDB.QueryRow("show create table " + tbnm)
		err := ddlRows.Scan(&tbName, &tblStr)
		if err != nil {
			panic(err)
		} else {
			// log.Printf("%q\n", tblStr)
			t := parseTable(tblStr + ";")
			tblList = append(tblList, t)
		}
	}
	parseTableEx(tblList)
	return tblList
}

func parseTable(tblStr string) *MysqlTable {
	tblStr += "\n"

	lines := lineRe.FindAllString(tblStr, -1)
	t := MysqlTable{
		SqlStr:  tblStr,
		Flds:    make([]FieldInfo, 0, len(lines)),
		Keys:    make([]KeyInfo, 0, len(lines)),
		Engine:  EngineInfo{},
		IsChild: false,
	}

	step := ""
	for idx, line := range lines {
		line = strings.ReplaceAll(line, "\r", "") // 兼容windows("\r\n")
		if idx == 0 {
			tblEx := ""
			ret := tnmRe.FindStringSubmatch(line)
			if len(ret) < 2 {
				panic("解析表名错误, line:" + line)
			} else if len(ret) > 2 {
				tblEx = ret[2]
			}
			t.Name = ret[1]

			// 支持：create table `xxx` like `yyy`;
			if len(tblEx) > 0 {
				// 复制表结构
				ret := likeRe.FindStringSubmatch(tblEx)
				if len(ret) == 2 {
					t.LikeTbl = ret[1]
					step = "t_end"
					continue
				}
			}
			step = "tname_end" // 表名解析完成
		} else {
			// 解析字段
			if step == "tname_end" {
				ret := fldRe.FindStringSubmatch(line)
				if len(ret) == 3 {
					fieldName, fieldDesc := ret[1], ret[2]
					t.Flds = append(t.Flds, FieldInfo{fieldName, fieldDesc})
				} else {
					step = "tflds_end" // 字段解析完成
				}
			}

			// 解析键（包括主键和其他键）
			if step == "tflds_end" {
				ret := keyRe.FindStringSubmatch(line) // RRIMARY KEY (`id`) 或 KEY `key_idx` (`xx`, `yy`)
				if len(ret) == 5 {
					var keyType, keyName, keyFlds string
					keyType = ret[1]
					keyFlds = ret[4]
					if strings.Contains(keyType, "PRIMARY") {
						// primary key
						keyName = ""
					} else {
						// other key
						knmRet := knmRe.FindStringSubmatch(ret[3])
						if len(knmRet) == 2 {
							keyName = knmRet[1]
						}
					}

					// bugfix:修复多个键名之间有空格时，每次都要重新更新数据库的问题
					keyFlds = strings.ReplaceAll(keyFlds, " ", "")
					t.Keys = append(t.Keys, KeyInfo{keyName, keyType, keyFlds})
				} else {
					// sort key(按键名升序)
					sort.Slice(t.Keys, func(i, j int) bool {
						return t.Keys[i].Name < t.Keys[j].Name
					})

					step = "tkeys_end"
				}
			}

			// 解析engine
			if step == "tkeys_end" {
				ret := ngnRe.FindStringSubmatch(line)
				if len(ret) == 3 {
					t.Engine.Name = ret[1]
					t.Engine.Desc = ret[2]

					if t.Engine.Name == "MRG_MyISAM" {
						// myisam 分表
						ret := childRe.FindStringSubmatch(t.Engine.Desc)
						if len(ret) == 2 {
							child := strings.Split(ret[1], ",")
							if len(child) > 0 {
								t.ChildNames = make([]string, 0, len(child))
								for _, v := range child {
									nmRet := tnameRe.FindStringSubmatch(v)
									if len(nmRet) == 2 {
										t.ChildNames = append(t.ChildNames, nmRet[1])
									}
								}
							}
						}
					}

					step = "t_end"
					break
				}
			}
		}
	}

	// append to table list
	if step != "t_end" {
		panic("解析table错误, sql:\n" + tblStr)
	}
	return &t
}

func parseTableEx(tbls []*MysqlTable) {
	childList := make([]string, 0)
	likeMap := make(map[string]string)

	tblMap := make(map[string]*MysqlTable, len(tbls))
	for _, tbl := range tbls {
		tblMap[tbl.Name] = tbl

		// 分表处理
		if tbl.Engine.Name == "MRG_MyISAM" {
			childList = append(childList, tbl.ChildNames...)
		}

		// like处理
		// eg.：create table `xxx` like `yyy`;
		if len(tbl.LikeTbl) > 0 {
			likeMap[tbl.Name] = tbl.LikeTbl
		}
	}

	for _, tnm := range childList {
		if t, ok := tblMap[tnm]; ok {
			t.IsChild = true
		}
	}

	for tnm, lktnm := range likeMap {
		if lkt, ok := tblMap[lktnm]; ok {
			t := tblMap[tnm]
			t.Flds = append(t.Flds, lkt.Flds...)
			t.Keys = append(t.Keys, lkt.Keys...)
			t.Engine = lkt.Engine
		}
	}
}
