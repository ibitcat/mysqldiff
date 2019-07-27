package main

import (
	"fmt"
	"log"
	"regexp"
)

func mysqlExec(fatal bool, sql string, args ...interface{}) error {
	sqlstr := fmt.Sprintf(sql, args...)
	_, err := MysqlDB.Exec(sqlstr)
	if err != nil {
		if !fatal {
			return err
		} else {
			panic(fmt.Sprintf("err: %s \n\nsql: %s", err.Error(), sqlstr))
		}
	}
	return err
}

func dropAndUse(dbname string) {
	mysqlExec(false, "drop database %s", dbname)
	mysqlExec(true, "CREATE DATABASE %s DEFAULT CHARSET utf8 COLLATE utf8_general_ci", dbname)
	mysqlExec(true, "use %s", dbname)
}

func mysqlDiffDB(dbBase, dbFile []*MysqlTable) []string {
	oldMap := make(map[string]*MysqlTable, len(dbBase))
	for _, t := range dbBase {
		oldMap[t.Name] = t
	}

	var upsql []string
	newMap := make(map[string]bool, len(dbFile))
	for _, nt := range dbFile {
		newMap[nt.Name] = true
		if ot, ok := oldMap[nt.Name]; !ok {
			// 新增的
			upsql = append(upsql, nt.SqlStr)
		} else {
			// 变化的
			if !nt.IsChild {
				// 对比table(不对比子表)
				mysqlDiffTable(ot, nt, &upsql)
			}
		}
	}

	// 删除的
	for _, t := range dbBase {
		if _, ok := newMap[t.Name]; !ok {
			upsql = append(upsql, "drop table "+t.Name)
		}
	}
	return upsql
}

func mysqlDiffTable(ot, nt *MysqlTable, upsql *[]string) {
	if ot.Name != nt.Name {
		panic("表名不一致")
	}

	// 对比field
	sqlUp := mysqlDiffField(ot, nt)

	// 对比key
	sqlDrop, sqlAdd := mysqlDiffKey(ot, nt)

	for _, str := range sqlDrop {
		*upsql = append(*upsql, str)
	}
	for _, str := range sqlUp {
		*upsql = append(*upsql, str)
	}
	for _, str := range sqlAdd {
		*upsql = append(*upsql, str)
	}
}

func mysqlDiffKey(ot, nt *MysqlTable) ([]string, []string) {
	oKeys := ot.Keys
	nKeys := nt.Keys
	oMap := make(map[string]bool, len(oKeys))
	nMap := make(map[string]bool, len(nKeys))
	for _, k := range nKeys {
		nMap[k.Name] = true
	}

	// 先drop
	ignoreMap := make(map[string]bool)
	sqlDrop := make([]string, 0)
	sqlAdd := make([]string, 0)
	for _, k := range oKeys {
		if _, ok := nMap[k.Name]; !ok {
			ignoreMap[k.Name] = true

			// mother
			// eg.: alter table xxx drop keytype keyname
			sqlstr := fmt.Sprintf("alter table %s drop %s %s", ot.Name, k.Type, k.Name)
			sqlDrop = append(sqlDrop, sqlstr)

			// child
			for _, cnm := range ot.ChildNames {
				sqlstr := fmt.Sprintf("alter table %s drop %s %s", cnm, k.Type, k.Name)
				sqlDrop = append(sqlDrop, sqlstr)
			}
		} else {
			oMap[k.Name] = true
		}
	}

	// 新增的和变化的
	oIdx := 0
	for _, nk := range nKeys {
		// 找一个基准
		var kp *KeyInfo
		for oi, k := range oKeys {
			if oi >= oIdx {
				if _, ok := ignoreMap[k.Name]; !ok {
					kp = &k
					break
				} else {
					oIdx += 1
				}
			}
		}

		var op string
		if kp != nil {
			if kp.Name != nk.Name {
				if _, ok := oMap[kp.Name]; !ok {
					op = "add"
				} else {
					op = "modify"
					ignoreMap[kp.Name] = true
				}
			} else if kp.Fields != nk.Fields || kp.Type != nk.Type {
				op = "modify"
				oIdx += 1
			} else {
				// no change
				oIdx += 1
			}
		} else {
			op = "add"
		}

		if len(op) > 0 {
			// key的modify,要先drop可以,再add回去
			if op == "modify" {
				sqlstr := fmt.Sprintf("alter table %s drop %s %s", nt.Name, nk.Type, nk.Name)
				sqlDrop = append(sqlDrop, sqlstr)

				// child
				for _, cnm := range ot.ChildNames {
					sqlstr := fmt.Sprintf("alter table %s drop %s %s", cnm, nk.Type, nk.Name)
					sqlDrop = append(sqlDrop, sqlstr)
				}
			}

			// add
			// eg.: alter table xxx add keytype keyname (keyfield)
			sqlstr := fmt.Sprintf("alter table %s add %s %s (%s)", nt.Name, nk.Type, nk.Name, nk.Fields)
			sqlAdd = append(sqlAdd, sqlstr)

			// child
			for _, cnm := range ot.ChildNames {
				sqlstr := fmt.Sprintf("alter table %s add %s %s (%s)", cnm, nk.Type, nk.Name, nk.Fields)
				sqlAdd = append(sqlAdd, sqlstr)
			}
		}
	}
	return sqlDrop, sqlAdd
}

func mysqlDiffField(ot, nt *MysqlTable) []string {
	oFlds := ot.Flds
	nFlds := nt.Flds
	oMap := make(map[string]string, len(oFlds))
	nMap := make(map[string]string, len(nFlds))
	for _, f := range nFlds {
		nMap[f.Name] = f.Desc
	}

	// 先drop
	ignoreMap := make(map[string]bool)
	sqlUp := make([]string, 0)
	for _, f := range oFlds {
		if _, ok := nMap[f.Name]; !ok {
			ignoreMap[f.Name] = true

			// mother
			sqlstr := fmt.Sprintf("alter table %s %s `%s`", ot.Name, "drop", f.Name)
			sqlUp = append(sqlUp, sqlstr)

			// child
			for _, cnm := range ot.ChildNames {
				sqlstr := fmt.Sprintf("alter table %s %s `%s`", cnm, "drop", f.Name)
				sqlUp = append(sqlUp, sqlstr)
			}
		} else {
			oMap[f.Name] = f.Desc
		}
	}

	// 新增的和变化的
	oIdx := 0
	lastFld := ""
	for _, nf := range nFlds {
		// 找一个基准
		var fp *FieldInfo
		for oi, f := range oFlds {
			if oi >= oIdx {
				if _, ok := ignoreMap[f.Name]; !ok {
					fp = &f
					break
				} else {
					oIdx += 1
				}
			}
		}

		var op string
		var last = lastFld
		lastFld = nf.Name
		if fp != nil {
			if fp.Name != nf.Name {
				if _, ok := oMap[nf.Name]; !ok {
					op = "add"
				} else {
					op = "modify"
					ignoreMap[nf.Name] = true
				}
			} else if fp.Desc != nf.Desc {
				// eg.: alter table xxx modify `yyy` desc pos;
				op = "modify"
				oIdx += 1
			} else {
				// no change
				oIdx += 1
			}
		} else {
			// 新加
			// eg.: alter table xxx add `yyy` desc pot;
			op = "add"
		}

		if len(op) > 0 {
			var pos string
			if len(last) == 0 {
				pos = "first"
			} else {
				pos = "after " + last
			}

			// mother
			upstr := fmt.Sprintf("alter table %s %s `%s` %s %s", nt.Name, op, nf.Name, nf.Desc, pos)
			sqlUp = append(sqlUp, upstr)

			//child
			for _, cnm := range nt.ChildNames {
				upstr := fmt.Sprintf("alter table %s %s `%s` %s %s", cnm, op, nf.Name, nf.Desc, pos)
				sqlUp = append(sqlUp, upstr)
			}
		}
	}
	return sqlUp
}

func mysqlDiffUpdate(file, dbname string) {
	tmpDB_A := dbname + "_temp_a" //当前正式库的实验库
	tmpDB_B := dbname + "_temp_b" //sql文件创建的实验库
	defer func() {
		mysqlExec(false, "drop database "+tmpDB_A)
		mysqlExec(false, "drop database "+tmpDB_B)
		if err := recover(); err != nil {
			log.Println("数据库对比失败：\n", err)
		}
	}()

	// 1. 解析sql文件,并创建临时库B
	log.Printf("1. 解析【%s】，并创建临时库【%s】\n", file, tmpDB_B)
	dbFile := parseTableFromFile(file)
	if len(dbFile) == 0 {
		log.Println("sql文件解析错误, 请检查...")
		return
	}
	dropAndUse(tmpDB_B)
	for _, t := range dbFile {
		mysqlExec(true, t.SqlStr)
	}
	log.Println("done!\n")

	// 2. 读取正式库的表结构
	log.Printf("2. 读取正式库【%s】的表结构\n", dbname)
	err := mysqlExec(false, "use "+dbname)
	if err != nil {
		log.Println("    正式库不存在，尝试创建...")
		mysqlExec(true, "CREATE DATABASE %s DEFAULT CHARSET utf8 COLLATE utf8_general_ci", dbname)
	}
	dbBase := parseTableFromDB(dbname)
	log.Println("done!\n")

	// 3. 对比表结构差异
	log.Printf("3. 对比表结构差异\n")
	upsql := mysqlDiffDB(dbBase, dbFile)
	if len(upsql) == 0 {
		log.Println("done! 无差异\n")
		return
	} else {
		log.Printf("存在【%d】个差异", len(upsql))
		re := regexp.MustCompile(`CREATE\s+?TABLE\s+?` + "`" + `\S+` + "`")
		for i, v := range upsql {
			brief := re.FindString(v)
			if len(brief) > 0 {
				v = brief + " ..."
			}
			log.Printf("    %5d : %s", i+1, v)
		}
	}
	log.Println("done!\n")

	// 4. 创建实验库
	log.Printf("4. 创建实验库\n")
	dropAndUse(tmpDB_A)
	for _, t := range dbBase {
		log.Printf("    正在创建实验表: %s \n", t.Name)
		mysqlExec(true, t.SqlStr)
	}
	log.Println("done!\n")
	if onlyCk {
		return
	}

	// 5. 测试差异能否在实验库执行成功
	log.Printf("5. 测试差异能否在实验库执行成功\n")
	for _, v := range upsql {
		mysqlExec(true, v)
	}
	log.Println("done! 差异执行通过测试\n")

	// 6. 差异应用到正式库
	log.Printf("6. 差异应用到正式库\n")
	mysqlExec(true, "use %s", dbname)
	for _, v := range upsql {
		mysqlExec(true, v)
	}
	log.Println("done! 数据库维护完成!\n")
}
