package main

type FieldInfo struct {
	Name string // 字段名
	Desc string // 字段描述
}

type KeyInfo struct {
	Name   string // 键名
	Type   string // 键类型
	Fields string // 键的字段列表
}

type EngineInfo struct {
	Name string // 引擎名
	Desc string // 引擎描述
}

type MysqlTable struct {
	Name       string      // 表名
	SqlStr     string      // sql语句
	Flds       []FieldInfo // 字段列表
	Keys       []KeyInfo   // 键列表
	Engine     EngineInfo  // 引擎
	IsChild    bool        // 是否是子表
	ChildNames []string    // 子表名列表
	LikeTbl    string      // like的表名
}
