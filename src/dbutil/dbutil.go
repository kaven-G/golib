package dbutil

import (
	"database/sql"
	"logger"
	"strings"
)

// 数据库操作对象，其包含了当前数据库的连接及操作方法

type SqlObj struct {
	// sql.DB对象
	db *sql.DB
	// sql语句
	sql strings.Builder
	// sql语句参数值
	params []interface{}
}

// 创建SqlObj对象
func NewSqlObj(driverName, dataSourceName string) *SqlObj {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		logger.Error(err)
	}

	obj := &SqlObj{db: db}
	logger.Info("Database connection created successfully")

	return obj
}

// 关闭数据库连接
func (s *SqlObj) Close() {
	s.Clear()
	if err := s.db.Close(); err != nil {
		logger.Error("Failed to close database connection")
	}
	logger.Info("Close database connection successfully")
}

// 填充sql语句、参数信息
func (s *SqlObj) Fill(sql string, params ...interface{}) *SqlObj {
	s.sql.WriteString(sql)
	s.params = append(s.params, params...)
	return s
}

// 获取SqlObj持有的sql.DB对象
func (s *SqlObj) GetDB() *sql.DB {
	return s.db
}

// 获取SqlObj持有的sql语句
func (s *SqlObj) GetSql() string {
	return s.sql.String()
}

// 设置SqlObj的db
func (s *SqlObj) SetDB(db *sql.DB) *SqlObj {
	s.db = db
	return s
}

// 设置SqlObj的sql语句
func (s *SqlObj) SetSql(sql string) {
	s.sql.Reset()
	s.sql.WriteString(sql)
}

// 设置SqlObj的sql语句参数值
func (s *SqlObj) SetParams(params ...interface{}) {
	s.params = params
}

// 清空SqlObj中的sql语句和参数
func (s *SqlObj) Clear() {
	s.sql.Reset()
	s.params = nil
}

// 查询单行
func (s *SqlObj) Query() map[string]string {
	resultList := s.QueryAsList()
	if resultList != nil {
		if len(resultList) > 0 {
			return resultList[0]
		} else {
			return make(map[string]string, 0)
		}
	} else {
		return nil
	}
}

// 查询列表
func (s *SqlObj) QueryAsList() []map[string]string {
	if s.sql.String() == "" {
		logger.Error("no sql statement")
	}

	rows, err := s.db.Query(s.sql.String(), s.params...)
	defer rows.Close()
	if err != nil {
		logger.Error(err)
	}

	// 获取字段切片
	cols, err := rows.Columns()
	if err != nil {
		logger.Error(err)
	}
	// 获取字段数
	colLen := len(cols)
	// 保存查询结果的字段值
	colSlice := make([]interface{}, colLen)
	// 初始化colSlice
	for i, _ := range colSlice {
		var a *string = new(string)
		colSlice[i] = a
	}

	// 保存返回值
	resultList := make([]map[string]string, 0)
	// 循环rows，处理每一行的数据
	for rows.Next() {
		rows.Scan(colSlice...)
		rowMap := make(map[string]string)
		for i, v := range colSlice {
			//content := *v.(*interface{})
			//rowMap[cols[i]] = byte2String(content.([]uint8))
			rowMap[cols[i]] = *v.(*string)

		}
		resultList = append(resultList, rowMap)
	}
	return resultList
}

// 字节数组转换成字符串
//func byte2String(source []uint8) string {
//	bytes := []byte{}
//	for _, v := range source {
//		bytes = append(bytes, byte(v))
//	}
//	return string(bytes)
//}