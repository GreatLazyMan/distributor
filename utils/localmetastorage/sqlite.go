package localmetastorage

import (
	"fmt"

	//"gorm.io/driver/sqlite"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	ctrl "sigs.k8s.io/controller-runtime"
)

var (
	sqliteLog = ctrl.Log.WithName("sqlite")
)

const (
	SqliteDatabase  = "/data/distributor.db"
	ChecksumTypeMd5 = "md5sum"
	ChecksumTypeKey = "checksumtype"
	ChecksumKey     = "checksum"
	FileNameKey     = "filename"
	DownloadFileKey = "downloadurl"
)

type SqliteEngine struct {
	*gorm.DB
}

// 定义模型
type FileInfo struct {
	gorm.Model
	LocalFileName string `gorm:"column:localfilename,uniqueIndex"`
	LocalDir      string `gorm:"column:localdir,index:dir_idx"`
	RemoteDIr     string `gorm:"column:remotedir"`
	Checksum      string `gorm:"column:checksum"`
	ChecksumType  string `gorm:"column,checksumtype"`
}

func InitSqliteDatabase() (*SqliteEngine, error) {
	// 连接数据库
	db, err := gorm.Open(sqlite.Open(SqliteDatabase), &gorm.Config{})
	if err != nil {
		sqliteLog.Error(err, "open sqlite database err")
		return nil, err
	}

	// 自动迁移模型
	err = db.AutoMigrate(&FileInfo{})
	if err != nil {
		sqliteLog.Error(err, "create table err")
		return nil, err
	}

	return &SqliteEngine{
		db,
	}, nil
}

func (s *SqliteEngine) CreateFileInfo(fi *FileInfo) error {
	if res := s.Create(fi); res.Error != nil {
		sqliteLog.Error(res.Error, "create file info %v error", fi)
		return res.Error
	}
	return nil
}

func (s *SqliteEngine) FindFileByLocalDir(localDir string) ([]FileInfo, error) {
	return s.findFileByDir("localdir", localDir)
}

func (s *SqliteEngine) FindFileByRemoteDir(remoteDir string) ([]FileInfo, error) {
	return s.findFileByDir("remotedir", remoteDir)
}

func (s *SqliteEngine) findFileByDir(field, dir string) ([]FileInfo, error) {
	var files []FileInfo
	res := s.Where(fmt.Sprintf("%s = ?", field), dir).Find(files)
	if res.Error != nil {
		sqliteLog.Error(res.Error, "select file where dir = %s error", dir)
		return nil, res.Error
	}
	return files, res.Error
}

func (s *SqliteEngine) FindLocalFile(localfilename string) ([]FileInfo, error) {
	var files []FileInfo
	res := s.Where("localfilename = ?", localfilename).Find(files)
	if res.Error != nil {
		sqliteLog.Error(res.Error, "select file where localfilename = %s error", localfilename)
		return nil, res.Error
	}
	return files, res.Error
}

// TODO: 核对md5值
func (s *SqliteEngine) CheckLocalFile(localfilename string) ([]FileInfo, error) {
	var files []FileInfo
	res := s.Where("localfilename = ?", localfilename).Find(files)
	if res.Error != nil {
		sqliteLog.Error(res.Error, "select file where localfilename = %s error", localfilename)
		return nil, res.Error
	}
	return files, res.Error
}
