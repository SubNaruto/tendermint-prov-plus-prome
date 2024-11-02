package option

const (
	KB int = 1024
	MB     = 1024 * KB
	GB     = 1024 * MB

	Level0Size = 10 * MB

	BlockPartFileSuffix string = ".part"  // 区块分片文件的后缀
	BlockDataFileSuffix        = ".block" // 区块数据文件的后缀
	DataFileSuffix             = ".data"  // 数据文件的后缀
	ProvenanceSuffix           = ".prov"  // 溯源数据文件的后缀

	BlockPartDir  = "blockPart"  // 区块分片文件所在的目录
	BlockDataDir  = "block"      // 区块数据文件所在的目录
	DataFileDir   = "data"       // 数据文件所在的目录
	ProvenanceDir = "provenance" // 溯源数据文件所在的目录

	EntityIDSep = "!"
	MaxChar     = "~"
)

type Option struct {
	BTreeDegreeForIndex    int    // 内存索引的BTree的阶数
	BTreeDegreeForMemTable int    // MemTable的BTree的阶数
	DiskFileSize           int    // 数据文件的阈值
	DataDir                string // 数据的目录

	MergeDir            string // merge文件所在的目录
	MergeHintFilename   string // merge索引文件的名称
	MergeFinishFileName string // 标识merge完成的文件的名称

	MemTableSize int    // MemTable的大小
	WalFilePath  string // wal文件的目录
	WalDisabled  bool   // 是否禁用wal

	EnableBlockDataCache bool // 是否启用区块非分片数据的缓存

	ProbationLimit  int // probation区域的大小   80%
	ProtectionLimit int // protection区域的大小  20%

	EnableProv bool // 是否支持存储溯源数据

	ProvWriteChanSize int

	MemIndexNum int
}

func DefaultOption() *Option {
	return &Option{
		BTreeDegreeForMemTable: 32,
		BTreeDegreeForIndex:    32,
		DiskFileSize:           64 * MB, // 32KB or 64MB,
		DataDir:                "/tmp/memcask/data",
		MemTableSize:           4 * GB, // 15KB or 4GB
		MergeDir:               "/tmp/memcask/merge",
		MergeHintFilename:      "merge-hint",
		MergeFinishFileName:    "merge-finish",
		WalFilePath:            "/tmp/memcask/data/wal.log",
		WalDisabled:            true,
		EnableBlockDataCache:   false,
		ProbationLimit:         200 * MB,
		ProtectionLimit:        800 * MB,
		EnableProv:             false,
		ProvWriteChanSize:      1024 * 4,
		MemIndexNum:            4,
	}
}
