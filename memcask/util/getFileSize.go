package util

import "os"

/*	GetFileSize
 *  @Description: 根据文件指针获取文件大小
 *  @param f: 文件指针
 *  @return int: 文件大小
 *  @return error: 错误信息
 */
func GetFileSize(f *os.File) (int, error) {
	stat, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return int(stat.Size()), nil
}
