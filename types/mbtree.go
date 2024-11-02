package types

import (
	"bytes"
	"container/list"
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/tendermint/tendermint/crypto/tmhash"
	tmproto "github.com/tendermint/tendermint/proto/tendermint/types"
)

const (
	m   = 16  // 阶数
	Sep = "@" // key@value
)

// 返回一个初始化的节点
func newNode() *tmproto.Node {
	initNode := &tmproto.Node{
		Transactions: make([]*tmproto.Transaction, 0, m),
	}
	return initNode
}

func copyTx(tx *tmproto.Transaction) *tmproto.Transaction {
	k, v := make([]byte, len(tx.Key)), make([]byte, len(tx.Value))
	copy(k, tx.Key)
	copy(v, tx.Value)
	return &tmproto.Transaction{
		Key:   k,
		Value: v,
	}
}

func copyTxKey(tx *tmproto.Transaction) *tmproto.Transaction {
	k := make([]byte, len(tx.Key))
	copy(k, tx.Key)
	return &tmproto.Transaction{
		Key: k,
	}
}

// 判断一个节点是否满足阶数限制，满足则返回true（关键字个数<=m 不会分裂）
func judgeM(node *tmproto.Node) bool {
	return len(node.Transactions) <= m
}

// 二分查找 寻找transactions中第一个key>=tx.key的元素的下标
func higher(transactions []*tmproto.Transaction, tx *tmproto.Transaction) int {
	low, high := 0, len(transactions)-1
	for low <= high {
		mid := (low + high) / 2
		if bytes.Compare(transactions[mid].Key, tx.Key) >= 0 {
			high = mid - 1
		} else {
			low = mid + 1
		}
	}
	if high+1 < len(transactions) {
		return high + 1
	}
	return -1
}

// 二分查找 寻找transactions中最后一个key<=tx.key的元素的下标
func lower(transactions []*tmproto.Transaction, tx *tmproto.Transaction) int {
	low, high := 0, len(transactions)-1
	for low <= high {
		mid := (low + high) / 2
		if bytes.Compare(transactions[mid].Key, tx.Key) <= 0 {
			low = mid + 1
		} else {
			high = mid - 1
		}
	}
	if low-1 >= 0 {
		return low - 1
	}
	return -1
}

func divideByTwo(m int) int { // M/2向上取整
	if m%2 == 0 {
		return m / 2
	}
	return m/2 + 1
}

// 获取tx应该存放的叶子节点
func reachLeafNode(tx *tmproto.Transaction, node *tmproto.Node) *tmproto.Node {
	// 到达叶子节点
	if len(node.ChildNodes) == 0 || node.ChildNodes[len(node.ChildNodes)-1] == nil {
		return node
	}

	// 获取当前节点Transactions中第一个>=tx.key的元素的下标
	index := higher(node.Transactions, tx)

	if index == -1 {
		// tx.key比当前节点所有的关键字都大
		// 沿着最右边的子节点继续向下
		return reachLeafNode(tx, node.ChildNodes[len(node.ChildNodes)-1])
	} else {
		// node.Transactions[index].key>=tx.key
		return reachLeafNode(tx, node.ChildNodes[index])
	}
}

func insert(tx *tmproto.Transaction, root *tmproto.Node) *tmproto.Node {
	targetNode := reachLeafNode(tx, root)        // 获取目标叶子节点
	index := higher(targetNode.Transactions, tx) // 叶子节点中需要插入的位置
	needChangeMaxNum := index == -1              // 是否需要自底向上更新最大值（tx.key是否比叶子节点所有关键字都大）

	// 在叶子节点插入关键字
	if index == -1 {
		// 插入到最后
		targetNode.Transactions = append(targetNode.Transactions, tx)
	} else {
		// 插入到前面某个位置
		targetNode.Transactions = append(targetNode.Transactions[:index],
			append([]*tmproto.Transaction{tx}, targetNode.Transactions[index:]...)...)
	}
	targetNode.ChildNodes = append(targetNode.ChildNodes, nil)

	// 节点不会分裂
	if judgeM(targetNode) {
		// 只需要自底向上更新最大值
		for {
			targetNode = targetNode.ParentNode
			if targetNode == nil || !needChangeMaxNum {
				break
			}
			index = lower(targetNode.Transactions, tx) // 需要更新的关键字的下标
			if index >= 0 {
				targetNode.Transactions[index] = copyTxKey(tx) // 非叶节点只保存key
			}
		}
		return root
	}

	inLeafLevel := true
	// 节点会分裂
	for !judgeM(targetNode) {
		splitIndex := divideByTwo(m)
		// [0,splitIndex-1]  [splitIndex,m]
		temp := targetNode
		targetNode = targetNode.ParentNode

		childNode1 := newNode() // 分裂产生的第一个子节点
		childNode2 := newNode() // 分裂产生的第二个子节点

		for i := 0; i < splitIndex; i++ {

			// 左子节点的关键字列表
			if inLeafLevel {
				// 如果在叶子层，我们需要记录完整的交易信息
				childNode1.Transactions = append(childNode1.Transactions, copyTx(temp.Transactions[i]))
			} else {
				// 如果不在叶子层，我们只记录key作为索引
				childNode1.Transactions = append(childNode1.Transactions, copyTxKey(temp.Transactions[i]))
			}

			// 左子节点的子节点列表
			childNode1.ChildNodes = append(childNode1.ChildNodes, temp.ChildNodes[i])
		}

		for i := splitIndex; i < len(temp.Transactions); i++ {

			// 右子节点的关键字列表
			if inLeafLevel {
				// 如果在叶子层，我们需要记录完整的交易信息
				childNode2.Transactions = append(childNode2.Transactions, copyTx(temp.Transactions[i]))
			} else {
				// 如果不在叶子层，我们只记录key作为索引
				childNode2.Transactions = append(childNode2.Transactions, copyTxKey(temp.Transactions[i]))
			}

			// 右子节点的子节点列表
			childNode2.ChildNodes = append(childNode2.ChildNodes, temp.ChildNodes[i])
		}

		inLeafLevel = false

		for i := 0; i < len(childNode1.ChildNodes); i++ {
			if childNode1.ChildNodes[i] != nil {
				childNode1.ChildNodes[i].ParentNode = childNode1 // 更改父指针
			}
		}

		for i := 0; i < len(childNode2.ChildNodes); i++ {
			if childNode2.ChildNodes[i] != nil {
				childNode2.ChildNodes[i].ParentNode = childNode2 // 更改父指针
			}
		}

		if targetNode == nil {
			// 如果向上没有节点，就生成一个新节点
			targetNode = newNode()

			// 左子节点的最大关键字
			targetNode.Transactions = append(targetNode.Transactions, copyTxKey(temp.Transactions[splitIndex-1]))

			// 右子节点的最大关键字
			targetNode.Transactions = append(targetNode.Transactions, copyTxKey(temp.Transactions[m]))

			targetNode.ChildNodes = append(targetNode.ChildNodes, childNode1) // 父节点添加左子节点
			targetNode.ChildNodes = append(targetNode.ChildNodes, childNode2) // 父节点添加右子节点
		} else {
			// 分裂时确定父节点需要修改的关键字 如果分裂 需要修改maxNum（原本该节点在父节点对应的关键字是maxNum）
			parentMaxNum := temp.Transactions[len(temp.Transactions)-1]

			// 父节点需要修改的关键字的下标
			changedNumIndex := lower(targetNode.Transactions, parentMaxNum)

			// 左子节点的最大关键字
			targetNode.Transactions[changedNumIndex] = copyTxKey(temp.Transactions[splitIndex-1])

			targetNode.Transactions = append(
				// 右子节点的最大关键字(插入到index=changedNumIndex+1的位置)
				targetNode.Transactions[:changedNumIndex+1],
				append([]*tmproto.Transaction{copyTxKey(temp.Transactions[m])}, targetNode.Transactions[changedNumIndex+1:]...)...)

			// 父节点更新左子节点
			targetNode.ChildNodes[changedNumIndex] = childNode1

			// 父节点插入右子节点
			targetNode.ChildNodes = append(
				targetNode.ChildNodes[:changedNumIndex+1],
				append([]*tmproto.Node{childNode2}, targetNode.ChildNodes[changedNumIndex+1:]...)...)
		}

		// 修改父指针
		for i := 0; i < len(targetNode.ChildNodes); i++ {
			if targetNode.ChildNodes[i] != nil {
				targetNode.ChildNodes[i].ParentNode = targetNode
			}
		}
	}

	// 这种情况是：从下往上分裂到某一层，不再向上分裂，我们需要自这一层向上更新最大值
	noNeedChange := false
	maxKeyTx := copyTxKey(targetNode.Transactions[len(targetNode.Transactions)-1])
	for {
		if targetNode.ParentNode == nil {
			return targetNode
		}
		targetNode = targetNode.ParentNode
		if noNeedChange == false {
			index = lower(targetNode.Transactions, maxKeyTx) // 需要更新的关键字
			if index >= 0 {
				if bytes.Compare(targetNode.Transactions[index].Key, maxKeyTx.Key) == 0 {
					// 父节点中，最大的且满足<=newNum.key的交易的key与maxKeyTx.key相等，说明不再需要更新最大值
					noNeedChange = true
				} else {
					targetNode.Transactions[index] = copyTxKey(maxKeyTx)
				}
			}
		}
	}
}

func isLeaf(n *tmproto.Node) bool { // 判断一个节点是否是叶子节点
	if len(n.ChildNodes) == 0 {
		return true
	}
	return n.ChildNodes[0] == nil
}

func height(root *tmproto.Node) int { // 获取树高
	if root == nil {
		return 0
	}
	if isLeaf(root) {
		return 1
	}
	return 1 + height(root.ChildNodes[0])
}

// 层序遍历 同时获取所有叶子节点 用于构建默克尔树
func getLeaves(root *tmproto.Node) []*tmproto.Node {
	queue := list.New()
	var res []*tmproto.Node
	queue.PushBack(root)
	for queue.Len() > 0 {
		size := queue.Len()
		res = make([]*tmproto.Node, 0, size)
		for i := 0; i < size; i++ {
			node := queue.Remove(queue.Front()).(*tmproto.Node)
			if node == nil {
				continue
			}

			res = append(res, node)

			//fmt.Printf("  { ")
			//for j := 0; j < len(node.Transactions); j++ {
			//	fmt.Printf("%v=%v ", string(node.Transactions[j].Key), string(node.Transactions[j].Value))
			//}
			//fmt.Printf("}")

			for j := 0; j < len(node.ChildNodes); j++ {
				if node.ChildNodes[j] != nil {
					queue.PushBack(node.ChildNodes[j])
				}
			}

			node.Hashes = make([][]byte, len(node.Transactions))
		}
		// fmt.Println()
	}
	return res
}

// 在B+树的基础上实现默克尔树
func buildMerkleTree(treeHeight int, leaves []*tmproto.Node) {
	leafHash := true
	for i := treeHeight; i >= 1; i-- { // 自底向上
		var parentNodes []*tmproto.Node

		// 从前到后遍历这一层的所有节点
		for j := 0; j < len(leaves); j++ {
			concatenatedHash := make([]byte, 0, len(leaves[j].Transactions)) // 用于拼接哈希值

			// 遍历节点中的所有索引项
			for k := 0; k < len(leaves[j].Transactions); k++ {
				if leafHash {
					// 在叶子层
					var tx []byte
					tx = append(tx, leaves[j].Transactions[k].Key...)
					tx = append(tx, []byte(Sep)...)
					tx = append(tx, leaves[j].Transactions[k].Value...)
					leaves[j].Hashes[k] = tmhash.Sum(tx) // 对叶子节点中的交易进行哈希运算
				}
				// 将节点中所有索引项的哈希进行拼接 H1|H2|....|Hn
				concatenatedHash = append(concatenatedHash, leaves[j].Hashes[k]...)
			}

			leaves[j].Hash = tmhash.Sum(concatenatedHash) // 计算该节点的哈希值 H(H1|H2|...|Hn) 对索引项的拼接值进行哈希运算

			if leaves[j].ParentNode != nil {
				ParentNode := leaves[j].ParentNode
				// 定位该节点对应的的父节点索引项
				index := lower(ParentNode.Transactions, leaves[j].Transactions[len(leaves[j].Transactions)-1])
				// 将该节点的哈希值添加到对应的父节点索引项中
				ParentNode.Hashes[index] = leaves[j].Hash
			}

			// 获取上一层的所有节点
			if len(parentNodes) == 0 {
				parentNodes = append(parentNodes, leaves[j].ParentNode)
			} else {
				if parentNodes[len(parentNodes)-1] != leaves[j].ParentNode { // 防止重复记录父节点
					parentNodes = append(parentNodes, leaves[j].ParentNode)
				}
			}
		}
		leafHash = false     // 离开了叶子层
		leaves = parentNodes // 进入父层
	}
}

func buildBPlusTree(txs []Tx) *tmproto.Node {
	root := newNode()

	// 实现B+树
	for i := 0; i < len(txs); i++ {
		var tx tmproto.Tx
		err := proto.Unmarshal(txs[i], &tx)
		if err != nil {
			panic(err)
		}

		pb, err := proto.Marshal(tx.TxBody)
		if err != nil {
			panic(err)
		}

		t := &tmproto.Transaction{Key: []byte(tx.Key), Value: pb}
		root = insert(t, root)
	}
	return root
}

func removeNilChild(node *tmproto.Node) {
	if node != nil {
		for j := 0; j < len(node.ChildNodes); j++ {
			if node.ChildNodes[j] == nil {
				node.ChildNodes = []*tmproto.Node{}
				return
			}
		}
	}
}

func removeParentNode(root *tmproto.Node) {
	queue := list.New()
	queue.PushBack(root)
	for queue.Len() > 0 {
		n := queue.Remove(queue.Front()).(*tmproto.Node)
		if n == nil {
			continue
		}
		n.ParentNode = nil
		for j := 0; j < len(n.ChildNodes); j++ {
			if n.ChildNodes[j] != nil {
				queue.PushBack(n.ChildNodes[j])
			}
		}
	}
}

// CreateVO 根据mbTree和key构造vo
func CreateVO(t *tmproto.Node, key []byte) (*tmproto.VO, error) {
	if t == nil {
		return nil, fmt.Errorf("nil mbtreeRoot")
	}

	// 根据key获取从根到叶子节点的路径(包含的哈希值)
	vo := new(tmproto.VO)

	tx := &tmproto.Transaction{Key: key}

	for {
		voNode := &tmproto.VONode{}

		// 添加节点中的所有索引项的Hash
		voNode.Hashes = append(voNode.Hashes, t.Hashes...)

		index := higher(t.Transactions, tx)

		if isLeaf(t) {
			if index == -1 {
				// 找不到第一个>=key的下标
				return nil, fmt.Errorf("tx search error")
			}

			targetTx := t.Transactions[index]

			// 在叶子节点找不到
			if bytes.Compare(targetTx.Key, key) != 0 {
				return nil, fmt.Errorf("tx search error")
			}

			voNode.Index = int64(index)
			vo.VONodes = append(vo.VONodes, voNode)
			break
		}
		if index == -1 {
			t = t.ChildNodes[0]
			voNode.Index = 0
		} else {
			t = t.ChildNodes[index]
			voNode.Index = int64(index)
		}
		vo.VONodes = append(vo.VONodes, voNode)
	}
	return vo, nil
}

func createMBTree(txs []Tx) *tmproto.Node {
	// 实现B+树
	root := buildBPlusTree(txs)

	treeHeight := height(root) // 树高
	leaves := getLeaves(root)  // 获取叶子节点

	// 使叶子节点形成单链表
	for i := 0; i < len(leaves); i++ {
		removeNilChild(leaves[i])
		if i+1 < len(leaves) {
			leaves[i].NextLeafNode = leaves[i+1]
		}
	}

	// 实现默克尔树
	buildMerkleTree(treeHeight, leaves)

	removeParentNode(root)
	return root
}
