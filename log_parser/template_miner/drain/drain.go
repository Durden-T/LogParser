package drain

import (
	"github.com/OneOfOne/xxhash"
	"strconv"
	"strings"
	"unicode"
)

// 使用paramStr，代替日志中被识别出的参数
const paramStr = "<*>"

type LogCluster struct {
	Tokens []string
	ID     uint32
	Size   int64
}

func newLogCluster(tokens []string) *LogCluster {
	c := &LogCluster{
		Tokens: tokens,
		Size:   1,
	}

	// 使用xxhash，计算tokens生成ID
	hash := xxhash.New32()
	for _, token := range tokens {
		_, _ = hash.WriteString(token)
	}
	c.ID = hash.Sum32()
	return c
}

// 将新的日志添加到现有的日志cluster中
func (c *LogCluster) attachNewLog(tokens []string) {
	for i, s1 := range c.Tokens {
		s2 := tokens[i]
		if s1 != s2 {
			// 将此token泛化为参数
			c.Tokens[i] = paramStr
		}
	}

	c.Size++
}

type node struct {
	Key   string
	Depth int

	Child    map[string]*node
	Clusters []*LogCluster // 从属节点下的日志cluster
}

func newNode(key string, depth int) *node {
	return &node{
		Key:   key,
		Depth: depth,
		Child: make(map[string]*node),
	}
}

func (n *node) getChild(key string) (*node, bool) {
	node, found := n.Child[key]
	return node, found
}

func (n *node) setChild(key string, child *node) {
	n.Child[key] = child
}

func (n *node) getChildCount() int {
	return len(n.Child)
}

type Drain struct {
	// 前缀解析树的根结点
	Root *node // 导出字段, 大写

	clusters []*LogCluster

	maxDepth            int
	similarityThreshold float64
	maxChildren         int
	extraDelimiters     []string
}

func New() *Drain {
	return &Drain{
		Root: newNode("(ROOT)", 0),
	}
}

func (d *Drain) SetMetaData(maxDepth, maxChildren int, similarityThreshold float64, extraDelimiters []string) {
	// number of prefix tokens in each tree path (exclude Root and leaf node)
	d.maxDepth = maxDepth - 2
	d.maxChildren = maxChildren
	d.similarityThreshold = similarityThreshold
	d.extraDelimiters = extraDelimiters
}

func (d *Drain) ProcessLogMessage(content string) *LogCluster {
	for _, delimiter := range d.extraDelimiters {
		content = strings.ReplaceAll(content, delimiter, " ")
	}

	tokens := strings.Split(content, " ")
	if len(tokens) == 0 {
		return nil
	}

	matchCluster := d.treeSearch(tokens)

	// doesn't match a existing log cluster
	if matchCluster == nil {
		matchCluster = newLogCluster(tokens)
		d.clusters = append(d.clusters, matchCluster)
		d.addClusterToPrefixTree(matchCluster)

	} else {
		// add the new log message to the existing cluster
		matchCluster.attachNewLog(tokens)
	}

	return matchCluster
}

func (d *Drain) treeSearch(tokens []string) *LogCluster {
	// at first level, children are grouped by token (word) count
	tokenCount := len(tokens)
	tokenCountKey := strconv.Itoa(tokenCount)
	last := d.Root

	cur, _ := last.getChild(tokenCountKey)

	// no template with same token count yet
	if cur == nil {
		return nil
	}

	// find the leaf node for this log - a path of nodes matching the first N tokens (N=tree maxDepth)
	for currentDepth, token := range tokens {
		// at max maxDepth or is last token; currentDepth starts with 0
		if currentDepth == d.maxDepth-1 || currentDepth == tokenCount-1 {
			break
		}

		next, _ := cur.getChild(token)
		if next == nil {
			next, _ = cur.getChild(paramStr)
			if next == nil {
				return nil
			}
		}

		last = cur
		cur = next
	}

	return d.fastMatch(cur.Clusters, tokens)
}

func (d *Drain) fastMatch(clusters []*LogCluster, tokens []string) *LogCluster {
	maxSimilarity := float64(-1)
	maxParamCount := -1
	var maxCluster *LogCluster

	for _, cluster := range clusters {
		similarity, paramCount := getTokensDistance(cluster.Tokens, tokens)
		if similarity > maxSimilarity || (similarity == maxSimilarity && paramCount > maxParamCount) {
			maxSimilarity = similarity
			maxParamCount = paramCount
			maxCluster = cluster
		}
	}

	if maxSimilarity > d.similarityThreshold {
		return maxCluster
	}

	return nil
}

// seq1 is template
func getTokensDistance(seq1, seq2 []string) (float64, int) {
	if len(seq1) != len(seq2) {
		return -1, -1
	}

	var similarTokens float64
	var paramCount int

	for i, token1 := range seq1 {
		if token1 == paramStr {
			paramCount++
		} else if token1 == seq2[i] {
			similarTokens++
		}
	}

	return similarTokens / float64(len(seq1)), paramCount
}

func (d *Drain) addClusterToPrefixTree(cluster *LogCluster) {
	tokenCount := len(cluster.Tokens)
	tokenCountKey := strconv.Itoa(tokenCount)

	var parentNode *node
	if node, found := d.Root.getChild(tokenCountKey); found {
		parentNode = node
	} else {
		parentNode = newNode(tokenCountKey, 1)
		d.Root.setChild(tokenCountKey, parentNode)
	}

	// handle case of empty log string
	if tokenCount == 0 {
		parentNode.Clusters = append(parentNode.Clusters, cluster)
		return
	}

	for curDepth, token := range cluster.Tokens {
		// add current log cluster to the leaf node
		// at max Depth or is last token
		// curDepth starts with 0, should be 1
		if curDepth == d.maxDepth-1 || curDepth == tokenCount-1 {
			parentNode.Clusters = append(parentNode.Clusters, cluster)
			break
		}

		// the token is matched
		if node, found := parentNode.getChild(token); found {
			parentNode = node
			continue
		}

		// token not matched in this layer of existing tree

		if containNumber(token) {
			if node, found := parentNode.getChild(paramStr); found {
				parentNode = node
			} else {
				parentNode = insertChild(parentNode, paramStr, curDepth+2)
			}
			continue
		}

		// token doesn't contain number

		if _, found := parentNode.getChild(paramStr); found {
			if parentNode.getChildCount() < d.maxChildren {
				parentNode = insertChild(parentNode, token, curDepth+2)
			} else {
				parentNode, _ = parentNode.getChild(paramStr)
			}
			continue
		}

		// paramStr isn't  parentNode's childNode
		childCount := parentNode.getChildCount() + 1
		if childCount < d.maxChildren {
			parentNode = insertChild(parentNode, token, curDepth+2)
			continue
		}
		if childCount == d.maxChildren {
			parentNode = insertChild(parentNode, paramStr, curDepth+2)
			continue
		}

		parentNode, _ = parentNode.getChild(paramStr)
	}
}

func containNumber(s string) bool {
	runes := []rune(s)
	for _, c := range runes {
		if unicode.IsDigit(c) {
			return true
		}
	}
	return false
}

func insertChild(root *node, key string, depth int) *node {
	node := newNode(key, depth)
	root.setChild(key, node)
	return node
}

// 只能在初始化时调用
func (d *Drain) LoadOldData(other *Drain) {
	d.Root = other.Root
	d.clusters = d.clusters[:0]

	d.addCluster(d.Root)
}

func (d *Drain) addCluster(root *node) {
	if root.Clusters != nil {
		d.clusters = append(d.clusters, root.Clusters...)
	}

	for _, child := range root.Child {
		d.addCluster(child)
	}
}

func (d *Drain) GetResult() []*LogCluster {
	return d.clusters
}
