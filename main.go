package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/zeebo/bencode"
)

// upstreamListURL 上游 tracker 列表来源
// 格式:每行一个 `<scheme>://host:port/announce`,空行 / # 开头忽略
const upstreamListURL = "https://ngosang.github.io/trackerslist/trackers_all.txt"

const (
	cacheTTL            = 5 * time.Minute
	upstreamHTTPTimeout = 4 * time.Second
	upstreamUDPTimeout  = 3 * time.Second
	upstreamListTimeout = 30 * time.Second
	overallTimeout      = 5 * time.Second
	refreshInterval     = 2 * time.Hour
	defaultNumWant      = 50
	defaultPeerPort     = 6881 // BT client peer port fallback (BT protocol convention)

	// health check — startup and after each list refresh, async filter unreachable
	probeHTTPTimeout = 3 * time.Second
	probeUDPTimeout  = 2 * time.Second
	probeConcurrency = 30
	probeOverallMax  = 30 * time.Second

	// HTTP server 默认监听 — 可被 -host / -port flag 覆盖
	defaultListenHost = "127.0.0.1"
	defaultListenPortFlag = "6969"
)

// UpstreamList 当前生效的上游 tracker 列表
type UpstreamList struct {
	HTTP []string // 完整 announce URL
	UDP  []string // host:port
}

// UpstreamResult 单个上游查询结果
// Complete / Incomplete 透传上游的种子/下载者计数
type UpstreamResult struct {
	Peers      []byte
	Complete   int32
	Incomplete int32
}

// upstreamList 持有当前生效的列表
// 启动时填充 fallback,后台 goroutine 每 2 小时刷新一次
var upstreamList atomic.Pointer[UpstreamList]

// fallbackUpstreams 启动期内置列表,在第一次成功刷新之前使用
// 即使远程列表拉不到也不会"零上游"
var fallbackUpstreams = &UpstreamList{
	HTTP: []string{
		"https://tracker.torrent.eu.org/announce",
		"https://tracker.opentrackr.org/announce",
	},
	UDP: []string{
		"tracker.opentrackr.org:1337",
		"tracker.openbittorrent.com:6969",
	},
}

func init() {
	upstreamList.Store(fallbackUpstreams)
}

type CacheItem struct {
	Result UpstreamResult
	Expire time.Time
}

var cache sync.Map

func cacheGet(key string) (UpstreamResult, bool) {
	v, ok := cache.Load(key)
	if !ok {
		return UpstreamResult{}, false
	}
	item := v.(CacheItem)
	if time.Now().After(item.Expire) {
		cache.Delete(key)
		return UpstreamResult{}, false
	}
	return item.Result, true
}

func cacheSet(key string, r UpstreamResult) {
	cache.Store(key, CacheItem{
		Result: r,
		Expire: time.Now().Add(cacheTTL),
	})
}

// rawInfoHash 从 url.Values 拿到 info_hash 的 20 字节原始数据
// q.Get 会做 URL 解码,所以 q["info_hash"][0] 已经是原始字节
func rawInfoHash(q url.Values) []byte {
	if v := q["info_hash"]; len(v) > 0 {
		return []byte(v[0])
	}
	return nil
}

// rawPeerID 同上,用于 peer_id (20 字节)
func rawPeerID(q url.Values) []byte {
	if v := q["peer_id"]; len(v) > 0 {
		return []byte(v[0])
	}
	return nil
}

func uint64Param(q url.Values, key string) uint64 {
	v, _ := strconv.ParseUint(q.Get(key), 10, 64)
	return v
}

func uint32Param(q url.Values, key string) uint32 {
	v, _ := strconv.ParseUint(q.Get(key), 10, 32)
	return uint32(v)
}

func eventCode(s string) uint32 {
	switch s {
	case "started":
		return 1
	case "stopped":
		return 2
	case "completed":
		return 3
	}
	return 0
}

// buildAnnouncePacket 按 BEP 15 构造 98 字节 announce 请求
//
//	0-8    connection_id    uint64
//	8-12   action = 1       uint32
//	12-16  transaction_id   uint32
//	16-36  info_hash        20 bytes
//	36-56  peer_id          20 bytes
//	56-64  downloaded       uint64
//	64-72  left             uint64
//	72-80  uploaded         uint64
//	80-84  event            uint32 (0=none 1=started 2=stopped 3=completed)
//	84-88  IP               uint32 (0 = tracker 自行决定)
//	88-92  key              uint32
//	92-96  num_want         int32
//	96-98  port             uint16
func buildAnnouncePacket(
	connectionID uint64,
	transaction uint32,
	infoHash, peerID []byte,
	downloaded, left, uploaded uint64,
	event, key uint32,
	port uint16,
) []byte {
	packet := make([]byte, 98)
	binary.BigEndian.PutUint64(packet[0:8], connectionID)
	binary.BigEndian.PutUint32(packet[8:12], 1)
	binary.BigEndian.PutUint32(packet[12:16], transaction)
	copy(packet[16:36], infoHash)
	copy(packet[36:56], peerID)
	binary.BigEndian.PutUint64(packet[56:64], downloaded)
	binary.BigEndian.PutUint64(packet[64:72], left)
	binary.BigEndian.PutUint64(packet[72:80], uploaded)
	binary.BigEndian.PutUint32(packet[80:84], event)
	binary.BigEndian.PutUint32(packet[84:88], 0)
	binary.BigEndian.PutUint32(packet[88:92], key)
	binary.BigEndian.PutUint32(packet[92:96], defaultNumWant)
	binary.BigEndian.PutUint16(packet[96:98], port)
	return packet
}

// dedupeIPv4Peers 合并多段 compact peer 列表,按 6 字节 (IP4 + port) 去重
// 尾部不完整的字节会被丢弃
func dedupeIPv4Peers(parts [][]byte) []byte {
	seen := make(map[string]struct{}, 1024)
	var out []byte
	for _, p := range parts {
		n := len(p) - len(p)%6
		for i := 0; i < n; i += 6 {
			key := string(p[i : i+6])
			if _, dup := seen[key]; !dup {
				seen[key] = struct{}{}
				out = append(out, p[i:i+6]...)
			}
		}
	}
	return out
}

func announceHandler(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	infoHashBytes := rawInfoHash(q)
	if len(infoHashBytes) != 20 {
		http.Error(w, "missing or invalid info_hash", http.StatusBadRequest)
		return
	}
	infoHashKey := string(infoHashBytes)

	// 缓存命中直接返回
	if result, ok := cacheGet(infoHashKey); ok {
		sendResponse(w, result)
		return
	}

	// 整体超时,避免被最慢的上游阻塞
	ctx, cancel := context.WithTimeout(r.Context(), overallTimeout)
	defer cancel()

	result := queryUpstreams(ctx, q)

	if len(result.Peers) == 0 {
		slog.Warn("no peers from any upstream",
			"info_hash", fmt.Sprintf("%x", infoHashBytes))
	}

	cacheSet(infoHashKey, result)
	sendResponse(w, result)
}

// queryUpstreams 并发查询所有上游,合并去重 peer 与 summary
// summary 字段取 sum(每个上游都回报它看到的 swarm 数,客户端可自行判断)
func queryUpstreams(ctx context.Context, q url.Values) UpstreamResult {
	list := upstreamList.Load()
	if list == nil {
		return UpstreamResult{}
	}
	httpTrackers := list.HTTP
	udpTrackers := list.UDP

	totalUpstreams := len(httpTrackers) + len(udpTrackers)

	// channel 必须有缓冲,否则上游 goroutine 多时全部卡死在 send 上
	ch := make(chan UpstreamResult, totalUpstreams)

	var wg sync.WaitGroup

	for _, t := range httpTrackers {
		wg.Add(1)
		go func(tracker string) {
			defer wg.Done()
			if r := queryHTTPTracker(ctx, tracker, q); len(r.Peers) > 0 || r.Complete > 0 || r.Incomplete > 0 {
				select {
				case ch <- r:
				case <-ctx.Done():
				}
			}
		}(t)
	}

	for _, t := range udpTrackers {
		wg.Add(1)
		go func(tracker string) {
			defer wg.Done()
			if r := queryUDPTracker(ctx, tracker, q); len(r.Peers) > 0 || r.Complete > 0 || r.Incomplete > 0 {
				select {
				case ch <- r:
				case <-ctx.Done():
				}
			}
		}(t)
	}

	go func() {
		wg.Wait()
		close(ch)
	}()

	var (
		parts    [][]byte
		combined UpstreamResult
	)
	for {
		select {
		case r, ok := <-ch:
			if !ok {
				combined.Peers = dedupeIPv4Peers(parts)
				return combined
			}
			parts = append(parts, r.Peers)
			combined.Complete += r.Complete
			combined.Incomplete += r.Incomplete
		case <-ctx.Done():
			combined.Peers = dedupeIPv4Peers(parts)
			return combined
		}
	}
}

func queryHTTPTracker(ctx context.Context, tracker string, q url.Values) UpstreamResult {
	u, err := url.Parse(tracker)
	if err != nil {
		slog.Warn("parse tracker url failed", "url", tracker, "err", err)
		return UpstreamResult{}
	}
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		slog.Warn("build request failed", "url", tracker, "err", err)
		return UpstreamResult{}
	}

	client := http.Client{Timeout: upstreamHTTPTimeout}
	resp, err := client.Do(req)
	if err != nil {
		// 网络/DNS/超时 — 公开 tracker 常态,DEBUG 级别
		slog.Debug("http tracker unreachable", "url", tracker, "err", err)
		return UpstreamResult{}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		// 非 200 可能是 tracker 配置 / Cloudflare 错误 — WARN 值得看
		slog.Warn("http tracker non-200", "url", tracker, "status", resp.StatusCode)
		return UpstreamResult{}
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		slog.Debug("http tracker read body failed", "url", tracker, "err", err)
		return UpstreamResult{}
	}

	var decoded struct {
		Peers      []byte `bencode:"peers"`
		Complete   int32  `bencode:"complete"`
		Incomplete int32  `bencode:"incomplete"`
	}
	if err := bencode.DecodeBytes(data, &decoded); err != nil {
		// 协议错误 — tracker 返回了非 bencode,是上游 BUG
		slog.Warn("http tracker decode failed", "url", tracker, "err", err)
		return UpstreamResult{}
	}
	return UpstreamResult{
		Peers:      decoded.Peers,
		Complete:   decoded.Complete,
		Incomplete: decoded.Incomplete,
	}
}

// queryUDPTracker 实现 BEP 15: connect + announce
// https://www.bittorrent.org/beps/bep_0015.html
func queryUDPTracker(ctx context.Context, addr string, q url.Values) UpstreamResult {
	server, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		slog.Debug("resolve udp addr failed", "addr", addr, "err", err)
		return UpstreamResult{}
	}

	conn, err := net.DialUDP("udp", nil, server)
	if err != nil {
		slog.Debug("dial udp failed", "addr", addr, "err", err)
		return UpstreamResult{}
	}
	defer conn.Close()

	// 用 ctx 控制整体 deadline,与上层 overallTimeout 对齐
	if d, ok := ctx.Deadline(); ok {
		if err := conn.SetDeadline(d); err != nil {
			slog.Debug("set deadline failed", "addr", addr, "err", err)
			return UpstreamResult{}
		}
	} else {
		if err := conn.SetDeadline(time.Now().Add(upstreamUDPTimeout)); err != nil {
			slog.Debug("set deadline failed", "addr", addr, "err", err)
			return UpstreamResult{}
		}
	}

	transaction := rand.Uint32()

	// connect request (16 bytes): magic(8) + action=0(4) + tx(4)
	connectReq := make([]byte, 16)
	binary.BigEndian.PutUint64(connectReq[0:8], 0x41727101980)
	binary.BigEndian.PutUint32(connectReq[8:12], 0)
	binary.BigEndian.PutUint32(connectReq[12:16], transaction)

	if _, err := conn.Write(connectReq); err != nil {
		slog.Debug("udp connect write failed", "addr", addr, "err", err)
		return UpstreamResult{}
	}

	respBuf := make([]byte, 2048)
	n, err := conn.Read(respBuf)
	if err != nil || n < 16 {
		slog.Debug("udp connect read failed", "addr", addr, "err", err, "n", n)
		return UpstreamResult{}
	}

	// 校验 response: action=0 + transaction_id 匹配 — 协议错误,WARN
	respAction := binary.BigEndian.Uint32(respBuf[0:4])
	respTx := binary.BigEndian.Uint32(respBuf[4:8])
	if respAction != 0 || respTx != transaction {
		slog.Warn("udp connect response invalid",
			"addr", addr, "action", respAction, "tx", respTx)
		return UpstreamResult{}
	}
	connectionID := binary.BigEndian.Uint64(respBuf[8:16])

	// 构建 announce request
	infoHash := rawInfoHash(q)
	peerID := rawPeerID(q)
	if len(infoHash) != 20 || len(peerID) != 20 {
		return UpstreamResult{}
	}

	downloaded := uint64Param(q, "downloaded")
	left := uint64Param(q, "left")
	uploaded := uint64Param(q, "uploaded")
	event := eventCode(q.Get("event"))

	port := uint32Param(q, "port")
	if port == 0 || port > 0xFFFF {
		port = defaultPeerPort
	}

	key := rand.Uint32()

	packet := buildAnnouncePacket(
		connectionID, transaction,
		infoHash, peerID,
		downloaded, left, uploaded,
		event, key, uint16(port),
	)

	if _, err := conn.Write(packet); err != nil {
		slog.Debug("udp announce write failed", "addr", addr, "err", err)
		return UpstreamResult{}
	}

	n, err = conn.Read(respBuf)
	if err != nil || n < 20 {
		// 部分 tracker 返回 < 20 字节(协议不兼容或 IP 白名单拒绝)— DEBUG
		slog.Debug("udp announce read failed", "addr", addr, "err", err, "n", n)
		return UpstreamResult{}
	}

	// 校验 announce response — 协议错误,WARN
	respAction = binary.BigEndian.Uint32(respBuf[0:4])
	respTx = binary.BigEndian.Uint32(respBuf[4:8])
	if respAction != 1 || respTx != transaction {
		slog.Warn("udp announce response invalid",
			"addr", addr, "action", respAction, "tx", respTx)
		return UpstreamResult{}
	}

	return parseUDPAnnounceResponse(respBuf[:n])
}

// parseUDPAnnounceResponse 从 BEP 15 announce response 里解析 seeders/leechers/peers
//
//	0-4   action (1, 已校验过)
//	4-8   transaction_id
//	8-12  interval  (uint32)
//	12-16 leechers  (uint32) -> incomplete
//	16-20 seeders   (uint32) -> complete
//	20+   peers (compact)
func parseUDPAnnounceResponse(buf []byte) UpstreamResult {
	if len(buf) < 20 {
		return UpstreamResult{}
	}
	leechers := int32(binary.BigEndian.Uint32(buf[12:16]))
	seeders := int32(binary.BigEndian.Uint32(buf[16:20]))
	return UpstreamResult{
		Peers:      buf[20:],
		Complete:   seeders,
		Incomplete: leechers,
	}
}

func sendResponse(w http.ResponseWriter, r UpstreamResult) {
	resp := map[string]any{
		"interval":     int32(1800),
		"min interval": int32(900),
		"complete":     r.Complete,
		"incomplete":   r.Incomplete,
		"peers":        r.Peers,
	}

	data, err := bencode.EncodeBytes(resp)
	if err != nil {
		http.Error(w, "encode error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/plain")
	_, _ = w.Write(data)
}

// probeHTTPTracker lightweight HTTP health check.
// Sends a minimal announce and verifies 200 + valid bencode.
func probeHTTPTracker(ctx context.Context, tracker string) bool {
	u, err := url.Parse(tracker)
	if err != nil {
		return false
	}
	q := url.Values{}
	q.Set("info_hash", string(make([]byte, 20))) // all-zero hash
	q.Set("peer_id", "-qp0000-probe12345678")
	q.Set("port", "6881")
	q.Set("uploaded", "0")
	q.Set("downloaded", "0")
	q.Set("left", "0")
	q.Set("compact", "1")
	q.Set("event", "stopped")
	q.Set("numwant", "0")
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return false
	}

	client := &http.Client{Timeout: probeHTTPTimeout}
	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return false
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return false
	}

	var probe struct {
		Peers []byte `bencode:"peers"`
	}
	if err := bencode.DecodeBytes(data, &probe); err != nil {
		return false
	}
	return true
}

// probeUDPTracker lightweight UDP health check.
// Sends only the BEP 15 connect step, verifies magic + action + tx match.
// Faster than full connect+announce and needs no info_hash.
func probeUDPTracker(ctx context.Context, addr string) bool {
	server, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return false
	}
	conn, err := net.DialUDP("udp", nil, server)
	if err != nil {
		return false
	}
	defer conn.Close()

	if d, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(d)
	} else {
		_ = conn.SetDeadline(time.Now().Add(probeUDPTimeout))
	}

	transaction := rand.Uint32()
	req := make([]byte, 16)
	binary.BigEndian.PutUint64(req[0:8], 0x41727101980)
	binary.BigEndian.PutUint32(req[8:12], 0)
	binary.BigEndian.PutUint32(req[12:16], transaction)

	if _, err := conn.Write(req); err != nil {
		return false
	}

	resp := make([]byte, 16)
	n, err := conn.Read(resp)
	if err != nil || n < 16 {
		return false
	}

	action := binary.BigEndian.Uint32(resp[0:4])
	tx := binary.BigEndian.Uint32(resp[4:8])
	return action == 0 && tx == transaction
}

// healthCheck concurrently probes every upstream and filters out the
// unreachable ones. Returns a new UpstreamList containing only alive
// entries; if the overall context times out, unfinished probes are
// dropped (so a slow upstream won't block the result forever).
func healthCheck(parent context.Context, list *UpstreamList) *UpstreamList {
	ctx, cancel := context.WithTimeout(parent, probeOverallMax)
	defer cancel()

	var (
		mu          sync.Mutex
		aliveHTTP   = make([]string, 0, len(list.HTTP))
		aliveUDP    = make([]string, 0, len(list.UDP))
		wg          sync.WaitGroup
		sem         = make(chan struct{}, probeConcurrency)
		httpDropped int
		udpDropped  int
	)

	checkHTTP := func(t string) {
		defer wg.Done()
		defer func() { <-sem }()
		sem <- struct{}{}
		if probeHTTPTracker(ctx, t) {
			mu.Lock()
			aliveHTTP = append(aliveHTTP, t)
			mu.Unlock()
		} else {
			mu.Lock()
			httpDropped++
			mu.Unlock()
		}
	}
	checkUDP := func(t string) {
		defer wg.Done()
		defer func() { <-sem }()
		sem <- struct{}{}
		if probeUDPTracker(ctx, t) {
			mu.Lock()
			aliveUDP = append(aliveUDP, t)
			mu.Unlock()
		} else {
			mu.Lock()
			udpDropped++
			mu.Unlock()
		}
	}

	for _, t := range list.HTTP {
		wg.Add(1)
		go checkHTTP(t)
	}
	for _, t := range list.UDP {
		wg.Add(1)
		go checkUDP(t)
	}
	wg.Wait()

	slog.Info("upstream health check complete",
		"http_alive", len(aliveHTTP), "http_dropped", httpDropped,
		"udp_alive", len(aliveUDP), "udp_dropped", udpDropped)

	return &UpstreamList{HTTP: aliveHTTP, UDP: aliveUDP}
}

// parseTrackerList 解析 ngosang/trackerslist 风格的纯文本列表
//
//	udp://host:port/announce     -> UDP,只保留 host:port
//	http(s)://host:port/announce -> HTTP,保留完整 URL
//
// 空行 / `#` 开头行忽略,坏 URL 跳过
func parseTrackerList(data []byte) *UpstreamList {
	list := &UpstreamList{
		HTTP: make([]string, 0, 64),
		UDP:  make([]string, 0, 64),
	}

	sc := bufio.NewScanner(bytes.NewReader(data))
	// 单行可能很长,放大缓冲
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	seenHTTP := make(map[string]struct{}, 256)
	seenUDP := make(map[string]struct{}, 256)

	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		switch {
		case strings.HasPrefix(line, "udp://"):
			u, err := url.Parse(line)
			if err != nil || u.Host == "" || u.Port() == "" {
				continue
			}
			if _, dup := seenUDP[u.Host]; dup {
				continue
			}
			seenUDP[u.Host] = struct{}{}
			list.UDP = append(list.UDP, u.Host)

		case strings.HasPrefix(line, "http://"), strings.HasPrefix(line, "https://"):
			if _, dup := seenHTTP[line]; dup {
				continue
			}
			seenHTTP[line] = struct{}{}
			list.HTTP = append(list.HTTP, line)
		}
	}
	return list
}

// loadUpstreamList 从 URL 下载并解析
func loadUpstreamList(ctx context.Context, url string) (*UpstreamList, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}

	client := &http.Client{Timeout: upstreamListTimeout}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("non-200: %d", resp.StatusCode)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}

	return parseTrackerList(data), nil
}

// refreshLoop 后台每 refreshInterval 拉一次上游。
// 拉到的 raw 列表立即生效(不等 health check),health check 在
// goroutine 里异步跑完后用过滤后的列表覆盖。这样:
//   - 服务启动后立刻可用(不会被 health check 阻塞)
//   - 经过 ~30s 后,atomic.Pointer 里替换成只含 alive 上游的列表
func refreshLoop(ctx context.Context) {
	ticker := time.NewTicker(refreshInterval)
	defer ticker.Stop()

	doRefresh := func() {
		cctx, cancel := context.WithTimeout(ctx, upstreamListTimeout+5*time.Second)
		defer cancel()
		slog.Info("refreshing upstream list", "url", upstreamListURL)
		list, err := loadUpstreamList(cctx, upstreamListURL)
		if err != nil {
			slog.Error("upstream list refresh failed; keeping previous",
				"err", err)
			return
		}

		upstreamList.Store(list)
		slog.Info("upstream list refreshed (raw, probing in background)",
			"http", len(list.HTTP), "udp", len(list.UDP))

		// 异步 health check,过滤后覆盖 atomic.Pointer
		go func() {
			checked := healthCheck(ctx, list)
			upstreamList.Store(checked)
			slog.Info("upstream list filtered",
				"http", len(checked.HTTP), "udp", len(checked.UDP))
		}()
	}

	doRefresh()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			doRefresh()
		}
	}
}

// runCheck is the diagnostic mode: probes every upstream and queries the
// given magnet/info_hash against each one, then prints a per-tracker
// report. Does not start the HTTP server.
//
// Accepts either:
//   - a magnet URL:  magnet:?xt=urn:btih:<40-hex>
//   - a raw 40-char hex info_hash
func runCheck(arg string) {
	infoHash, hexStr, err := extractInfoHash(arg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		fmt.Fprintf(os.Stderr, "expected: magnet:?xt=urn:btih:<40hex>  or  <40hex>\n")
		os.Exit(1)
	}

	fmt.Printf("=== tracker proxy diagnostic ===\n")
	fmt.Printf("info_hash: %s  (%d bytes)\n\n", hexStr, len(infoHash))

	// 1) load + health check
	fmt.Println("[1/3] loading upstream list...")
	ctx := context.Background()
	rawList, err := loadUpstreamList(ctx, upstreamListURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load upstream list failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("      raw list: %d HTTP + %d UDP\n\n", len(rawList.HTTP), len(rawList.UDP))

	fmt.Println("[2/3] probing reachability...")
	t := time.Now()
	alive := healthCheck(ctx, rawList)
	fmt.Printf("      alive:    %d HTTP + %d UDP  (%s)\n\n",
		len(alive.HTTP), len(alive.UDP), time.Since(t).Truncate(100*time.Millisecond))

	// 2) per-tracker announce
	fmt.Println("[3/3] querying each alive upstream with the info_hash...")
	q := buildAnnounceQuery(infoHash)

	type row struct {
		kind     string
		target   string
		peers    int
		seeders  int32
		leechers int32
	}
	rows := make([]row, 0, len(alive.HTTP)+len(alive.UDP))
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, probeConcurrency)

	queryHTTP := func(target string) {
		defer wg.Done()
		defer func() { <-sem }()
		sem <- struct{}{}
		cctx, cancel := context.WithTimeout(ctx, upstreamHTTPTimeout)
		defer cancel()
		r := queryHTTPTracker(cctx, target, q)
		mu.Lock()
		rows = append(rows, row{
			kind: "HTTP", target: target,
			peers:    len(r.Peers) / 6,
			seeders:  r.Complete,
			leechers: r.Incomplete,
		})
		mu.Unlock()
	}
	queryUDP := func(target string) {
		defer wg.Done()
		defer func() { <-sem }()
		sem <- struct{}{}
		cctx, cancel := context.WithTimeout(ctx, upstreamUDPTimeout)
		defer cancel()
		r := queryUDPTracker(cctx, target, q)
		mu.Lock()
		rows = append(rows, row{
			kind: "UDP", target: target,
			peers:    len(r.Peers) / 6,
			seeders:  r.Complete,
			leechers: r.Incomplete,
		})
		mu.Unlock()
	}
	for _, t := range alive.HTTP {
		wg.Add(1)
		go queryHTTP(t)
	}
	for _, t := range alive.UDP {
		wg.Add(1)
		go queryUDP(t)
	}
	wg.Wait()

	// 3) report
	var totalSeeds, totalLeech int32
	for _, r := range rows {
		totalSeeds += r.seeders
		totalLeech += r.leechers
	}

	fmt.Println()
	fmt.Println("=== per-tracker results ===")
	fmt.Printf("%-7s %-55s %5s %5s %5s\n", "KIND", "TRACKER", "PEERS", "LEECH", "SEEDS")
	fmt.Println(strings.Repeat("-", 90))
	var okCount int
	for _, r := range rows {
		status := "OK"
		if r.peers == 0 && r.seeders == 0 && r.leechers == 0 {
			status = "empty"
		} else {
			okCount++
		}
		display := r.target
		if len(display) > 54 {
			display = display[:51] + "..."
		}
		fmt.Printf("%-7s %-55s %5d %5d %5d  %s\n",
			r.kind, display, r.peers, r.leechers, r.seeders, status)
	}
	fmt.Println(strings.Repeat("-", 90))
	fmt.Printf("%d trackers responded with data out of %d\n", okCount, len(rows))
	fmt.Printf("sum(seeders)=%d  sum(leechers)=%d\n", totalSeeds, totalLeech)
}

// extractInfoHash returns 20-byte info_hash + canonical hex string.
// Accepts magnet:?xt=urn:btih:<hex> or raw 40-char hex.
func extractInfoHash(arg string) ([]byte, string, error) {
	hexStr := arg
	if strings.HasPrefix(arg, "magnet:") {
		u, err := url.Parse(arg)
		if err != nil {
			return nil, "", err
		}
		xt := u.Query().Get("xt")
		if !strings.HasPrefix(xt, "urn:btih:") {
			return nil, "", fmt.Errorf("magnet missing urn:btih: xt")
		}
		hexStr = strings.TrimPrefix(xt, "urn:btih:")
	}
	hexStr = strings.ToLower(strings.TrimSpace(hexStr))
	if len(hexStr) != 40 {
		return nil, "", fmt.Errorf("info_hash must be 40 hex chars, got %d", len(hexStr))
	}
	b, err := hex.DecodeString(hexStr)
	if err != nil {
		return nil, "", err
	}
	return b, hexStr, nil
}

// buildAnnounceQuery constructs the url.Values sent to every upstream.
// Fixed test parameters — actual /announce clients send more, but
// upstream trackers treat this consistently for diagnostic purposes.
func buildAnnounceQuery(infoHash []byte) url.Values {
	q := url.Values{}
	q.Set("info_hash", string(infoHash))
	q.Set("peer_id", "-qB4500-diagnose0001")
	q.Set("port", "6881")
	q.Set("uploaded", "0")
	q.Set("downloaded", "0")
	q.Set("left", "1") // want to be visible in swarm counts
	q.Set("compact", "1")
	q.Set("event", "started")
	q.Set("numwant", "50")
	return q
}

func main() {
	host := flag.String("host", defaultListenHost, "bind host (use 0.0.0.0 to listen on all interfaces)")
	port := flag.String("port", defaultListenPortFlag, "listen port")
	checkArg := flag.String("check", "", "if non-empty, run diagnostic for this magnet or 40-char hex info_hash and exit (does not start the proxy server)")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [-host HOST] [-port PORT] [-check MAGNET|HEX]\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "\nModes:\n")
		fmt.Fprintf(os.Stderr, "  (default)      Start the HTTP tracker proxy server\n")
		fmt.Fprintf(os.Stderr, "  -check ARG    Probe every upstream and report per-tracker announce results for the given magnet/hex\n")
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  %s -host 0.0.0.0 -port 8080\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -check 'magnet:?xt=urn:btih:<40-hex-info-hash>'\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -check <40-hex-info-hash>\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	if *checkArg != "" {
		runCheck(*checkArg)
		return
	}

	// SIGINT/SIGTERM 触发 ctx 取消,用于后台 goroutine + HTTP server shutdown
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go refreshLoop(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("/announce", announceHandler)

	addr := net.JoinHostPort(*host, *port)
	srv := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	// ctx 取消时优雅关闭 HTTP server
	go func() {
		<-ctx.Done()
		slog.Info("shutdown signal received")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			slog.Error("shutdown failed", "err", err)
		}
	}()

	slog.Info("tracker proxy listening", "addr", addr)
	// ListenAndServe 在 Shutdown 被调后返回 http.ErrServerClosed,这是正常路径
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		slog.Error("listen failed", "err", err)
		os.Exit(1)
	}
	slog.Info("tracker proxy stopped")
}
