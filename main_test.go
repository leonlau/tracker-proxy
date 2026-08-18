package main

import (
	"encoding/binary"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/zeebo/bencode"
)

// 构造一段 20 字节的 info_hash 用于测试
func makeInfoHash(b byte) []byte {
	h := make([]byte, 20)
	for i := range h {
		h[i] = b
	}
	return h
}

func TestBuildAnnouncePacket_Layout(t *testing.T) {
	connID := uint64(0x0102030405060708)
	tx := uint32(0xAABBCCDD)
	infoHash := makeInfoHash(0x11)
	peerID := makeInfoHash(0x22)
	downloaded := uint64(0x1111111111111111)
	left := uint64(0x2222222222222222)
	uploaded := uint64(0x3333333333333333)
	event := uint32(1) // started
	key := uint32(0x55555555)
	port := uint16(6881)

	pkt := buildAnnouncePacket(connID, tx, infoHash, peerID,
		downloaded, left, uploaded, event, key, port)

	if len(pkt) != 98 {
		t.Fatalf("packet length = %d, want 98", len(pkt))
	}

	checkU64 := func(off int, want uint64, label string) {
		t.Helper()
		got := binary.BigEndian.Uint64(pkt[off : off+8])
		if got != want {
			t.Errorf("%s at [%d:%d] = %#x, want %#x", label, off, off+8, got, want)
		}
	}
	checkU32 := func(off int, want uint32, label string) {
		t.Helper()
		got := binary.BigEndian.Uint32(pkt[off : off+4])
		if got != want {
			t.Errorf("%s at [%d:%d] = %#x, want %#x", label, off, off+4, got, want)
		}
	}
	checkU16 := func(off int, want uint16, label string) {
		t.Helper()
		got := binary.BigEndian.Uint16(pkt[off : off+2])
		if got != want {
			t.Errorf("%s at [%d:%d] = %#x, want %#x", label, off, off+2, got, want)
		}
	}
	checkBytes := func(off, n int, want []byte, label string) {
		t.Helper()
		got := pkt[off : off+n]
		for i := range n {
			if got[i] != want[i] {
				t.Errorf("%s at offset %d: byte %d = %#x, want %#x",
					label, off, i, got[i], want[i])
				break
			}
		}
	}

	checkU64(0, connID, "connection_id")
	checkU32(8, 1, "action(announce)")
	checkU32(12, tx, "transaction_id")
	checkBytes(16, 20, infoHash, "info_hash")
	checkBytes(36, 20, peerID, "peer_id")
	checkU64(56, downloaded, "downloaded")
	checkU64(64, left, "left")
	checkU64(72, uploaded, "uploaded")
	checkU32(80, event, "event")
	checkU32(84, 0, "IP(0=auto)")
	checkU32(88, key, "key")
	checkU32(92, defaultNumWant, "num_want")
	checkU16(96, port, "port")
}

func TestDedupeIPv4Peers(t *testing.T) {
	peer := func(ip byte, port uint16) []byte {
		return []byte{ip, 0, 0, 0, byte(port >> 8), byte(port)}
	}

	tests := []struct {
		name  string
		parts [][]byte
		want  []byte
	}{
		{
			name:  "empty",
			parts: nil,
			want:  nil,
		},
		{
			name:  "single segment no dup",
			parts: [][]byte{peer(1, 6881)},
			want:  peer(1, 6881),
		},
		{
			name: "dedupe within single segment",
			parts: [][]byte{
				append(peer(1, 6881), peer(1, 6881)...),
			},
			want: peer(1, 6881),
		},
		{
			name: "dedupe across segments",
			parts: [][]byte{
				peer(1, 6881),
				peer(1, 6881),
				peer(2, 6881),
			},
			want: append(peer(1, 6881), peer(2, 6881)...),
		},
		{
			name: "different ports same ip are different peers",
			parts: [][]byte{
				peer(1, 6881),
				peer(1, 6882),
			},
			want: append(peer(1, 6881), peer(1, 6882)...),
		},
		{
			name: "truncate trailing partial bytes",
			parts: [][]byte{
				append(peer(1, 6881), 0xFF, 0xAA, 0xBB),
			},
			want: peer(1, 6881),
		},
		{
			name: "preserve order of first occurrence",
			parts: [][]byte{
				append(peer(1, 6881), peer(2, 6881)...),
				append(peer(2, 6881), peer(3, 6881)...),
			},
			want: append(append(peer(1, 6881), peer(2, 6881)...), peer(3, 6881)...),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := dedupeIPv4Peers(tt.parts)
			if !bytesEqual(got, tt.want) {
				t.Errorf("dedupeIPv4Peers = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEventCode(t *testing.T) {
	tests := []struct {
		in   string
		want uint32
	}{
		{"", 0},
		{"", 0},
		{"started", 1},
		{"stopped", 2},
		{"completed", 3},
		{"unknown", 0},
		{"STARTED", 0}, // 大小写敏感
	}
	for _, tt := range tests {
		if got := eventCode(tt.in); got != tt.want {
			t.Errorf("eventCode(%q) = %d, want %d", tt.in, got, tt.want)
		}
	}
}

func TestCacheGetSet(t *testing.T) {
	key := "test-key"
	want := UpstreamResult{
		Peers:      []byte{1, 2, 3, 4, 5, 6},
		Complete:   10,
		Incomplete: 20,
	}

	// 不存在的 key
	if got, ok := cacheGet(key); ok {
		t.Errorf("cacheGet on missing key returned ok=true, data=%+v", got)
	}

	cacheSet(key, want)

	// 写入后立即可读
	got, ok := cacheGet(key)
	if !ok {
		t.Fatalf("cacheGet after cacheSet returned ok=false")
	}
	if !bytesEqual(got.Peers, want.Peers) {
		t.Errorf("cacheGet.Peers = %v, want %v", got.Peers, want.Peers)
	}
	if got.Complete != want.Complete {
		t.Errorf("cacheGet.Complete = %d, want %d", got.Complete, want.Complete)
	}
	if got.Incomplete != want.Incomplete {
		t.Errorf("cacheGet.Incomplete = %d, want %d", got.Incomplete, want.Incomplete)
	}

	// 强制过期
	cache.Store(key, CacheItem{
		Result: want,
		Expire: time.Now().Add(-1 * time.Second),
	})
	if _, ok := cacheGet(key); ok {
		t.Errorf("cacheGet on expired item returned ok=true")
	}
}

func TestRawInfoHash_BinaryPreserved(t *testing.T) {
	// 模拟 BT 客户端把 20 字节原始 info_hash URL 编码发送
	raw := make([]byte, 20)
	for i := range raw {
		raw[i] = byte(i)
	}

	q := url.Values{}
	q.Set("info_hash", string(raw))

	got := rawInfoHash(q)
	if len(got) != 20 {
		t.Fatalf("rawInfoHash length = %d, want 20", len(got))
	}
	for i := range got {
		if got[i] != raw[i] {
			t.Errorf("byte %d: got %#x, want %#x", i, got[i], raw[i])
			break
		}
	}
}

func TestRawInfoHash_URLEncodedInput(t *testing.T) {
	// 构造含特殊字节(0x12, 0xAB 等)的 info_hash
	raw := []byte{0x12, 0x34, 0xAB, 0xCD, 0xEF, 0x12, 0x34, 0xAB, 0xCD, 0xEF,
		0x00, 0xFF, 0x12, 0x34, 0xAB, 0xCD, 0xEF, 0x12, 0x34, 0xAB}

	// url.Values 的 Set 内部会做 URL encode,Set 回来应该还原
	q := url.Values{}
	q.Set("info_hash", string(raw))

	got := rawInfoHash(q)
	if !bytesEqual(got, raw) {
		t.Errorf("URL-encoded roundtrip failed:\n got %v\nwant %v", got, raw)
	}
}

func TestRawInfoHash_Missing(t *testing.T) {
	q := url.Values{}
	if got := rawInfoHash(q); got != nil {
		t.Errorf("rawInfoHash missing = %v, want nil", got)
	}
}

func TestRawPeerID(t *testing.T) {
	raw := make([]byte, 20)
	for i := range raw {
		raw[i] = byte(0xF0 | i)
	}
	q := url.Values{}
	q.Set("peer_id", string(raw))

	got := rawPeerID(q)
	if !bytesEqual(got, raw) {
		t.Errorf("rawPeerID roundtrip failed:\n got %v\nwant %v", got, raw)
	}
}

func TestUint64Param(t *testing.T) {
	q := url.Values{}
	q.Set("a", "12345")
	q.Set("b", "not a number")
	q.Set("c", "9999999999999")

	if got := uint64Param(q, "a"); got != 12345 {
		t.Errorf("uint64Param(a) = %d, want 12345", got)
	}
	if got := uint64Param(q, "b"); got != 0 {
		t.Errorf("uint64Param(bad) = %d, want 0", got)
	}
	if got := uint64Param(q, "c"); got != 9999999999999 {
		t.Errorf("uint64Param(c) = %d, want 9999999999999", got)
	}
	if got := uint64Param(q, "missing"); got != 0 {
		t.Errorf("uint64Param(missing) = %d, want 0", got)
	}
}

func TestUint32Param(t *testing.T) {
	q := url.Values{}
	q.Set("p", "6881")
	q.Set("bad", "abc")

	if got := uint32Param(q, "p"); got != 6881 {
		t.Errorf("uint32Param(p) = %d, want 6881", got)
	}
	if got := uint32Param(q, "bad"); got != 0 {
		t.Errorf("uint32Param(bad) = %d, want 0", got)
	}
}

func TestSendResponse_BencodeFormat(t *testing.T) {
	// 验证 sendResponse 输出是合法的 bencode 且 summary 透传
	result := UpstreamResult{
		Peers:      []byte{1, 2, 3, 4, 5, 6},
		Complete:   42,
		Incomplete: 7,
	}

	rec := httptest.NewRecorder()
	sendResponse(rec, result)

	body := rec.Body.Bytes()
	if !strings.HasPrefix(string(body), "d") {
		t.Errorf("bencode response should start with 'd', got %q", body[:1])
	}

	var decoded map[string]any
	if err := bencode.DecodeBytes(body, &decoded); err != nil {
		t.Fatalf("bencode decode failed: %v\nbody: %s", err, body)
	}

	for _, k := range []string{"interval", "min interval", "complete", "incomplete", "peers"} {
		if _, ok := decoded[k]; !ok {
			t.Errorf("response missing key %q", k)
		}
	}

	gotPeers, ok := decoded["peers"].(string)
	if !ok {
		t.Fatalf("response peers type = %T, want string", decoded["peers"])
	}
	if gotPeers != string(result.Peers) {
		t.Errorf("response peers = %v, want %v", gotPeers, result.Peers)
	}

	if got := decoded["complete"].(int64); got != int64(result.Complete) {
		t.Errorf("response complete = %d, want %d", got, result.Complete)
	}
	if got := decoded["incomplete"].(int64); got != int64(result.Incomplete) {
		t.Errorf("response incomplete = %d, want %d", got, result.Incomplete)
	}

	if got := rec.Header().Get("Content-Type"); got != "text/plain" {
		t.Errorf("Content-Type = %q, want text/plain", got)
	}
}

func TestSendResponse_EncodeError(t *testing.T) {
	// 当前实现里 peers=nil 也能正常编码,只验证不 panic
	rec := httptest.NewRecorder()
	sendResponse(rec, UpstreamResult{})

	if rec.Body.Len() == 0 {
		t.Errorf("expected bencode body")
	}
}

func TestParseTrackerList(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantHTTP []string
		wantUDP  []string
	}{
		{
			name:  "empty",
			input: "",
		},
		{
			name:  "only blank lines",
			input: "\n\n   \n\t\n",
		},
		{
			name:  "only comments",
			input: "# this is a comment\n# another one\n",
		},
		{
			name:  "blanks and comments mixed with trackers",
			input: "\n# header\nudp://a.example:1337/announce\n\n# mid\nhttp://b.example/announce\n",
			wantUDP: []string{
				"a.example:1337",
			},
			wantHTTP: []string{
				"http://b.example/announce",
			},
		},
		{
			name: "udp extracts host:port, drops path",
			input: "udp://tracker.example.com:1337/announce\n" +
				"udp://other.example.org:6969/announce\n",
			wantUDP: []string{
				"tracker.example.com:1337",
				"other.example.org:6969",
			},
		},
		{
			name: "http and https preserved verbatim",
			input: "http://h.example/announce\n" +
				"https://hs.example:443/announce\n",
			wantHTTP: []string{
				"http://h.example/announce",
				"https://hs.example:443/announce",
			},
		},
		{
			name: "dedup within scheme",
			input: "udp://a.example:1337/announce\n" +
				"udp://a.example:1337/announce\n" +
				"http://b.example/announce\n" +
				"http://b.example/announce\n",
			wantUDP:  []string{"a.example:1337"},
			wantHTTP: []string{"http://b.example/announce"},
		},
		{
			name: "whitespace around lines is trimmed",
			input: "  udp://a.example:1337/announce  \n" +
				"\thttps://b.example/announce\t\n",
			wantUDP:  []string{"a.example:1337"},
			wantHTTP: []string{"https://b.example/announce"},
		},
		{
			name: "unknown scheme is skipped",
			input: "wss://not.a.tracker\n" +
				"udp://a.example:1337/announce\n",
			wantUDP: []string{"a.example:1337"},
		},
		{
			name:  "udp without port dropped",
			input: "udp://no.port.example/announce\n",
		},
		{
			name: "realistic trackerslist sample",
			input: `
udp://zer0day.ch:1337/announce

udp://tracker.opentrackr.org:1337/announce
http://tracker.opentrackr.org:1337/announce
https://tracker.zhuqiy.com:443/announce
`,
			wantUDP: []string{
				"zer0day.ch:1337",
				"tracker.opentrackr.org:1337",
			},
			wantHTTP: []string{
				"http://tracker.opentrackr.org:1337/announce",
				"https://tracker.zhuqiy.com:443/announce",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			list := parseTrackerList([]byte(tt.input))
			if !stringSlicesEqual(list.HTTP, tt.wantHTTP) {
				t.Errorf("HTTP = %v, want %v", list.HTTP, tt.wantHTTP)
			}
			if !stringSlicesEqual(list.UDP, tt.wantUDP) {
				t.Errorf("UDP = %v, want %v", list.UDP, tt.wantUDP)
			}
		})
	}
}

func TestParseTrackerList_LongLineBuffer(t *testing.T) {
	// 一行超长也能正确处理(确保 bufio.Scanner 缓冲够大)
	long := strings.Repeat("a", 200_000)
	input := "udp://h.example:1337/announce\n" + long + "\nudp://h2.example:80/announce\n"
	list := parseTrackerList([]byte(input))
	if len(list.UDP) != 2 {
		t.Errorf("expected 2 UDP trackers, got %d: %v", len(list.UDP), list.UDP)
	}
}

func TestParseUDPAnnounceResponse(t *testing.T) {
	// 构造一个合法 BEP 15 announce response
	// 0-4   action (1, 已校验)
	// 4-8   transaction_id
	// 8-12  interval
	// 12-16 leechers
	// 16-20 seeders
	// 20+   peers (compact IPv4)
	buf := make([]byte, 32)
	binary.BigEndian.PutUint32(buf[0:4], 1)
	binary.BigEndian.PutUint32(buf[4:8], 0xAABBCCDD)
	binary.BigEndian.PutUint32(buf[8:12], 1800)   // interval
	binary.BigEndian.PutUint32(buf[12:16], 42)    // leechers -> incomplete
	binary.BigEndian.PutUint32(buf[16:20], 7)     // seeders -> complete
	// 2 个 peer: 1.2.3.4:6881, 5.6.7.8:6882
	buf[20] = 1
	buf[21] = 2
	buf[22] = 3
	buf[23] = 4
	binary.BigEndian.PutUint16(buf[24:26], 6881)
	buf[26] = 5
	buf[27] = 6
	buf[28] = 7
	buf[29] = 8
	binary.BigEndian.PutUint16(buf[30:32], 6882)

	r := parseUDPAnnounceResponse(buf)

	if r.Complete != 7 {
		t.Errorf("Complete = %d, want 7", r.Complete)
	}
	if r.Incomplete != 42 {
		t.Errorf("Incomplete = %d, want 42", r.Incomplete)
	}
	if !bytesEqual(r.Peers, []byte{1, 2, 3, 4, 0x1A, 0xE1, 5, 6, 7, 8, 0x1A, 0xE2}) {
		t.Errorf("Peers = %v, want 2 IPv4 peers", r.Peers)
	}
}

func TestParseUDPAnnounceResponse_EmptyPeers(t *testing.T) {
	// 最小合法 response (20 字节,无 peer)
	buf := make([]byte, 20)
	binary.BigEndian.PutUint32(buf[0:4], 1)
	binary.BigEndian.PutUint32(buf[8:12], 1800)
	binary.BigEndian.PutUint32(buf[12:16], 100)
	binary.BigEndian.PutUint32(buf[16:20], 50)

	r := parseUDPAnnounceResponse(buf)

	if r.Complete != 50 || r.Incomplete != 100 {
		t.Errorf("got complete=%d incomplete=%d, want 50/100",
			r.Complete, r.Incomplete)
	}
	if len(r.Peers) != 0 {
		t.Errorf("expected empty peers, got %d bytes", len(r.Peers))
	}
}

func TestParseUDPAnnounceResponse_TooShort(t *testing.T) {
	r := parseUDPAnnounceResponse([]byte{1, 2, 3})
	if r.Peers != nil || r.Complete != 0 || r.Incomplete != 0 {
		t.Errorf("short buf should return zero result, got %+v", r)
	}
}

func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range len(a) {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// ---------- helpers ----------

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range len(a) {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}