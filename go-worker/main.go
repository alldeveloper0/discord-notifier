// Discord Join Notifier Worker — Go port of discord-notifier/notifier_worker.py.
//
// One process per user (spawned by discord_notifier_bot.py). Watches every guild
// the user-token account is in and posts a join alert into a Discord group DM.
// Same selfbot pattern as the shadowping monitor worker, so it uses the proven
// gateway recipe (GO_WORKER_PORT.md §5): web-client IDENTIFY + op 37
// BULK_GUILD_SUBSCRIBE with member_updates:true per guild. ~12 MB RSS vs ~30-65 MB
// for the Python worker.
//
// Env (set by discord_notifier_bot.py start_worker, plus inherited):
//   ACCOUNT_TOKEN  user token (required)
//   GROUP_DM_ID    channel id to post join alerts into (required for delivery)
//   USER_CHAT_ID   telegram chat id (status notifications + hb file name)
//   BOT_TOKEN      telegram bot token (best-effort status messages)
//   DRY_RUN=1      log alerts instead of sending (isolated testing)
//   NOTIFIER_DIR   dir of notifier_data.json + hb files (default /root/discord-notifier)
//
// On token death (gateway close 4004 / confirmed-invalid) it marks the user
// inactive in notifier_data.json (so the bot's watchdog stops restarting it) and
// pings the user + admin on Telegram, then exits — matching _mark_disconnected.
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	gatewayURL   = "wss://gateway.discord.gg/?v=9&encoding=json"
	propsURL     = "https://cordapi.dolfi.es/api/v2/properties/web"
	capabilities = 22525
	discordEpoch = 1420070400000
	apiBase      = "https://discord.com/api/v9"
	adminChatID  = "6402264162"
)

const (
	opDispatch           = 0
	opHeartbeat          = 1
	opIdentify           = 2
	opResume             = 6
	opReconnect          = 7
	opInvalidSession     = 9
	opHello              = 10
	opHeartbeatACK       = 11
	opBulkGuildSubscribe = 37
)

type action int

const (
	actResume action = iota
	actIdentify
	actStop
)

type inbound struct {
	Op   int             `json:"op"`
	Seq  *int            `json:"s"`
	Type string          `json:"t"`
	Data json.RawMessage `json:"d"`
}

var httpClient = &http.Client{Timeout: 15 * time.Second}

type worker struct {
	token, groupDMID, chatID, botToken string
	notifierDir                         string
	dryRun                              bool
	props                               map[string]any
	encodedProps                        string
	userAgent                           string

	mu        sync.Mutex
	guildName map[string]string
	sessionID string
	resumeURL string
	seq       *int

	conn    *websocket.Conn
	writeMu sync.Mutex
	hbOnce  sync.Once
}

func logf(format string, args ...any) {
	fmt.Printf(time.Now().UTC().Format("15:04:05")+" "+format+"\n", args...)
}

// ---- gateway write helpers ----

func (w *worker) writeJSON(v any) error {
	w.writeMu.Lock()
	defer w.writeMu.Unlock()
	if w.conn == nil {
		return fmt.Errorf("no connection")
	}
	b, _ := json.Marshal(v)
	return w.conn.WriteMessage(websocket.TextMessage, b)
}

func (w *worker) sendHeartbeat() {
	w.mu.Lock()
	s := w.seq
	w.mu.Unlock()
	w.writeJSON(map[string]any{"op": opHeartbeat, "d": s})
}

func (w *worker) subscribeGuilds(ids []string) {
	if len(ids) == 0 {
		return
	}
	subs := map[string]any{}
	for _, id := range ids {
		subs[id] = map[string]any{
			"typing": true, "threads": true, "activities": true,
			"member_updates": true, "members": []any{},
			"thread_member_lists": []any{}, "channels": map[string]any{},
		}
	}
	w.writeJSON(map[string]any{"op": opBulkGuildSubscribe, "d": map[string]any{"subscriptions": subs}})
}

func (w *worker) knownGuildIDs() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	ids := make([]string, 0, len(w.guildName))
	for id := range w.guildName {
		ids = append(ids, id)
	}
	return ids
}

// ---- connection lifecycle ----

func (w *worker) run() {
	wantResume := false
	backoff := 5 * time.Second
	for {
		switch w.runConnection(wantResume) {
		case actStop:
			logf("token dead — worker exiting")
			return
		case actResume:
			wantResume = true
		case actIdentify:
			wantResume = false
			w.mu.Lock()
			w.sessionID, w.resumeURL, w.seq = "", "", nil
			w.mu.Unlock()
		}
		time.Sleep(backoff)
		if backoff < 300*time.Second {
			backoff *= 2
		}
	}
}

func (w *worker) runConnection(resume bool) action {
	w.mu.Lock()
	dialURL := gatewayURL
	if resume && w.resumeURL != "" {
		dialURL = w.resumeURL + "/?v=9&encoding=json"
	}
	sessionID := w.sessionID
	w.mu.Unlock()

	c, _, err := websocket.DefaultDialer.Dial(dialURL, nil)
	if err != nil {
		logf("dial err: %v", err)
		return actResume
	}
	w.writeMu.Lock()
	w.conn = c
	w.writeMu.Unlock()
	defer func() {
		c.Close()
		w.writeMu.Lock()
		w.conn = nil
		w.writeMu.Unlock()
	}()

	_, msg, err := c.ReadMessage()
	if err != nil {
		logf("read hello err: %v", err)
		return actResume
	}
	var hello inbound
	json.Unmarshal(msg, &hello)
	var hd struct {
		HeartbeatInterval int `json:"heartbeat_interval"`
	}
	json.Unmarshal(hello.Data, &hd)
	interval := time.Duration(hd.HeartbeatInterval) * time.Millisecond
	if interval <= 0 {
		interval = 41250 * time.Millisecond
	}

	done := make(chan struct{})
	var once sync.Once
	stop := func() { once.Do(func() { close(done) }) }
	defer stop()
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				w.sendHeartbeat()
			}
		}
	}()

	if resume && sessionID != "" {
		w.mu.Lock()
		s := w.seq
		w.mu.Unlock()
		w.writeJSON(map[string]any{"op": opResume,
			"d": map[string]any{"token": w.token, "session_id": sessionID, "seq": s}})
		logf("resuming session")
	} else {
		w.writeJSON(map[string]any{"op": opIdentify, "d": map[string]any{
			"token": w.token, "capabilities": capabilities, "properties": w.props,
			"presence":     map[string]any{"status": "unknown", "since": 0, "activities": []any{}, "afk": false},
			"compress":     false,
			"client_state": map[string]any{"guild_versions": map[string]any{}},
		}})
		logf("sent IDENTIFY")
	}

	for {
		_, message, err := c.ReadMessage()
		if err != nil {
			if ce, ok := err.(*websocket.CloseError); ok {
				logf("gateway closed code=%d", ce.Code)
				if ce.Code == 4004 {
					w.markDisconnected("Discord token no longer valid")
					return actStop
				}
			} else {
				logf("read err: %v", err)
			}
			return actResume
		}
		var ev inbound
		if json.Unmarshal(message, &ev) != nil {
			continue
		}
		if ev.Seq != nil {
			w.mu.Lock()
			w.seq = ev.Seq
			w.mu.Unlock()
		}
		switch ev.Op {
		case opDispatch:
			w.handleDispatch(ev)
		case opHeartbeat:
			w.sendHeartbeat()
		case opReconnect:
			return actResume
		case opInvalidSession:
			var resumable bool
			json.Unmarshal(ev.Data, &resumable)
			time.Sleep(2 * time.Second)
			if resumable {
				return actResume
			}
			return actIdentify
		case opHeartbeatACK:
		}
	}
}

func (w *worker) handleDispatch(ev inbound) {
	switch ev.Type {
	case "READY":
		var r struct {
			User             struct{ Username string } `json:"user"`
			SessionID        string                    `json:"session_id"`
			ResumeGatewayURL string                    `json:"resume_gateway_url"`
			Guilds           []json.RawMessage         `json:"guilds"`
		}
		json.Unmarshal(ev.Data, &r)
		w.mu.Lock()
		w.sessionID = r.SessionID
		if r.ResumeGatewayURL != "" {
			w.resumeURL = r.ResumeGatewayURL
		}
		w.mu.Unlock()
		for _, raw := range r.Guilds {
			w.upsertGuild(raw)
		}
		logf("READY as %s — watching %d guild(s)", r.User.Username, len(r.Guilds))
		w.subscribeGuilds(w.knownGuildIDs())
		w.startHeartbeatFile()
	case "RESUMED":
		logf("gateway resumed")
		w.subscribeGuilds(w.knownGuildIDs())
	case "GUILD_CREATE":
		if id := w.upsertGuild(ev.Data); id != "" {
			w.subscribeGuilds([]string{id})
		}
	case "GUILD_UPDATE":
		w.upsertGuild(ev.Data)
	case "GUILD_DELETE":
		var g struct {
			ID          string `json:"id"`
			Unavailable bool   `json:"unavailable"`
		}
		json.Unmarshal(ev.Data, &g)
		if g.ID != "" && !g.Unavailable {
			w.mu.Lock()
			delete(w.guildName, g.ID)
			w.mu.Unlock()
		}
	case "GUILD_MEMBER_ADD":
		w.handleMemberJoin(ev.Data)
	}
}

// upsertGuild stores guild name (user-session payloads nest it under "properties").
func (w *worker) upsertGuild(raw json.RawMessage) string {
	var g struct {
		ID          string `json:"id"`
		Name        string `json:"name"`
		Unavailable bool   `json:"unavailable"`
		Properties  *struct {
			Name string `json:"name"`
		} `json:"properties"`
	}
	if json.Unmarshal(raw, &g) != nil || g.ID == "" || g.Unavailable {
		return ""
	}
	name := g.Name
	if name == "" && g.Properties != nil {
		name = g.Properties.Name
	}
	w.mu.Lock()
	if _, ok := w.guildName[g.ID]; !ok || name != "" {
		w.guildName[g.ID] = name
	}
	w.mu.Unlock()
	return g.ID
}

func (w *worker) handleMemberJoin(raw json.RawMessage) {
	var m struct {
		GuildID string `json:"guild_id"`
		User    struct {
			ID       string `json:"id"`
			Username string `json:"username"`
			Bot      bool   `json:"bot"`
		} `json:"user"`
	}
	if json.Unmarshal(raw, &m) != nil || m.User.ID == "" || m.User.Bot {
		return
	}
	w.mu.Lock()
	guildName := w.guildName[m.GuildID]
	w.mu.Unlock()

	age := accountAge(m.User.ID)
	joinTime := time.Now().UTC().Format("02 Jan 2006, 03:04 PM") + " UTC"
	text := fmt.Sprintf("🔔 New Member Joined\n\n👤 `%s`\n🏠 %s\n📅 Account Age: %s\n🕐 Joined: %s",
		m.User.Username, guildName, age, joinTime)

	logf("join: %s in %s", m.User.Username, guildName)
	if w.groupDMID == "" {
		return
	}
	if w.dryRun {
		logf("[DRY_RUN] would send to group %s:\n%s", w.groupDMID, text)
		return
	}
	w.sendGroupDM(text)
}

// sendGroupDM posts a message into the group DM using the user token (REST). A
// browser User-Agent + X-Super-Properties are required or Cloudflare 403s a
// user-token request.
func (w *worker) sendGroupDM(text string) {
	body, _ := json.Marshal(map[string]any{"content": text})
	req, _ := http.NewRequest("POST", apiBase+"/channels/"+w.groupDMID+"/messages", bytes.NewReader(body))
	req.Header.Set("Authorization", w.token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", w.userAgent)
	if w.encodedProps != "" {
		req.Header.Set("X-Super-Properties", w.encodedProps)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		logf("[send] error: %v", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		logf("[send] HTTP %d posting to group %s", resp.StatusCode, w.groupDMID)
	}
}

// ---- heartbeat file (proof of life, matches hb_<chat_id>.txt) ----

func (w *worker) startHeartbeatFile() {
	w.hbOnce.Do(func() {
		go func() {
			path := filepath.Join(w.notifierDir, "hb_"+w.chatID+".txt")
			for {
				os.WriteFile(path, []byte(time.Now().UTC().Format(time.RFC3339Nano)), 0644)
				time.Sleep(120 * time.Second)
			}
		}()
	})
}

// ---- token-expiry handling (matches _mark_disconnected) ----

func (w *worker) markDisconnected(reason string) {
	w.updateDataDisconnected(reason)
	w.telegram(w.chatID, "🔴 Your Discord monitor has stopped.\n\nReason: "+reason+
		"\nUse /settoken to reconnect with a new token.")
	w.telegram(adminChatID, "🔴 Worker stopped for chat_id "+w.chatID+"\n\nReason: "+reason)
}

func (w *worker) updateDataDisconnected(reason string) {
	path := filepath.Join(w.notifierDir, "notifier_data.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		logf("[data] read failed: %v", err)
		return
	}
	var data map[string]any
	if json.Unmarshal(raw, &data) != nil {
		return
	}
	users, _ := data["users"].(map[string]any)
	if users == nil {
		return
	}
	u, _ := users[w.chatID].(map[string]any)
	if u == nil {
		return
	}
	u["active"] = false
	u["token_expired"] = true
	u["disconnect_reason"] = reason
	u["disconnect_time"] = time.Now().UTC().Format(time.RFC3339)
	out, _ := json.MarshalIndent(data, "", "  ")
	tmp := path + ".tmp"
	if os.WriteFile(tmp, out, 0644) == nil {
		os.Rename(tmp, path)
	}
}

func (w *worker) telegram(chatID, text string) {
	if w.botToken == "" {
		return
	}
	body, _ := json.Marshal(map[string]any{"chat_id": chatID, "text": text})
	req, _ := http.NewRequest("POST", "https://api.telegram.org/bot"+w.botToken+"/sendMessage", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if resp, err := httpClient.Do(req); err == nil {
		resp.Body.Close()
	}
}

// ---- helpers ----

func accountAge(userID string) string {
	n, err := strconv.ParseUint(userID, 10, 64)
	if err != nil {
		return ""
	}
	created := time.UnixMilli(int64(n>>22) + discordEpoch).UTC()
	days := int(time.Since(created) / (24 * time.Hour))
	if days < 0 {
		days = 0
	}
	if years := days / 365; years > 0 {
		return fmt.Sprintf("%dy %dd", years, days%365)
	}
	return fmt.Sprintf("%dd", days)
}

func fetchProperties() (map[string]any, string, error) {
	req, _ := http.NewRequest("POST", propsURL, nil)
	req.Header.Set("Accept", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()
	var body struct {
		Properties map[string]any `json:"properties"`
		Extra      map[string]any `json:"extra_gateway_properties"`
		Encoded    string         `json:"encoded"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, "", err
	}
	if body.Properties == nil {
		return nil, "", fmt.Errorf("no properties")
	}
	for k, v := range body.Extra {
		body.Properties[k] = v
	}
	return body.Properties, body.Encoded, nil
}

func main() {
	w := &worker{
		token:       os.Getenv("ACCOUNT_TOKEN"),
		groupDMID:   os.Getenv("GROUP_DM_ID"),
		chatID:      os.Getenv("USER_CHAT_ID"),
		botToken:    os.Getenv("BOT_TOKEN"),
		notifierDir: os.Getenv("NOTIFIER_DIR"),
		dryRun:      os.Getenv("DRY_RUN") == "1",
		guildName:   map[string]string{},
	}
	if w.token == "" {
		fmt.Println("ACCOUNT_TOKEN missing")
		os.Exit(1)
	}
	if w.chatID == "" {
		w.chatID = "unknown"
	}
	if w.notifierDir == "" {
		w.notifierDir = "/root/discord-notifier"
	}

	props, encoded, err := fetchProperties()
	if err != nil {
		fmt.Println("cordapi fetch failed:", err)
		os.Exit(1)
	}
	w.props = props
	w.encodedProps = encoded
	if ua, ok := props["browser_user_agent"].(string); ok {
		w.userAgent = ua
	}
	logf("notifier worker start chat=%s group=%s dry_run=%v build=%v",
		w.chatID, w.groupDMID, w.dryRun, props["client_build_number"])
	if w.dryRun {
		logf("DRY_RUN active — alerts will be logged, not sent")
	}
	if strings.TrimSpace(w.groupDMID) == "" {
		logf("[WARN] GROUP_DM_ID empty — joins will be logged but not delivered")
	}

	w.run()
}
