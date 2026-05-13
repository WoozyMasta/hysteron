// Copyright 2026 WoozyMasta
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied
// See the License for the specific language governing permissions and
// limitations under the License.

package sentinel

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"html/template"
	"io/fs"
	"net/http"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/woozymasta/hysteron/internal/assets"
	"github.com/woozymasta/hysteron/internal/cluster"
	runtimecommon "github.com/woozymasta/hysteron/internal/runtime/common"
	"github.com/woozymasta/hysteron/internal/utils/buildflags"
)

type webOptions struct {
	ListenAddress string `long:"listen-address" env:"LISTEN_ADDRESS" description:"web status dashboard listen address, for example 0.0.0.0:8081 (disabled by default)"`
	BasePath      string `long:"base-path" env:"BASE_PATH" default:"/" validate-regex:"^/.*" description:"base path prefix for web UI and API routes"`

	AuthUsername string `long:"auth-username" env:"AUTH_USERNAME" and:"web-auth" description:"optional HTTP Basic auth username for web endpoints"`
	AuthPassword string `long:"auth-password" env:"AUTH_PASSWORD" and:"web-auth" secret:"true" description:"optional HTTP Basic auth password for web endpoints"`

	ReadTimeout  time.Duration `long:"read-timeout" env:"READ_TIMEOUT" default:"5s" validate-min:"0" description:"maximum duration for reading the entire request, including the body"`
	WriteTimeout time.Duration `long:"write-timeout" env:"WRITE_TIMEOUT" default:"10s" validate-min:"0" description:"maximum duration before timing out writes of the response"`

	AllowUnsafeAdminWithoutAuth bool `long:"allow-unsafe-admin-without-auth" env:"ALLOW_UNSAFE_ADMIN_WITHOUT_AUTH" description:"allow admin API endpoints when web auth is disabled (unsafe; intended only for controlled environments)"`
}

type sentinelWebRegistry struct {
	uid     string
	mu      sync.RWMutex
	runners map[string]*Sentinel
}

func newSentinelWebRegistry(uid string) *sentinelWebRegistry {
	return &sentinelWebRegistry{
		uid:     uid,
		runners: make(map[string]*Sentinel),
	}
}

func (r *sentinelWebRegistry) Set(clusterName string, s *Sentinel) {
	r.mu.Lock()
	r.runners[clusterName] = s
	r.mu.Unlock()
}

func (r *sentinelWebRegistry) Delete(clusterName string) {
	r.mu.Lock()
	delete(r.runners, clusterName)
	r.mu.Unlock()
}

func (r *sentinelWebRegistry) LocalLeadership(clusterName string) bool {
	r.mu.RLock()
	s := r.runners[clusterName]
	r.mu.RUnlock()
	if s == nil {
		return false
	}
	leader, _ := s.leaderInfo()
	return leader
}

type webSnapshot struct {
	GeneratedAt string             `json:"generated_at"`
	BasePath    string             `json:"base_path"`
	Build       webBuildInfo       `json:"build"`
	Sentinels   []webSentinelRow   `json:"sentinels"`
	Clusters    []webClusterStatus `json:"clusters"`
}

type webBuildInfo struct {
	Version string `json:"version"`
	Commit  string `json:"commit"`
	Date    string `json:"date"`
	URL     string `json:"url"`
}

type webSentinelRow struct {
	ClusterName string `json:"cluster_name"`
	UID         string `json:"uid"`
	IsLocal     bool   `json:"is_local"`
	IsLeader    bool   `json:"is_leader"`
}

type webClusterStatus struct {
	Name           string     `json:"name"`
	Phase          string     `json:"phase"`
	Generation     int64      `json:"generation"`
	FormatVersion  uint64     `json:"format_version"`
	MasterDBUID    string     `json:"master_db_uid"`
	MasterKeeperUID string    `json:"master_keeper_uid"`
	KeepersTotal   int        `json:"keepers_total"`
	KeepersHealthy int        `json:"keepers_healthy"`
	DBsTotal       int        `json:"dbs_total"`
	DBsHealthy     int        `json:"dbs_healthy"`
	ProxiesSeen    int        `json:"proxies_seen"`
	Error          string     `json:"error,omitempty"`
	KeeperRows     []webKeeperRow `json:"keeper_rows"`
	DBRows         []webDBRow `json:"db_rows"`
	ProxyRows      []webProxyRow `json:"proxy_rows"`
}

type webKeeperRow struct {
	UID                     string `json:"uid"`
	Healthy                 bool   `json:"healthy"`
	CanBeMaster             bool   `json:"can_be_master"`
	CanBeSynchronousReplica bool   `json:"can_be_synchronous_replica"`
	PGHealthy               bool   `json:"pg_healthy"`
	ListenAddress           string `json:"listen_address"`
	Generation              int64  `json:"generation"`
}

type webDBRow struct {
	UID       string `json:"uid"`
	KeeperUID string `json:"keeper_uid"`
	Role      string `json:"role"`
	Healthy   bool   `json:"healthy"`
	XLogPos   uint64 `json:"xlog_pos"`
	LagBytes  string `json:"lag_bytes"`
	Address   string `json:"address"`
}

type webProxyRow struct {
	UID          string `json:"uid"`
	Seen         bool   `json:"seen"`
	Enabled      bool   `json:"enabled"`
	Generation   int64  `json:"generation"`
	ProxyTimeout string `json:"proxy_timeout"`
}

func validateWebConfig(cfg *config) error {
	if cfg == nil {
		return errors.New("nil config")
	}
	if strings.TrimSpace(cfg.Web.BasePath) == "" {
		cfg.Web.BasePath = "/"
	}
	if cfg.Web.ReadTimeout == 0 {
		cfg.Web.ReadTimeout = 5 * time.Second
	}
	if cfg.Web.WriteTimeout == 0 {
		cfg.Web.WriteTimeout = 10 * time.Second
	}

	hasWebUser := strings.TrimSpace(cfg.Web.AuthUsername) != ""
	if cfg.Web.AllowUnsafeAdminWithoutAuth && !hasWebUser {
		log.Warn().Msg("web unsafe admin without auth is enabled; do not use in untrusted environments")
	}
	return nil
}

func newWebServer(cfg *config, clusterNames []string, registry *sentinelWebRegistry) *http.Server {
	basePath := normalizeWebBasePath(cfg.Web.BasePath)
	mux := http.NewServeMux()
	registerWebRoutes(mux, basePath, cfg, clusterNames, registry)
	return &http.Server{
		Addr:              cfg.Web.ListenAddress,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       cfg.Web.ReadTimeout,
		WriteTimeout:      cfg.Web.WriteTimeout,
	}
}

func normalizeWebBasePath(raw string) string {
	p := strings.TrimSpace(raw)
	if p == "" || p == "/" {
		return "/"
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	return strings.TrimRight(p, "/")
}

func registerWebRoutes(
	mux *http.ServeMux,
	basePath string,
	cfg *config,
	clusterNames []string,
	registry *sentinelWebRegistry,
) {
	tmpl := template.Must(template.ParseFS(
		assets.Web,
		"web/templates/layout.html",
		"web/templates/dashboard.html",
	))
	staticFS := assets.Web
	webFS, err := fs.Sub(staticFS, "web")
	if err != nil {
		log.Fatal().Err(err).Msg("failed to prepare embedded web assets filesystem")
	}
	static := http.FileServer(http.FS(webFS))
	mux.Handle("/health", healthHandler())
	mux.Handle("/healthz", healthHandler())
	mux.Handle("/health/live", healthHandler())
	mux.Handle("/health/ready", healthHandler())
	mux.Handle("/health/startup", healthHandler())

	root := webAuthMiddleware(cfg, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		snapshot := collectWebSnapshot(r.Context(), cfg, clusterNames, basePath, registry)
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		if err := tmpl.ExecuteTemplate(w, "layout.html", snapshot); err != nil {
			log.Error().Err(err).Msg("failed to render web dashboard template")
			http.Error(w, "failed to render dashboard", http.StatusInternalServerError)
		}
	}))
	status := webAuthMiddleware(cfg, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		snapshot := collectWebSnapshot(r.Context(), cfg, clusterNames, basePath, registry)
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		if err := json.NewEncoder(w).Encode(snapshot); err != nil {
			log.Debug().Err(err).Msg("failed to encode web status response")
		}
	}))

	if basePath == "/" {
		mux.Handle("/", root)
		mux.Handle("/api/v1/status", status)
		mux.Handle("/static/", http.StripPrefix("/static/", static))
		return
	}
	mux.Handle(basePath, http.RedirectHandler(basePath+"/", http.StatusTemporaryRedirect))
	mux.Handle(basePath+"/", http.StripPrefix(basePath, root))
	mux.Handle(basePath+"/api/v1/status", http.StripPrefix(basePath, status))
	mux.Handle(
		basePath+"/static/",
		http.StripPrefix(path.Clean(basePath)+"/static/", static),
	)
}

func healthHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
}

func collectWebSnapshot(
	ctx context.Context,
	cfg *config,
	clusterNames []string,
	basePath string,
	registry *sentinelWebRegistry,
) webSnapshot {
	names := append([]string(nil), clusterNames...)
	slices.Sort(names)
	snapshot := webSnapshot{
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		BasePath:    webLinkBasePath(basePath),
		Build: webBuildInfo{
			Version: buildflags.Version,
			Commit:  buildflags.Commit,
			Date:    buildflags.Date,
			URL:     buildflags.URL,
		},
		Sentinels: make([]webSentinelRow, 0, len(names)),
		Clusters:  make([]webClusterStatus, 0, len(names)),
	}

	timeout := cfg.Store.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	for _, clusterName := range names {
		clusterStatus := webClusterStatus{Name: clusterName}
		reqCtx, cancel := context.WithTimeout(ctx, timeout)
		s, err := runtimecommon.NewStoreForCluster(&cfg.CommonConfig, clusterName, false)
		if err != nil {
			cancel()
			clusterStatus.Error = err.Error()
			snapshot.Clusters = append(snapshot.Clusters, clusterStatus)
			continue
		}

		cdata, _, err := s.GetClusterData(reqCtx)
		if err != nil {
			clusterStatus.Error = err.Error()
		}
		sentinelsInfo, err := s.GetSentinelsInfo(reqCtx)
		if err == nil {
			for _, si := range sentinelsInfo {
				if si == nil {
					continue
				}
				isLocal := registry != nil && si.UID == registry.uid
				isLeader := false
				if isLocal {
					isLeader = registry.LocalLeadership(clusterName)
				}
				snapshot.Sentinels = append(snapshot.Sentinels, webSentinelRow{
					ClusterName: clusterName,
					UID:         si.UID,
					IsLocal:     isLocal,
					IsLeader:    isLeader,
				})
			}
		}

		var proxiesInfo map[string]*cluster.ProxyInfo
		rawProxiesInfo, err := s.GetProxiesInfo(reqCtx)
		if err == nil {
			clusterStatus.ProxiesSeen = len(rawProxiesInfo)
			proxiesInfo = rawProxiesInfo
		}
		if cdata != nil {
			clusterStatus.Phase = string(cdata.Cluster.Status.Phase)
			clusterStatus.Generation = cdata.Cluster.Generation
			clusterStatus.FormatVersion = cdata.FormatVersion
			clusterStatus.MasterDBUID = cdata.Cluster.Status.Master
			if master, ok := cdata.DBs[cdata.Cluster.Status.Master]; ok && master != nil && master.Spec != nil {
				clusterStatus.MasterKeeperUID = master.Spec.KeeperUID
			}
			clusterStatus.KeepersTotal = len(cdata.Keepers)
			keeperRows := make([]webKeeperRow, 0, len(cdata.Keepers))
			for _, k := range cdata.Keepers {
				if k == nil {
					continue
				}
				if k.Status.Healthy {
					clusterStatus.KeepersHealthy++
				}
				row := webKeeperRow{
					UID:                     k.UID,
					Healthy:                 k.Status.Healthy,
					CanBeMaster:             k.Status.CanBeMaster != nil && *k.Status.CanBeMaster,
					CanBeSynchronousReplica: k.Status.CanBeSynchronousReplica != nil && *k.Status.CanBeSynchronousReplica,
					Generation:              k.Generation,
				}
				for _, db := range cdata.DBs {
					if db == nil || db.Spec == nil || db.Spec.KeeperUID != k.UID {
						continue
					}
					row.PGHealthy = db.Status.Healthy
					row.ListenAddress = db.Status.ListenAddress + ":" + db.Status.Port
					break
				}
				keeperRows = append(keeperRows, row)
			}
			slices.SortFunc(keeperRows, func(a, b webKeeperRow) int {
				return strings.Compare(a.UID, b.UID)
			})
			clusterStatus.KeeperRows = keeperRows
			clusterStatus.DBsTotal = len(cdata.DBs)
			masterXLog := uint64(0)
			if master, ok := cdata.DBs[cdata.Cluster.Status.Master]; ok && master != nil {
				masterXLog = master.Status.XLogPos
			}
			rows := make([]webDBRow, 0, len(cdata.DBs))
			for _, db := range cdata.DBs {
				if db == nil || db.Spec == nil {
					continue
				}
				if db.Status.Healthy {
					clusterStatus.DBsHealthy++
				}
				lagBytes := "-"
				if db.UID != cdata.Cluster.Status.Master && masterXLog > db.Status.XLogPos {
					lagBytes = strconv.FormatUint(masterXLog-db.Status.XLogPos, 10)
				}
				rows = append(rows, webDBRow{
					UID:       db.UID,
					KeeperUID: db.Spec.KeeperUID,
					Role:      string(db.Spec.Role),
					Healthy:   db.Status.Healthy,
					XLogPos:   db.Status.XLogPos,
					LagBytes:  lagBytes,
					Address:   db.Status.ListenAddress + ":" + db.Status.Port,
				})
			}
			slices.SortFunc(rows, func(a, b webDBRow) int {
				return strings.Compare(a.UID, b.UID)
			})
			clusterStatus.DBRows = rows

			enabled := make(map[string]struct{})
			if cdata.Proxy != nil {
				for _, uid := range cdata.Proxy.Spec.EnabledProxies {
					if uid == "" {
						continue
					}
					enabled[uid] = struct{}{}
				}
			}
			allProxyUIDs := make(map[string]struct{}, len(enabled))
			for uid := range enabled {
				allProxyUIDs[uid] = struct{}{}
			}
			for uid := range proxiesInfo {
				allProxyUIDs[uid] = struct{}{}
			}
			proxyRows := make([]webProxyRow, 0, len(allProxyUIDs))
			for uid := range allProxyUIDs {
				row := webProxyRow{UID: uid}
				_, row.Enabled = enabled[uid]
				if pi, ok := proxiesInfo[uid]; ok && pi != nil {
					row.Seen = true
					row.Generation = pi.Generation
					row.ProxyTimeout = pi.ProxyTimeout.String()
				}
				proxyRows = append(proxyRows, row)
			}
			slices.SortFunc(proxyRows, func(a, b webProxyRow) int {
				return strings.Compare(a.UID, b.UID)
			})
			clusterStatus.ProxyRows = proxyRows
		}
		cancel()
		snapshot.Clusters = append(snapshot.Clusters, clusterStatus)
	}
	slices.SortFunc(snapshot.Sentinels, func(a, b webSentinelRow) int {
		if c := strings.Compare(a.ClusterName, b.ClusterName); c != 0 {
			return c
		}
		return strings.Compare(a.UID, b.UID)
	})
	return snapshot
}

func webLinkBasePath(basePath string) string {
	if basePath == "/" {
		return ""
	}
	return basePath
}

func webAuthMiddleware(cfg *config, next http.Handler) http.Handler {
	username := strings.TrimSpace(cfg.Web.AuthUsername)
	if username == "" {
		return next
	}
	password := cfg.Web.AuthPassword
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		if !ok || !compareConstantTime(user, username) || !compareConstantTime(pass, password) {
			w.Header().Set("WWW-Authenticate", `Basic realm="hysteron-sentinel-web", charset="UTF-8"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func compareConstantTime(got, want string) bool {
	gotBytes := []byte(got)
	wantBytes := []byte(want)
	if len(gotBytes) != len(wantBytes) {
		return false
	}
	return subtle.ConstantTimeCompare(gotBytes, wantBytes) == 1
}
