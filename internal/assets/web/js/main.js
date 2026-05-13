(function () {
  var refresh = document.getElementById("refresh");
  var themeButton = document.getElementById("theme");
  var timer = null;
  var basePath = document.body.getAttribute("data-base-path") || "";
  var openDetailKeys = new Set();

  function esc(value) {
    return String(value == null ? "" : value)
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");
  }

  function boolText(value) {
    return value ? "true" : "false";
  }

  function boolBadge(value, falsyClass) {
    var cls = value ? "is-true" : (falsyClass || "is-false");
    return '<span class="bool-badge ' + cls + '">' + boolText(value) + "</span>";
  }

  function mono(value) {
    return '<span class="mono">' + esc(value) + "</span>";
  }

  function escAttr(value) {
    return esc(value).replaceAll("`", "&#96;");
  }

  function detailToggle(kind, id, obj) {
    var payload = encodeURIComponent(JSON.stringify(obj, null, 2));
    var key = kind + ":" + String(id);
    return '<button type="button" class="row-toggle" data-kind="' + escAttr(kind) +
      '" data-id="' + escAttr(id) + '" data-key="' + escAttr(key) + '" data-payload="' + payload +
      '" aria-expanded="false" title="Show details">+</button>';
  }

  function detailRow(colspan) {
    return '<tr class="detail-row" hidden><td colspan="' + colspan +
      '"><div class="detail-wrap"></div></td></tr>';
  }

  function renderValue(value) {
    if (value == null) return '<span class="detail-empty">-</span>';
    if (Array.isArray(value)) {
      if (value.length === 0) return '<span class="detail-empty">[]</span>';
      return esc(value.map(function (v) {
        if (typeof v === "object" && v !== null) return JSON.stringify(v);
        return String(v);
      }).join(", "));
    }
    if (typeof value === "object") {
      return "<code>" + esc(JSON.stringify(value)) + "</code>";
    }
    if (typeof value === "boolean") return boolBadge(value, "is-neutral");
    return esc(String(value));
  }

  function renderDetailTable(obj) {
    if (!obj || typeof obj !== "object") return '<span class="detail-empty">no details</span>';
    var keys = Object.keys(obj).sort();
    if (keys.length === 0) return '<span class="detail-empty">empty object</span>';
    var rows = keys.map(function (k) {
      return "<tr><th>" + esc(k) + "</th><td>" + renderValue(obj[k]) + "</td></tr>";
    }).join("");
    return '<table class="detail-table"><tbody>' + rows + "</tbody></table>";
  }

  function clamp(value, min, max) {
    if (value < min) return min;
    if (value > max) return max;
    return value;
  }

  function mixColor(a, b, t) {
    var r = Math.round(a[0] + (b[0] - a[0]) * t);
    var g = Math.round(a[1] + (b[1] - a[1]) * t);
    var bl = Math.round(a[2] + (b[2] - a[2]) * t);
    return "rgb(" + r + "," + g + "," + bl + ")";
  }

  // direction:
  // - "higher-worse": min=green, mid=yellow, max=red
  // - "lower-worse": min=red, mid=yellow, max=green
  function heatColor(value, opts) {
    if (!Number.isFinite(value)) return "";
    var min = Number(opts.min);
    var mid = Number(opts.mid);
    var max = Number(opts.max);
    if (!(min < mid && mid < max)) return "";
    var direction = opts.direction || "higher-worse";
    var good = [34, 197, 94];
    var warn = [245, 158, 11];
    var bad = [239, 68, 68];
    if (direction === "lower-worse") {
      var tmp = good;
      good = bad;
      bad = tmp;
    }
    var v = clamp(value, min, max);
    if (v <= mid) {
      return mixColor(good, warn, (v - min) / (mid - min));
    }
    return mixColor(warn, bad, (v - mid) / (max - mid));
  }

  function coloredNumber(value, opts, suffix) {
    if (!Number.isFinite(value)) return esc(value);
    var color = heatColor(value, opts);
    var text = String(value) + (suffix || "");
    if (!color) return text;
    return '<span class="metric-color" style="color:' + color + '">' + esc(text) + "</span>";
  }

  function ratioBadge(ok, total, direction) {
    if (!Number.isFinite(ok) || !Number.isFinite(total) || total <= 0) {
      return esc(ok) + "/" + esc(total);
    }
    var percent = (ok / total) * 100;
    var color = heatColor(percent, {
      min: 0,
      mid: 70,
      max: 100,
      direction: direction || "lower-worse",
    });
    var text = ok + "/" + total;
    return '<span class="metric-color mono" style="color:' + color + '">' + esc(text) + "</span>";
  }

  function statusPath() {
    return (basePath || "") + "/api/v1/status";
  }

  function renderSentinels(rows) {
    var body = document.getElementById("sentinels-body");
    if (!body) return;
    if (!rows || rows.length === 0) {
      body.innerHTML = '<tr><td colspan="4">no sentinel data</td></tr>';
      return;
    }

    function codeList(value) {
      if (!value || value.length === 0) return "-";
      return value.map(function (v) {
        return mono(v);
      }).join(" ");
    }
    body.innerHTML = rows.map(function (row) {
      return "<tr>" +
        "<td>" + detailToggle("sentinel", row.uid, row) + " " + mono(row.uid) + "</td>" +
        "<td>" + boolBadge(row.is_local, "is-neutral") + "</td>" +
        "<td>" + codeList(row.leader_clusters) + "</td>" +
        "<td>" + codeList(row.clusters) + "</td>" +
        "</tr>" + detailRow(4);
    }).join("");
  }

  function renderKeepers(rows) {
    if (!rows || rows.length === 0) {
      return '<tr><td colspan="6">no keeper rows</td></tr>';
    }
    return rows.map(function (row) {
      return "<tr>" +
        "<td>" + detailToggle("keeper", row.uid, row) + " " + mono(row.uid) + "</td>" +
        "<td>" + boolBadge(row.healthy) + "</td>" +
        "<td>" + boolBadge(row.pg_healthy) + "</td>" +
        "<td>" + boolBadge(row.can_be_master) + "</td>" +
        "<td>" + boolBadge(row.can_be_synchronous_replica) + "</td>" +
        "<td>" + mono(row.listen_address) + "</td>" +
        "</tr>" + detailRow(6);
    }).join("");
  }

  function renderDBs(rows) {
    if (!rows || rows.length === 0) {
      return '<tr><td colspan="7">no database rows</td></tr>';
    }
    return rows.map(function (row) {
      var lagValue = Number(row.lag_bytes);
      var lag = row.lag_bytes === "-" || !Number.isFinite(lagValue) ?
        esc(row.lag_bytes) :
        coloredNumber(lagValue, {
          min: 0,
          mid: 16 * 1024 * 1024,
          max: 128 * 1024 * 1024,
          direction: "higher-worse",
        });
      return "<tr>" +
        "<td>" + detailToggle("database", row.uid, row) + " " + mono(row.uid) + "</td>" +
        "<td>" + mono(row.keeper_uid) + "</td>" +
        "<td>" + mono(row.pg_version || "-") + "</td>" +
        "<td>" + esc(row.role) + "</td>" +
        "<td>" + boolBadge(row.healthy) + "</td>" +
        "<td>" + mono(row.xlog_pos) + "</td>" +
        "<td>" + lag + "</td>" +
        "</tr>" + detailRow(7);
    }).join("");
  }

  function listenerPill(label, address, active) {
    if (!address) return "";
    var cls = active ? "up" : "down";
    return '<span class="listener-pill ' + cls + '">' + esc(label) + " " + mono(address) + "</span>";
  }

  function renderProxyListeners(row) {
    var rw = listenerPill("RW", row.rw_address, row.rw_active);
    var ro = listenerPill("RO", row.ro_address, row.ro_active);
    var out = [rw, ro].filter(Boolean).join(" ");
    return out || "-";
  }

  function renderProxies(rows) {
    if (!rows || rows.length === 0) {
      return '<tr><td colspan="7">no proxy rows</td></tr>';
    }
    return rows.map(function (row) {
      return "<tr>" +
        "<td>" + detailToggle("proxy", row.uid, row) + " " + mono(row.uid) + "</td>" +
        "<td>" + mono(row.mode || "-") + "</td>" +
        "<td>" + renderProxyListeners(row) + "</td>" +
        "<td>" + boolBadge(row.seen) + "</td>" +
        "<td>" + boolBadge(row.enabled) + "</td>" +
        "<td>" + mono(row.generation) + "</td>" +
        "<td>" + mono(row.proxy_timeout) + "</td>" +
        "</tr>" + detailRow(7);
    }).join("");
  }

  function renderClusters(rows) {
    var root = document.getElementById("clusters-root");
    if (!root) return;
    root.innerHTML = (rows || []).map(function (cluster) {
      var errorBlock = "";
      if (cluster.error) {
        errorBlock = '<div class="alert alert-error">Error: ' + esc(cluster.error) + "</div>";
      }
      return "<section>" +
        "<h2>Cluster: " + esc(cluster.name) + "</h2>" +
        errorBlock +
        '<div class="meta">' +
        '<div title="Cluster phase from cluster data">Phase: ' + esc(cluster.phase) + "</div>" +
        '<div title="Cluster object generation in DCS">Generation: ' + mono(cluster.generation) + "</div>" +
        '<div title="UID of the current writable database">Master DB: ' + mono(cluster.master_db_uid) + "</div>" +
        '<div title="Keeper assigned to current writable database">Master Keeper: ' + mono(cluster.master_keeper_uid) + "</div>" +
        '<div title="Healthy keepers / total keepers">Keepers: ' + ratioBadge(cluster.keepers_healthy, cluster.keepers_total, "lower-worse") + "</div>" +
        '<div title="Healthy databases / total databases">DBs: ' + ratioBadge(cluster.dbs_healthy, cluster.dbs_total, "lower-worse") + "</div>" +
        '<div title="Number of proxy heartbeats visible in DCS">Proxies Seen: ' + mono(cluster.proxies_seen) + "</div>" +
        "</div>" +
        "<h3>Keepers</h3>" +
        "<table><thead><tr>" +
        '<th title="Keeper unique identifier">Keeper UID</th>' +
        '<th title="Keeper health status reported to DCS">Healthy</th>' +
        '<th title="PostgreSQL health for keeper\'s assigned database">PG Healthy</th>' +
        '<th title="Keeper is eligible to become writable primary">Can Be Master</th>' +
        '<th title="Keeper is eligible for synchronous standby selection">Can Be Sync Replica</th>' +
        '<th title="Keeper PostgreSQL listen address">Address</th>' +
        "</tr></thead><tbody>" + renderKeepers(cluster.keeper_rows) + "</tbody></table>" +
        "<h3>Databases</h3>" +
        "<table><thead><tr>" +
        '<th title="Database object unique identifier">DB UID</th>' +
        '<th title="Keeper currently assigned to DB">Keeper UID</th>' +
        '<th title="PostgreSQL binary version reported by keeper">PG Version</th>' +
        '<th title="DB role in cluster topology">Role</th>' +
        '<th title="DB health status">Healthy</th>' +
        '<th title="Current WAL position">XLog Pos</th>' +
        '<th title="Estimated lag from current primary in bytes">Lag Bytes</th>' +
        "</tr></thead><tbody>" + renderDBs(cluster.db_rows) + "</tbody></table>" +
        "<h3>Proxies</h3>" +
        "<table><thead><tr>" +
        '<th title="Proxy unique identifier">Proxy UID</th>' +
        '<th title="Active listener mode(s): write/read/write+read">Mode</th>' +
        '<th title="Proxy listeners and runtime status">Listeners</th>' +
        '<th title="Proxy heartbeat currently visible in DCS">Seen</th>' +
        '<th title="Proxy is enabled by current cluster proxy spec">Enabled</th>' +
        '<th title="Last seen proxy generation">Generation</th>' +
        '<th title="Proxy timeout announced by proxy">Proxy Timeout</th>' +
        "</tr></thead><tbody>" + renderProxies(cluster.proxy_rows) + "</tbody></table>" +
        "</section>";
    }).join("");
  }

  function renderSnapshot(snapshot) {
    var updated = document.getElementById("updated-at");
    var clustersCount = document.getElementById("clusters-count");
    var sentinelsCount = document.getElementById("sentinels-count");
    if (updated) updated.textContent = "Updated: " + (snapshot.generated_at || "");
    if (clustersCount) clustersCount.textContent = "Clusters: " + ((snapshot.clusters || []).length);
    if (sentinelsCount) sentinelsCount.textContent = "Sentinels seen: " + ((snapshot.sentinels || []).length);
    renderSentinels(snapshot.sentinels || []);
    renderClusters(snapshot.clusters || []);
    restoreExpandedDetails();
  }

  function refreshData() {
    fetch(statusPath(), {
        cache: "no-store"
      })
      .then(function (resp) {
        if (!resp.ok) throw new Error("status " + resp.status);
        return resp.json();
      })
      .then(renderSnapshot)
      .catch(function () {});
  }

  function restoreExpandedDetails() {
    if (openDetailKeys.size === 0) return;
    var toggles = document.querySelectorAll(".row-toggle[data-key]");
    for (var i = 0; i < toggles.length; i++) {
      var btn = toggles[i];
      var key = btn.getAttribute("data-key");
      if (!key || !openDetailKeys.has(key)) continue;
      openDetail(btn);
    }
  }

  function openDetail(button) {
    var row = button.closest("tr");
    if (!row || !row.nextElementSibling || !row.nextElementSibling.classList.contains("detail-row")) return;
    var details = row.nextElementSibling;
    var wrap = details.querySelector(".detail-wrap");
    if (!wrap) return;
    var payload = button.getAttribute("data-payload") || "";
    var obj = {};
    try {
      obj = JSON.parse(decodeURIComponent(payload));
    } catch (_) {
      obj = {};
    }
    wrap.innerHTML = renderDetailTable(obj);
    details.removeAttribute("hidden");
    button.setAttribute("aria-expanded", "true");
    button.textContent = "−";
    button.title = "Hide details";
  }

  function closeDetail(button) {
    var row = button.closest("tr");
    if (!row || !row.nextElementSibling || !row.nextElementSibling.classList.contains("detail-row")) return;
    var details = row.nextElementSibling;
    details.setAttribute("hidden", "");
    button.setAttribute("aria-expanded", "false");
    button.textContent = "+";
    button.title = "Show details";
  }

  function onRowToggleClick(event) {
    var target = event.target;
    if (!target || !target.classList || !target.classList.contains("row-toggle")) {
      return;
    }
    var row = target.closest("tr");
    if (!row || !row.nextElementSibling || !row.nextElementSibling.classList.contains("detail-row")) {
      return;
    }
    var details = row.nextElementSibling;
    var key = target.getAttribute("data-key");
    var opened = !details.hasAttribute("hidden");
    if (opened) {
      closeDetail(target);
      if (key) openDetailKeys.delete(key);
      return;
    }
    openDetail(target);
    if (key) openDetailKeys.add(key);
  }

  function setRefresh(value) {
    if (timer) {
      clearInterval(timer);
      timer = null;
    }
    if (value === "off") {
      localStorage.setItem("hysteron.refresh", "off");
      return;
    }
    var seconds = parseInt(value, 10);
    if (!Number.isFinite(seconds) || seconds <= 0) {
      return;
    }
    localStorage.setItem("hysteron.refresh", String(seconds));
    timer = setInterval(refreshData, seconds * 1000);
  }

  function setTheme(theme) {
    document.documentElement.setAttribute("data-theme", theme);
    localStorage.setItem("hysteron.theme", theme);
    if (themeButton) themeButton.textContent = theme === "dark" ? "🌙" : "☀️";
  }

  function preferredTheme() {
    return window.matchMedia &&
      window.matchMedia("(prefers-color-scheme: dark)").matches ?
      "dark" :
      "light";
  }

  var storedRefresh = localStorage.getItem("hysteron.refresh");
  if (refresh) {
    if (storedRefresh === "off") {
      refresh.value = "off";
    } else if (storedRefresh) {
      refresh.value = storedRefresh;
    } else {
      refresh.value = "off";
    }
    setRefresh(refresh.value);
    refresh.addEventListener("change", function () {
      setRefresh(refresh.value);
    });
  }

  var storedTheme = localStorage.getItem("hysteron.theme");
  if (storedTheme === "light" || storedTheme === "dark") {
    setTheme(storedTheme);
  } else {
    setTheme(preferredTheme());
  }
  if (themeButton) {
    themeButton.addEventListener("click", function () {
      var current = document.documentElement.getAttribute("data-theme");
      setTheme(current === "dark" ? "light" : "dark");
    });
  }
  document.addEventListener("click", onRowToggleClick);

  refreshData();
})();
