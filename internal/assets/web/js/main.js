(function () {
  var refresh = document.getElementById("refresh");
  var themeButton = document.getElementById("theme");
  var timer = null;
  var basePath = document.body.getAttribute("data-base-path") || "";

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

  function boolBadge(value) {
    var cls = value ? "is-true" : "is-false";
    return '<span class="bool-badge ' + cls + '">' + boolText(value) + "</span>";
  }

  function mono(value) {
    return '<span class="mono">' + esc(value) + "</span>";
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

    function list(value) {
      if (!value || value.length === 0) return "-";
      return value.map(esc).join(", ");
    }
    body.innerHTML = rows.map(function (row) {
      return "<tr>" +
        "<td>" + mono(row.uid) + "</td>" +
        "<td>" + boolBadge(row.is_local) + "</td>" +
        "<td>" + list(row.leader_clusters) + "</td>" +
        "<td>" + list(row.clusters) + "</td>" +
        "</tr>";
    }).join("");
  }

  function renderKeepers(rows) {
    if (!rows || rows.length === 0) {
      return '<tr><td colspan="6">no keeper rows</td></tr>';
    }
    return rows.map(function (row) {
      return "<tr>" +
        "<td>" + mono(row.uid) + "</td>" +
        "<td>" + boolBadge(row.healthy) + "</td>" +
        "<td>" + boolBadge(row.pg_healthy) + "</td>" +
        "<td>" + boolBadge(row.can_be_master) + "</td>" +
        "<td>" + boolBadge(row.can_be_synchronous_replica) + "</td>" +
        "<td>" + mono(row.listen_address) + "</td>" +
        "</tr>";
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
        "<td>" + mono(row.uid) + "</td>" +
        "<td>" + mono(row.keeper_uid) + "</td>" +
        "<td>" + esc(row.role) + "</td>" +
        "<td>" + boolBadge(row.healthy) + "</td>" +
        "<td>" + mono(row.xlog_pos) + "</td>" +
        "<td>" + lag + "</td>" +
        "<td>" + mono(row.address) + "</td>" +
        "</tr>";
    }).join("");
  }

  function renderProxies(rows) {
    if (!rows || rows.length === 0) {
      return '<tr><td colspan="5">no proxy rows</td></tr>';
    }
    return rows.map(function (row) {
      return "<tr>" +
        "<td>" + mono(row.uid) + "</td>" +
        "<td>" + boolBadge(row.seen) + "</td>" +
        "<td>" + boolBadge(row.enabled) + "</td>" +
        "<td>" + mono(row.generation) + "</td>" +
        "<td>" + mono(row.proxy_timeout) + "</td>" +
        "</tr>";
    }).join("");
  }

  function renderClusters(rows) {
    var root = document.getElementById("clusters-root");
    if (!root) return;
    root.innerHTML = (rows || []).map(function (cluster) {
      return "<section>" +
        "<h2>Cluster: " + esc(cluster.name) + "</h2>" +
        '<div class="meta">' +
        "<div>Phase: " + esc(cluster.phase) + "</div>" +
        "<div>Generation: " + mono(cluster.generation) + "</div>" +
        "<div>Master DB: " + mono(cluster.master_db_uid) + "</div>" +
        "<div>Master Keeper: " + mono(cluster.master_keeper_uid) + "</div>" +
        "<div>Keepers: " + ratioBadge(cluster.keepers_healthy, cluster.keepers_total, "lower-worse") + "</div>" +
        "<div>DBs: " + ratioBadge(cluster.dbs_healthy, cluster.dbs_total, "lower-worse") + "</div>" +
        "<div>Proxies seen: " + coloredNumber(Number(cluster.proxies_seen), {
          min: 0,
          mid: 1,
          max: 3,
          direction: "lower-worse"
        }) + "</div>" +
        "<div>Error: " + esc(cluster.error || "") + "</div>" +
        "</div>" +
        "<h3>Keepers</h3>" +
        "<table><thead><tr>" +
        "<th>Keeper UID</th><th>Healthy</th><th>PG Healthy</th><th>CanBeMaster</th><th>CanBeSyncReplica</th><th>Address</th>" +
        "</tr></thead><tbody>" + renderKeepers(cluster.keeper_rows) + "</tbody></table>" +
        "<h3>Databases</h3>" +
        "<table><thead><tr>" +
        "<th>DB UID</th><th>Keeper UID</th><th>Role</th><th>Healthy</th><th>XLogPos</th><th>LagBytes</th><th>Address</th>" +
        "</tr></thead><tbody>" + renderDBs(cluster.db_rows) + "</tbody></table>" +
        "<h3>Proxies</h3>" +
        "<table><thead><tr>" +
        "<th>Proxy UID</th><th>Seen</th><th>Enabled</th><th>Generation</th><th>ProxyTimeout</th>" +
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

  refreshData();
})();
