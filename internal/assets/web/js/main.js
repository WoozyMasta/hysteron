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
    body.innerHTML = rows.map(function (row) {
      return "<tr>" +
        "<td>" + esc(row.cluster_name) + "</td>" +
        "<td>" + esc(row.uid) + "</td>" +
        "<td>" + boolText(row.is_local) + "</td>" +
        "<td>" + boolText(row.is_leader) + "</td>" +
      "</tr>";
    }).join("");
  }

  function renderKeepers(rows) {
    if (!rows || rows.length === 0) {
      return '<tr><td colspan="6">no keeper rows</td></tr>';
    }
    return rows.map(function (row) {
      return "<tr>" +
        "<td>" + esc(row.uid) + "</td>" +
        "<td>" + boolText(row.healthy) + "</td>" +
        "<td>" + boolText(row.pg_healthy) + "</td>" +
        "<td>" + boolText(row.can_be_master) + "</td>" +
        "<td>" + boolText(row.can_be_synchronous_replica) + "</td>" +
        "<td>" + esc(row.listen_address) + "</td>" +
      "</tr>";
    }).join("");
  }

  function renderDBs(rows) {
    if (!rows || rows.length === 0) {
      return '<tr><td colspan="7">no database rows</td></tr>';
    }
    return rows.map(function (row) {
      return "<tr>" +
        "<td>" + esc(row.uid) + "</td>" +
        "<td>" + esc(row.keeper_uid) + "</td>" +
        "<td>" + esc(row.role) + "</td>" +
        "<td>" + boolText(row.healthy) + "</td>" +
        "<td>" + esc(row.xlog_pos) + "</td>" +
        "<td>" + esc(row.lag_bytes) + "</td>" +
        "<td>" + esc(row.address) + "</td>" +
      "</tr>";
    }).join("");
  }

  function renderProxies(rows) {
    if (!rows || rows.length === 0) {
      return '<tr><td colspan="5">no proxy rows</td></tr>';
    }
    return rows.map(function (row) {
      return "<tr>" +
        "<td>" + esc(row.uid) + "</td>" +
        "<td>" + boolText(row.seen) + "</td>" +
        "<td>" + boolText(row.enabled) + "</td>" +
        "<td>" + esc(row.generation) + "</td>" +
        "<td>" + esc(row.proxy_timeout) + "</td>" +
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
          "<div>Generation: " + esc(cluster.generation) + "</div>" +
          "<div>Master DB: " + esc(cluster.master_db_uid) + "</div>" +
          "<div>Master Keeper: " + esc(cluster.master_keeper_uid) + "</div>" +
          "<div>Keepers: " + esc(cluster.keepers_healthy) + "/" + esc(cluster.keepers_total) + "</div>" +
          "<div>DBs: " + esc(cluster.dbs_healthy) + "/" + esc(cluster.dbs_total) + "</div>" +
          "<div>Proxies seen: " + esc(cluster.proxies_seen) + "</div>" +
          "<div>Error: " + esc(cluster.error || "") + "</div>" +
        "</div>" +
        "<h3>Keepers</h3>" +
        "<table><thead><tr>" +
        "<th>Keeper UID</th><th>Healthy</th><th>PG Healthy</th><th>CanBeMaster</th><th>CanBeSyncReplica</th><th>Address</th>" +
        "</tr></thead><tbody>" + renderKeepers(cluster.keeper_rows) + "</tbody></table>" +
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
    fetch(statusPath(), { cache: "no-store" })
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
      window.matchMedia("(prefers-color-scheme: dark)").matches
      ? "dark"
      : "light";
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
