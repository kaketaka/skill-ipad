const state = {
  dashboard: null,
  activeTab: "dashboard",
  staticMode: false,
};

const money = new Intl.NumberFormat("zh-CN", {
  maximumFractionDigits: 2,
});

document.addEventListener("DOMContentLoaded", () => {
  bindTabs();
  bindActions();
  loadStatus();
  if ("serviceWorker" in navigator) {
    navigator.serviceWorker.register("service-worker.js").catch(() => {});
  }
});

function bindTabs() {
  document.querySelectorAll(".tab").forEach((button) => {
    button.addEventListener("click", () => {
      state.activeTab = button.dataset.tab;
      document.querySelectorAll(".tab").forEach((item) => item.classList.toggle("active", item === button));
      document.querySelectorAll(".view").forEach((view) => view.classList.toggle("active", view.id === state.activeTab));
    });
  });
}

function bindActions() {
  document.getElementById("run-us").addEventListener("click", () => runCycle(["US"]));
  document.getElementById("run-jp").addEventListener("click", () => runCycle(["JP"]));
  document.getElementById("run-all").addEventListener("click", () => runCycle(["US", "JP"]));
  document.getElementById("review").addEventListener("click", createReview);
  document.getElementById("review-inline").addEventListener("click", createReview);
  document.getElementById("save-settings").addEventListener("click", saveSettings);
}

async function loadStatus() {
  setStatus("正在读取系统状态...");
  const data = await loadDashboardData();
  state.dashboard = data;
  render(data);
  setStatus(
    state.staticMode
      ? "云端静态模式：自动任务在 GitHub Actions 跑，iPad 只负责查看。"
      : "系统就绪。全部交易都是模拟盘，不会触发真实下单。",
  );
}

async function runCycle(markets) {
  if (state.staticMode) {
    setStatus("云端静态模式不能从 iPad 直接手动跑任务；可在 GitHub Actions 里手动触发。");
    return;
  }
  setStatus(`正在分析 ${markets.join(" + ")}，这一步会联网拉取免费行情...`);
  const result = await requestJson("/api/run", {
    method: "POST",
    body: JSON.stringify({ markets }),
  });
  const errorText = result.errors?.length ? `，${result.errors.length} 个代码拉取失败` : "";
  setStatus(`分析完成：生成 ${result.signals.length} 个信号，模拟成交 ${result.trades.length} 笔${errorText}。`);
  await loadStatus();
}

async function createReview() {
  if (state.staticMode) {
    setStatus("云端静态模式会按计划自动复盘；手动触发请用 GitHub Actions。");
    return;
  }
  setStatus("正在生成今日复盘并微调策略权重...");
  const result = await requestJson("/api/review", { method: "POST" });
  setStatus(result.summary || "复盘完成。");
  await loadStatus();
}

async function saveSettings() {
  if (state.staticMode) {
    setStatus("云端静态模式下请改仓库里的配置或观察池，再让 Actions 重新导出看板。");
    return;
  }
  const payload = {
    watchlists: {
      US: lines("watch-us"),
      JP: lines("watch-jp"),
    },
    data_sources: document
      .getElementById("sources")
      .value.split(",")
      .map((item) => item.trim())
      .filter(Boolean),
  };
  await requestJson("/api/settings", {
    method: "POST",
    body: JSON.stringify(payload),
  });
  setStatus("设置已保存。");
  await loadStatus();
}

async function requestJson(url, options = {}) {
  const response = await fetch(url, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || response.statusText);
  }
  return response.json();
}

async function loadDashboardData() {
  try {
    state.staticMode = false;
    return await requestJson("api/status");
  } catch (error) {
    state.staticMode = true;
    const response = await fetch(`state.json?ts=${Date.now()}`);
    if (!response.ok) {
      throw error;
    }
    return response.json();
  }
}

function render(data) {
  renderEquity(data.portfolio.equity);
  renderLatestPrices(data.latest_prices);
  renderSignals(data.signals);
  renderPositions(data.portfolio.positions);
  renderTrades(data.trades);
  renderReviews(data.reviews);
  renderSettings(data.settings);
}

function renderEquity(rows) {
  const grid = document.getElementById("equity-grid");
  grid.innerHTML = rows
    .map(
      (row) => `
      <article class="metric">
        <span>${row.currency} 模拟权益</span>
        <strong>${money.format(row.equity)}</strong>
        <small>现金 ${money.format(row.cash)} / 持仓 ${money.format(row.positions_value)}</small>
      </article>
    `,
    )
    .join("");
}

function renderLatestPrices(rows) {
  document.getElementById("latest-prices").innerHTML = rows.length
    ? rows
        .map(
          (row) => `
        <tr>
          <td>${escapeHtml(row.symbol)}</td>
          <td>${row.market}</td>
          <td>${row.date}</td>
          <td>${money.format(row.close)}</td>
          <td>${row.source}</td>
        </tr>
      `,
        )
        .join("")
    : `<tr><td colspan="5">暂无行情，点“立即分析”开始。</td></tr>`;
}

function renderSignals(rows) {
  document.getElementById("signal-list").innerHTML = rows.length
    ? rows
        .map((row) => {
          const rationale = parseJson(row.rationale, []).join(" / ");
          return `
          <article class="item">
            <div class="item-head">
              <h3>${escapeHtml(row.symbol)} · ${row.market}</h3>
              <span class="badge ${row.action}">${row.action} ${Number(row.score).toFixed(2)}</span>
            </div>
            <p>${row.ts} · 收盘 ${money.format(row.close)} · 置信度 ${Math.round(row.confidence * 100)}%</p>
            <p>${escapeHtml(rationale)}</p>
          </article>
        `;
        })
        .join("")
    : `<article class="item"><p>暂无信号。</p></article>`;
}

function renderPositions(rows) {
  document.getElementById("position-rows").innerHTML = rows.length
    ? rows
        .map((row) => {
          const pnl = Number(row.unrealized_pnl || 0);
          return `
          <tr>
            <td>${escapeHtml(row.symbol)}</td>
            <td>${money.format(row.quantity)}</td>
            <td>${money.format(row.avg_cost)}</td>
            <td>${money.format(row.last_price)}</td>
            <td class="${pnl >= 0 ? "positive" : "negative"}">${money.format(pnl)}</td>
          </tr>
        `;
        })
        .join("")
    : `<tr><td colspan="5">暂无持仓。</td></tr>`;
}

function renderTrades(rows) {
  document.getElementById("trade-rows").innerHTML = rows.length
    ? rows
        .map(
          (row) => `
        <tr>
          <td>${shortTime(row.ts)}</td>
          <td>${escapeHtml(row.symbol)}</td>
          <td>${row.side}</td>
          <td>${money.format(row.quantity)}</td>
          <td>${money.format(row.price)}</td>
          <td>${escapeHtml(row.reason)}</td>
        </tr>
      `,
        )
        .join("")
    : `<tr><td colspan="6">暂无成交。</td></tr>`;
}

function renderReviews(rows) {
  document.getElementById("review-list").innerHTML = rows.length
    ? rows
        .map(
          (row) => `
        <article class="item">
          <div class="item-head">
            <h3>${row.review_date}</h3>
            <span class="badge HOLD">复盘</span>
          </div>
          <p>${escapeHtml(row.summary)}</p>
        </article>
      `,
        )
        .join("")
    : `<article class="item"><p>暂无复盘。</p></article>`;
}

function renderSettings(settings) {
  document.getElementById("watch-us").value = settings.watchlists.US.join("\n");
  document.getElementById("watch-jp").value = settings.watchlists.JP.join("\n");
  document.getElementById("sources").value = settings.data_sources.join(", ");
}

function setStatus(text) {
  document.querySelector("#status-band .status-text").textContent = text;
}

function lines(id) {
  return document
    .getElementById(id)
    .value.split(/\n|,/)
    .map((item) => item.trim())
    .filter(Boolean);
}

function shortTime(value) {
  return String(value || "").replace("T", " ").slice(0, 16);
}

function parseJson(value, fallback) {
  try {
    return JSON.parse(value);
  } catch {
    return fallback;
  }
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}
