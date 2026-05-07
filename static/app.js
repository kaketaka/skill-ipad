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
  document.getElementById("run-all-observe").addEventListener("click", () => runCycle(["US", "JP"]));
  document.getElementById("review").addEventListener("click", createReview);
  document.getElementById("review-inline").addEventListener("click", createReview);
  document.getElementById("save-settings").addEventListener("click", saveSettings);
  document.getElementById("sync-universe").addEventListener("click", syncUniverse);
}

async function loadStatus() {
  setStatus("正在读取系统状态...");
  const data = await loadDashboardData();
  state.dashboard = data;
  render(data);
  setStatus(
    state.staticMode
      ? "云端静态模式：自动任务在 GitHub Actions 跑，iPad 负责查看。"
      : "系统就绪。观察池必扫，全市场股票池按日轮换扫描。",
  );
}

async function runCycle(markets) {
  if (state.staticMode) {
    setStatus("云端静态模式不能从 iPad 直接手动跑任务；可在 GitHub Actions 里手动触发。");
    return;
  }
  setStatus(`正在分析 ${markets.join(" + ")}：观察池必扫，并从全市场池轮换扫描...`);
  const result = await requestJson("api/run", {
    method: "POST",
    body: JSON.stringify({ markets }),
  });
  const errorText = result.errors?.length ? `，${result.errors.length} 个代码拉取失败` : "";
  const scanText = result.scan_plan
    ? Object.entries(result.scan_plan)
        .map(([market, count]) => `${market} ${count}`)
        .join(" / ")
    : `${result.signals.length}`;
  setStatus(`分析完成：扫描 ${scanText}，生成 ${result.signals.length} 个信号，模拟成交 ${result.trades.length} 笔${errorText}。`);
  await loadStatus();
}

async function createReview() {
  if (state.staticMode) {
    setStatus("云端静态模式会按计划自动复盘；手动触发请用 GitHub Actions。");
    return;
  }
  setStatus("正在生成今日复盘并微调策略权重...");
  const result = await requestJson("api/review", { method: "POST" });
  setStatus(result.summary || "复盘完成。");
  await loadStatus();
}

async function syncUniverse() {
  if (state.staticMode) {
    setStatus("云端静态模式会由 GitHub Actions 自动同步全市场股票池。");
    return;
  }
  setStatus("正在同步美股和日股全市场股票池...");
  const result = await requestJson("api/sync-universe", {
    method: "POST",
    body: JSON.stringify({ markets: ["US", "JP"] }),
  });
  setStatus(`同步完成：US ${result.US?.count || 0} 只，JP ${result.JP?.count || 0} 只。`);
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
    universe: {
      daily_scan_limit: {
        US: Number(document.getElementById("scan-us").value || 0),
        JP: Number(document.getElementById("scan-jp").value || 0),
      },
    },
    strategy: {
      recommend_buy_score: Number(document.getElementById("buy-score").value || 70),
      recommend_sell_score: Number(document.getElementById("sell-score").value || 30),
    },
  };
  await requestJson("api/settings", {
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
  renderEquity(data.portfolio.equity, data.portfolio_summary || []);
  renderUniverse(data.universe || {});
  renderGuidance(data.guidance || {});
  renderObservationSignals(data.observation_signals || []);
  renderMarketQuotes(data.market_quotes || {});
  renderTradedQuotes(data.traded_quotes || []);
  renderSignals(data.signals);
  renderPositions(data.portfolio.positions);
  renderPositionDetails(data.positions_enriched || []);
  renderTrades(data.trades);
  renderReviews(data.reviews);
  renderSettings(data.settings);
}

function renderEquity(rows, summaryRows = []) {
  const grid = document.getElementById("equity-grid");
  const summaryByCurrency = Object.fromEntries(summaryRows.map((row) => [row.currency, row]));
  grid.innerHTML = rows
    .map((row) => {
      const summary = summaryByCurrency[row.currency] || {};
      const delta = Number(summary.equity_delta);
      const deltaPct = Number(summary.equity_delta_pct);
      const deltaClass = Number.isFinite(delta) ? (delta >= 0 ? "positive" : "negative") : "";
      return `
      <article class="metric">
        <span>${row.currency} 模拟权益</span>
        <strong>${money.format(row.equity)}</strong>
        <small>现金 ${money.format(row.cash)} / 持仓 ${money.format(row.positions_value)}</small>
        <small class="${deltaClass}">
          较初始 ${Number.isFinite(delta) ? money.format(delta) : "-"}
          ${Number.isFinite(deltaPct) ? `(${formatPct(deltaPct)})` : ""}
          · 累计手续费 ${money.format(summary.total_fees || 0)}
        </small>
      </article>
    `;
    })
    .join("");
}

function renderUniverse(universe) {
  const grid = document.getElementById("universe-grid");
  grid.innerHTML = ["US", "JP"]
    .map((market) => {
      const row = universe[market] || {};
      return `
        <article class="metric">
          <span>${market} 全市场股票池</span>
          <strong>${money.format(row.count || 0)}</strong>
          <small>${escapeHtml(row.source || "尚未同步")}</small>
        </article>
      `;
    })
    .join("");
}

function renderGuidance(guidance) {
  document.getElementById("guidance").innerHTML = `
    <span>推荐指数 0-100</span>
    <strong>${guidance.buy || 70}+ 适合买入；${guidance.sell || 30}- 适合卖出；中间继续观察。</strong>
  `;
}

function renderObservationSignals(rows) {
  document.getElementById("observe-list").innerHTML = rows.length
    ? rows.map(renderSignalCard).join("")
    : `<article class="item"><p>观察池还没有评分，点“刷新评分”开始。</p></article>`;
}

function renderMarketQuotes(marketQuotes) {
  const rows = []
    .concat(marketQuotes.US || [])
    .concat(marketQuotes.JP || [])
    .filter(Boolean);
  document.getElementById("market-quotes").innerHTML = rows.length
    ? rows
        .map((row) => {
          const change = Number(row.change);
          const changePct = Number(row.change_pct);
          const cls = Number.isFinite(change) ? (change >= 0 ? "positive" : "negative") : "";
          return `
            <tr>
              <td>${escapeHtml(row.symbol)}</td>
              <td>${escapeHtml(row.market)}</td>
              <td>${escapeHtml(row.date)}</td>
              <td>${money.format(row.close)}</td>
              <td class="${cls}">${Number.isFinite(change) ? money.format(change) : "-"}</td>
              <td class="${cls}">${Number.isFinite(changePct) ? formatPct(changePct) : "-"}</td>
            </tr>
          `;
        })
        .join("")
    : `<tr><td colspan="6">暂无当日行情（需要先跑一次分析以更新价格库）。</td></tr>`;
}

function renderTradedQuotes(rows) {
  document.getElementById("traded-quotes").innerHTML = rows.length
    ? rows
        .map((row) => {
          const changePct = Number(row.change_pct);
          const cls = Number.isFinite(changePct) ? (changePct >= 0 ? "positive" : "negative") : "";
          const candles = Array.isArray(row.candles) ? row.candles.slice(-40) : [];
          return `
            <tr>
              <td>${escapeHtml(row.symbol)}</td>
              <td>${escapeHtml(row.market)}</td>
              <td>${escapeHtml(row.date)}</td>
              <td>${money.format(row.close)}</td>
              <td class="${cls}">${Number.isFinite(changePct) ? formatPct(changePct) : "-"}</td>
              <td>${candles.length ? renderCandleSvg(candles) : "<span class=\"muted\">暂无K线</span>"}</td>
            </tr>
          `;
        })
        .join("")
    : `<tr><td colspan="6">近 30 天无成交且当前无持仓，因此不展示成交行情。</td></tr>`;
}

function renderSignals(rows) {
  document.getElementById("signal-list").innerHTML = rows.length
    ? rows.map(renderSignalCard).join("")
    : `<article class="item"><p>暂无信号。</p></article>`;
}

function renderSignalCard(row) {
  const rationale = parseJson(row.rationale, []).join(" / ");
  const index = Number(row.recommendation_index ?? Math.round((Number(row.score || 0) + 1) * 50));
  return `
    <article class="item">
      <div class="item-head">
        <h3>${escapeHtml(row.symbol)} · ${row.market}</h3>
        <span class="badge ${row.action}">${escapeHtml(row.recommendation_label || row.action)} ${index}</span>
      </div>
      <div class="score-line">
        <div class="score-bar"><span style="width:${Math.max(0, Math.min(100, index))}%"></span></div>
        <b>${row.action} · 技术分 ${Number(row.score).toFixed(2)}</b>
      </div>
      <p>${shortTime(row.ts)} · 收盘 ${money.format(row.close)} · 置信度 ${Math.round(row.confidence * 100)}%</p>
      <p>${escapeHtml(rationale)}</p>
    </article>
  `;
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

function renderPositionDetails(rows) {
  const container = document.getElementById("position-details");
  container.innerHTML = rows.length
    ? rows
        .map((row) => {
          const pnl = Number(row.unrealized_pnl || 0);
          const pnlPct = Number(row.unrealized_pct || 0);
          const dayPct = Number(row.day_change_pct);
          const dayCls = Number.isFinite(dayPct) ? (dayPct >= 0 ? "positive" : "negative") : "";
          const candles = Array.isArray(row.candles) ? row.candles.slice(-60) : [];
          const ind = row.indicators || {};
          return `
            <article class="item">
              <div class="item-head">
                <h3>${escapeHtml(row.symbol)} · ${escapeHtml(row.market)} · 持仓</h3>
                <span class="badge ${pnl >= 0 ? "BUY" : "SELL"}">${pnl >= 0 ? "浮盈" : "浮亏"} ${money.format(pnl)} (${formatPct(pnlPct)})</span>
              </div>
              <p>
                数量 ${money.format(row.quantity)} · 成本 ${money.format(row.avg_cost)} · 现价 ${money.format(row.last_price)}
                · 当日 <span class="${dayCls}">${Number.isFinite(dayPct) ? formatPct(dayPct) : "-"}</span>
                · 市值 ${money.format(row.market_value)} · 成本额 ${money.format(row.cost_value)}
              </p>
              <div class="chart-row">${candles.length ? renderCandleSvg(candles) : "<span class=\"muted\">暂无K线</span>"}</div>
              <div class="kv">
                <div><b>趋势</b><span>SMA20 ${fmt(ind.sma20)} / SMA50 ${fmt(ind.sma50)} / slope20 ${fmt(ind.slope20)}</span></div>
                <div><b>动量</b><span>RSI14 ${fmt(ind.rsi14)} / MACD_hist ${fmt(ind.macd_hist)} / return1d ${fmt(ind.return_1d, true)}</span></div>
                <div><b>波动</b><span>ATR14 ${fmt(ind.atr14)} / 布林 ${fmt(ind.bb_lower)} - ${fmt(ind.bb_upper)}</span></div>
                <div><b>资金流</b><span>MFI14 ${fmt(ind.mfi14)} / CMF20 ${fmt(ind.cmf20)} / OBV ${fmt(ind.obv)}</span></div>
              </div>
            </article>
          `;
        })
        .join("")
    : `<article class="item"><p>暂无持仓，持仓详情不展示。</p></article>`;
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
          <p class="multiline">${escapeHtml(row.summary).replaceAll("\n", "<br>")}</p>
        </article>
      `,
        )
        .join("")
    : `<article class="item"><p>暂无复盘。</p></article>`;
}

function formatPct(value) {
  const pct = Number(value) * 100;
  if (!Number.isFinite(pct)) return "-";
  const sign = pct > 0 ? "+" : "";
  return `${sign}${pct.toFixed(2)}%`;
}

function fmt(value, pct = false) {
  if (!Number.isFinite(Number(value))) return "-";
  if (pct) return formatPct(value);
  return money.format(value);
}

function renderCandleSvg(candles) {
  const width = 240;
  const height = 84;
  const padX = 4;
  const padY = 6;
  const usableW = width - padX * 2;
  const usableH = height - padY * 2;
  const highs = candles.map((c) => Number(c.high ?? c.close)).filter(Number.isFinite);
  const lows = candles.map((c) => Number(c.low ?? c.close)).filter(Number.isFinite);
  const max = Math.max(...highs);
  const min = Math.min(...lows);
  const range = max - min || 1;
  const step = usableW / Math.max(1, candles.length);
  const bodyW = Math.max(1, step * 0.6);

  const items = candles
    .map((c, idx) => {
      const open = Number(c.open ?? c.close);
      const close = Number(c.close);
      const high = Number(c.high ?? Math.max(open, close));
      const low = Number(c.low ?? Math.min(open, close));
      if (![open, close, high, low].every(Number.isFinite)) return "";
      const x = padX + idx * step + step / 2;
      const yHigh = padY + ((max - high) / range) * usableH;
      const yLow = padY + ((max - low) / range) * usableH;
      const yOpen = padY + ((max - open) / range) * usableH;
      const yClose = padY + ((max - close) / range) * usableH;
      const up = close >= open;
      const yTop = Math.min(yOpen, yClose);
      const yBot = Math.max(yOpen, yClose);
      const cls = up ? "candle-up" : "candle-down";
      return `
        <line x1="${x}" y1="${yHigh}" x2="${x}" y2="${yLow}" class="wick ${cls}" />
        <rect x="${x - bodyW / 2}" y="${yTop}" width="${bodyW}" height="${Math.max(1, yBot - yTop)}" class="body ${cls}" />
      `;
    })
    .join("");
  return `
    <svg class="candle" viewBox="0 0 ${width} ${height}" width="${width}" height="${height}" aria-hidden="true">
      <rect x="0" y="0" width="${width}" height="${height}" rx="10" class="candle-bg"></rect>
      ${items}
    </svg>
  `;
}

function renderSettings(settings) {
  document.getElementById("watch-us").value = settings.watchlists.US.join("\n");
  document.getElementById("watch-jp").value = settings.watchlists.JP.join("\n");
  document.getElementById("sources").value = settings.data_sources.join(", ");
  document.getElementById("scan-us").value = settings.universe?.daily_scan_limit?.US ?? 40;
  document.getElementById("scan-jp").value = settings.universe?.daily_scan_limit?.JP ?? 40;
  document.getElementById("buy-score").value = settings.strategy?.recommend_buy_score ?? 70;
  document.getElementById("sell-score").value = settings.strategy?.recommend_sell_score ?? 30;
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
