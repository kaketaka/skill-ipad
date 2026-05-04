# 美股日股模拟交易系统

这是一个模拟交易 MVP：后台抓取免费行情，计算技术指标，生成信号并做纸面交易；iPad 通过 Safari 打开 PWA 看板。系统不会连接券商，也不会进行真实下单。

## 功能

- 美股、日股观察池，日股使用 Yahoo/yfinance 常见的 `.T` 代码。
- 免费数据源顺序：`yfinance`、`stooq`、`alpha_vantage`。Alpha Vantage 需要设置 `ALPHAVANTAGE_API_KEY`。
- 技术指标：SMA、EMA、MACD、RSI、ATR、布林带、20 日突破、量能。
- 模拟撮合：分 USD/JPY 现金账户，日股按 100 股手数，美股按 1 股。
- 风控：单票上限、止损、止盈减仓、手续费估算。
- 每日复盘：统计成交、权益、持仓浮盈亏，并用有边界的权重微调做“自我学习”。
- iPad PWA：移动端看板、信号、持仓、成交、复盘、观察池设置。
- 无电脑模式：GitHub Actions 免费定时跑任务，GitHub Pages 给 iPad 展示静态看板。

## 不开电脑：GitHub Actions + Pages

这是更适合 iPad 的方式。iPadOS 不适合后台定时跑网页任务，所以自动化放在 GitHub Actions，iPad 只打开网页看结果。

1. 把这个文件夹推到一个 GitHub 仓库。
2. 在仓库 Settings -> Actions 允许 workflow 读写仓库内容。
3. 在 Settings -> Pages 选择 `Deploy from a branch`，目录选 `docs`。
4. 如需 Alpha Vantage，在 Settings -> Secrets and variables -> Actions 添加 `ALPHAVANTAGE_API_KEY`。
5. 到 Actions 手动跑一次 `Market Sim Trader`，模式选 `ALL`。
6. iPad 打开 GitHub Pages 地址，并添加到主屏幕。

云端默认计划：

- 日股：周一到周五 16:10 日本时间，东京收盘后。
- 美股：周二到周六 07:15 日本时间，纽约收盘后。
- 复盘：周一到周六 22:30 日本时间。

## 本地启动

```powershell
python -m market_sim.cli init
python app.py
```

默认服务地址：

```text
http://localhost:8765
```

如果 iPad 和这台电脑在同一 Wi-Fi，先查电脑局域网 IP：

```powershell
ipconfig
```

然后在 iPad Safari 打开：

```text
http://电脑局域网IP:8765
```

Safari 打开后可使用“分享”里的“添加到主屏幕”，作为 PWA 使用。

## 手动运行

```powershell
python -m market_sim.cli run --markets JP
python -m market_sim.cli run --markets US
python -m market_sim.cli review
python -m market_sim.cli export --output docs
```

## Windows 定时任务

如果你以后也想让电脑开机时自己跑，可以用管理员 PowerShell 运行：

```powershell
.\scripts\install_windows_tasks.ps1
```

默认时间按日本时间设计：

- 日股：周一到周五 16:10。
- 美股：周二到周六 07:15。
- 复盘：周一到周六 22:30。

## 数据源说明

- `yfinance`：默认源，适合原型和个人研究，日股示例 `7203.T`。
- `stooq`：免费历史行情备选源，美股通常使用 `AAPL.US` 形式，系统会自动尝试映射。
- `alpha_vantage`：免费层可用但有日请求限制，需要 API key。

免费源可能有延迟、限额、覆盖缺口或条款变化。这个系统适合模拟、学习和策略研究，不适合直接作为实盘交易依据。
