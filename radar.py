#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
比特币情报雷达 (BTC Radar) v1.0
================================
从多个免费公开数据源抓取"可能影响比特币价格"的重大消息,
经过关键词打分 → 过滤 → 限流后, 通过 PushPlus 推送到微信。

运行方式
--------
    python radar.py             正常运行(GitHub Actions 定时任务用这个)
    python radar.py --test      只发一条测试推送, 用来验证 PushPlus 配置是否通
    python radar.py --dry-run   只在终端打印结果, 不推送(本地调试用)

需要设置的环境变量
------------------
    PUSHPLUS_TOKEN       必填。你的 PushPlus 令牌
    ANTHROPIC_API_KEY    选填。只有开启 config.yaml 里的 llm_filter 才需要

数据来源全部是公开、免费、无需注册的接口。
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path

import requests
import yaml

# --------------------------------------------------------------------------
# 常量
# --------------------------------------------------------------------------

ROOT = Path(__file__).resolve().parent
CONFIG_PATH = ROOT / "config.yaml"
STATE_PATH = ROOT / "state.json"

CST = timezone(timedelta(hours=8))          # 北京时间
UTC = timezone.utc

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "*/*",
}

PUSHPLUS_API = "https://www.pushplus.plus/send"

NS_ATOM = "{http://www.w3.org/2005/Atom}"
NS_SM = "{http://www.sitemaps.org/schemas/sitemap/0.9}"
NS_NEWS = "{http://www.google.com/schemas/sitemap-news/0.9}"


def log(*args) -> None:
    """带时间戳的日志输出, 方便在 Actions 里排查问题。"""
    print(datetime.now(CST).strftime("[%m-%d %H:%M:%S]"), *args, flush=True)


# --------------------------------------------------------------------------
# 基础工具
# --------------------------------------------------------------------------

def http_get(url: str, timeout: int = 15, **kw) -> requests.Response:
    resp = requests.get(url, headers=HEADERS, timeout=timeout, **kw)
    resp.raise_for_status()
    return resp


def get_json(url: str, **kw):
    return http_get(url, **kw).json()


def strip_html(raw: str) -> str:
    """去掉 HTML 标签和实体, 只留纯文本。"""
    if not raw:
        return ""
    text = re.sub(r"<[^>]+>", " ", raw)
    for src, dst in (
        ("&nbsp;", " "), ("&amp;", "&"), ("&quot;", '"'),
        ("&#39;", "'"), ("&apos;", "'"), ("&lt;", "<"), ("&gt;", ">"),
        ("&#8217;", "'"), ("&#8220;", '"'), ("&#8221;", '"'),
    ):
        text = text.replace(src, dst)
    return re.sub(r"\s+", " ", text).strip()


def parse_date(raw: str):
    """兼容 RFC822(RSS) 和 ISO8601(Atom/JSON) 两种时间格式。"""
    raw = (raw or "").strip()
    if not raw:
        return None
    try:
        dt = parsedate_to_datetime(raw)
        if dt is not None:
            return dt if dt.tzinfo else dt.replace(tzinfo=UTC)
    except Exception:
        pass
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        # 必须保证返回的是"带时区"的时间, 否则后面做减法会直接崩
        return dt if dt.tzinfo else dt.replace(tzinfo=UTC)
    except Exception:
        return None


def fmt_time(dt) -> str:
    if dt is None:
        return "时间未知"
    return dt.astimezone(CST).strftime("%m-%d %H:%M")


def make_item(source: str, title: str, url: str, ts, summary: str = "",
              weight: int = 0, **extra) -> dict:
    """统一的条目结构。"""
    item = {
        "source": source,
        "title": strip_html(title),
        "url": url or "",
        "ts": ts,
        "summary": strip_html(summary)[:400],
        "weight": weight,
        "score": 0,
        "reasons": [],
    }
    item.update(extra)
    return item


def item_key(item: dict) -> str:
    """去重用的指纹: 优先用链接, 没有链接就用标题。"""
    basis = item.get("url") or item.get("title", "")
    return hashlib.sha1(f"{item['source']}|{basis}".encode("utf-8")).hexdigest()[:16]


# --------------------------------------------------------------------------
# 状态管理(去重 / 限流 / 预警记录)
# --------------------------------------------------------------------------

def load_state() -> dict:
    if STATE_PATH.exists():
        try:
            return json.loads(STATE_PATH.read_text(encoding="utf-8"))
        except Exception as exc:
            log(f"[!] state.json 读取失败, 将重建: {exc}")
    return {}


def save_state(state: dict) -> None:
    # 清理 10 天前的去重记录, 避免文件无限膨胀
    cutoff = (datetime.now(UTC) - timedelta(days=10)).isoformat()
    seen = state.get("seen", {})
    state["seen"] = {k: v for k, v in seen.items() if v >= cutoff}
    STATE_PATH.write_text(
        json.dumps(state, ensure_ascii=False, indent=1), encoding="utf-8"
    )


def bump_daily_counter(state: dict) -> int:
    """返回今天已推送的条数, 并在日期变化时自动归零。"""
    today = datetime.now(CST).strftime("%Y-%m-%d")
    counter = state.get("push_count", {})
    if counter.get("date") != today:
        counter = {"date": today, "n": 0}
    state["push_count"] = counter
    return counter["n"]


# --------------------------------------------------------------------------
# 数据源
# --------------------------------------------------------------------------

def parse_feed(content: bytes, source: str, weight: int, limit: int = 50) -> list[dict]:
    """通用 XML 解析器, 同时支持 RSS 2.0 / Atom / Google News Sitemap。"""
    items: list[dict] = []
    try:
        root = ET.fromstring(content)
    except ET.ParseError as exc:
        log(f"  [!] {source}: XML 解析失败 ({exc})")
        return items

    # --- RSS 2.0 ---
    for node in list(root.iter("item"))[:limit]:
        title = node.findtext("title") or ""
        link = (node.findtext("link") or "").strip()
        pub = (node.findtext("pubDate")
               or node.findtext("{http://purl.org/dc/elements/1.1/}date") or "")
        desc = node.findtext("description") or ""
        if title.strip():
            items.append(make_item(source, title, link, parse_date(pub),
                                   desc, weight))

    # --- Atom (Blockworks 等用这个格式) ---
    for node in list(root.iter(f"{NS_ATOM}entry"))[:limit]:
        title = node.findtext(f"{NS_ATOM}title") or ""
        link = ""
        for ln in node.findall(f"{NS_ATOM}link"):
            if ln.get("rel") in (None, "alternate"):
                link = ln.get("href") or ""
                break
        pub = (node.findtext(f"{NS_ATOM}updated")
               or node.findtext(f"{NS_ATOM}published") or "")
        desc = (node.findtext(f"{NS_ATOM}summary")
                or node.findtext(f"{NS_ATOM}content") or "")
        if title.strip():
            items.append(make_item(source, title, link, parse_date(pub),
                                   desc, weight))

    # --- Google News Sitemap (路透社用这个, 更新最快) ---
    for node in list(root.iter(f"{NS_SM}url"))[:limit]:
        loc = (node.findtext(f"{NS_SM}loc") or "").strip()
        title = node.findtext(f"{NS_NEWS}news/{NS_NEWS}title") or ""
        pub = node.findtext(f"{NS_NEWS}news/{NS_NEWS}publication_date") or ""
        if title.strip():
            items.append(make_item(source, title, loc, parse_date(pub), "", weight))

    return items


def fetch_feeds(cfg: dict) -> list[dict]:
    """抓取所有配置好的 RSS / Sitemap 源。单个源失败不影响其他源。"""
    out: list[dict] = []
    for feed in cfg["sources"].get("feeds", []):
        if not feed.get("enabled", True):
            continue
        name = feed["name"]
        try:
            resp = http_get(feed["url"], timeout=15)
            got = parse_feed(resp.content, name, feed.get("weight", 1))
            log(f"  · {name}: {len(got)} 条")
            out.extend(got)
        except Exception as exc:
            log(f"  [!] {name} 抓取失败: {type(exc).__name__} {exc}")
    return out


def fetch_trump(cfg: dict) -> list[dict]:
    """特朗普 Truth Social 动态(经第三方镜像)。"""
    conf = cfg["sources"].get("trump", {})
    if not conf.get("enabled"):
        return []
    try:
        resp = http_get(conf["url"], timeout=15)
        got = parse_feed(resp.content, "特朗普", conf.get("weight", 6), limit=30)
        # 标记来源, 打分时会做特殊处理(见 score_item)
        for item in got:
            item["kind"] = "trump"
        log(f"  · 特朗普动态: {len(got)} 条")
        return got
    except Exception as exc:
        log(f"  [!] 特朗普动态抓取失败: {type(exc).__name__} {exc}")
        return []


def fetch_binance_announcements(cfg: dict) -> list[dict]:
    """
    币安公告。只取"新币上线"和"下架"两个栏目 —— 这两类公告对价格冲击最直接,
    其他栏目(系统维护/API更新/空投)基本都是噪音, 直接不要。

    注意: pageSize 最大只能填 20, 填 30 会返回 400。
    """
    conf = cfg["sources"].get("binance_announcements", {})
    if not conf.get("enabled"):
        return []
    wanted = set(conf.get("catalog_ids", [48, 161]))
    try:
        data = get_json(conf["url"], timeout=15)
        catalogs = (data.get("data") or {}).get("catalogs") or []
        out = []
        for cat in catalogs:
            if cat.get("catalogId") not in wanted:
                continue
            for art in cat.get("articles") or []:
                ts = None
                if art.get("releaseDate"):
                    ts = datetime.fromtimestamp(art["releaseDate"] / 1000, tz=UTC)
                out.append(make_item(
                    "币安公告", art.get("title", ""),
                    "https://www.binance.com/zh-CN/support/announcement/"
                    f"{art.get('code','')}",
                    ts, "", conf.get("weight", 6), kind="announcement",
                ))
        log(f"  · 币安公告: {len(out)} 条(仅上线/下架栏目)")
        return out
    except Exception as exc:
        log(f"  [!] 币安公告抓取失败: {type(exc).__name__} {exc}")
        return []


def _parse_ff_xml_time(date_s: str, time_s: str):
    """
    解析财经日历 XML 里的 date="09-16-2026" + time="6:00pm"。

    ！！重要！！经实测比对: XML 里的 time 是 UTC, 不是美东时间。
    同一条 FOMC 事件, JSON 接口给的是 2026-09-16T14:00:00-04:00(美东下午2点,
    真实值), XML 里写的是 6:00pm —— 正好差 4 小时。所以这里按 UTC 处理。
    """
    try:
        month, day, year = (int(x) for x in date_s.split("-"))
    except Exception:
        return None

    time_s = (time_s or "").strip().lower()
    if not time_s:
        return datetime(year, month, day, tzinfo=UTC)      # 全天事件

    hit = re.match(r"(\d{1,2}):(\d{2})\s*(am|pm)", time_s)
    if not hit:
        return None
    hour, minute, ampm = int(hit.group(1)), int(hit.group(2)), hit.group(3)
    if ampm == "pm" and hour != 12:
        hour += 12
    elif ampm == "am" and hour == 12:
        hour = 0
    return datetime(year, month, day, hour, minute, tzinfo=UTC)


def _load_macro_events(conf: dict, state: dict) -> list[dict]:
    """
    取财经日历, 返回统一格式的事件列表:
        {"title","country","impact","when"(UTC),"forecast","previous","actual"}

    这个接口限流很严(同一 IP 连续请求基本必被 429), 所以:
      1. 先试 JSON(时间戳自带时区偏移, 最可靠)
      2. 失败就退到 XML(时间字段是 UTC, 已实测确认)
      3. 成功后把结果缓存进 state.json, 默认 24 小时内不再重复请求
      4. 全部失败就继续用旧缓存(过期的日历也比没有强)
    """
    cache = state.get("macro_cache") or {}
    ttl = float(conf.get("cache_hours", 24))
    ttl_ok = False
    if cache.get("events") and cache.get("fetched_at"):
        try:
            age_h = (datetime.now(UTC)
                     - datetime.fromisoformat(cache["fetched_at"])).total_seconds() / 3600
            ttl_ok = age_h < ttl
            if ttl_ok:
                log(f"  · 财经日历: 用 {age_h:.1f} 小时前的缓存 "
                    f"({len(cache['events'])} 条事件)")
        except Exception:
            ttl_ok = False

    if ttl_ok:
        return [dict(e, when=datetime.fromisoformat(e["when"]))
                for e in cache["events"]]

    events: list[dict] = []
    for attempt in (1, 2, 3):
        try:
            resp = http_get(conf["url"], timeout=20)
            if resp.status_code == 200:
                payload = resp.json()
                for ev in payload if isinstance(payload, list) else []:
                    when = parse_date(ev.get("date", ""))
                    if when is None:
                        continue
                    events.append({
                        "title": ev.get("title", ""),
                        "country": ev.get("country", ""),
                        "impact": ev.get("impact", ""),
                        "when": when,
                        "forecast": ev.get("forecast") or "",
                        "previous": ev.get("previous") or "",
                        "actual": ev.get("actual") or "",
                    })
                log(f"  · 财经日历: JSON 接口成功, {len(events)} 条事件")
                break
        except Exception as exc:
            log(f"  · 财经日历第 {attempt} 次尝试失败 ({type(exc).__name__}), 稍后重试")
            time.sleep(4 * attempt)

    if not events:
        xml_url = conf.get("fallback_url", "")
        if xml_url:
            try:
                resp = http_get(xml_url, timeout=20)
                root = ET.fromstring(resp.content)
                for node in root.findall(".//event"):
                    when = _parse_ff_xml_time(node.findtext("date") or "",
                                              node.findtext("time") or "")
                    if when is None:
                        continue
                    events.append({
                        "title": node.findtext("title") or "",
                        "country": node.findtext("country") or "",
                        "impact": node.findtext("impact") or "",
                        "when": when,
                        "forecast": node.findtext("forecast") or "",
                        "previous": node.findtext("previous") or "",
                        "actual": node.findtext("actual") or "",
                    })
                log(f"  · 财经日历: 改用 XML 备用接口, {len(events)} 条事件")
            except Exception as exc:
                log(f"  [!] 财经日历 XML 备用接口也失败: {type(exc).__name__} {exc}")

    if events:
        state["macro_cache"] = {
            "fetched_at": datetime.now(UTC).isoformat(),
            "events": [dict(e, when=e["when"].isoformat()) for e in events],
        }
        return events

    if cache.get("events"):
        log("  [!] 财经日历本次全部失败, 回退到过期缓存")
        return [dict(e, when=datetime.fromisoformat(e["when"]))
                for e in cache["events"]]
    return []


def fetch_macro(cfg: dict, state: dict) -> list[dict]:
    """财经日历: 高影响数据公布前提前预警, 若已出实际值则直接推送。"""
    conf = cfg["sources"].get("macro_calendar", {})
    if not conf.get("enabled"):
        return []

    events = _load_macro_events(conf, state)
    if not events:
        log("  · 宏观日历: 无可用数据, 跳过")
        return []

    alerted = set(state.get("macro_alerted", []))
    now = datetime.now(UTC)
    lead = int(conf.get("lead_minutes", 20))
    countries = set(conf.get("countries", ["USD"]))
    impacts = set(conf.get("impacts", ["High"]))
    weight = conf.get("weight", 8)
    out: list[dict] = []

    for ev in events:
        if ev.get("impact") not in impacts:
            continue
        if ev.get("country") not in countries:
            continue
        when = ev.get("when")
        if when is None:
            continue

        key = f"{when.isoformat()}|{ev.get('title','')}"
        delta_min = (when - now).total_seconds() / 60
        title = f"{ev.get('title','')} ({ev.get('country','')})"

        # 情形一: 即将公布 → 提前预警
        if 0 < delta_min <= lead and f"pre|{key}" not in alerted:
            out.append(make_item(
                "宏观数据", f"即将公布: {title}",
                "https://www.forexfactory.com/calendar",
                when, "", weight,
                kind="macro", dedup_override=f"pre|{key}",
                detail=(f"预计 {fmt_time(when)} 公布 · 预测 {ev.get('forecast') or '—'} "
                        f"· 前值 {ev.get('previous') or '—'}"),
                force_score=int(conf.get("pre_score", 15)),
            ))

        # 情形二: 刚公布且有实际值 → 推送实际值
        actual = (ev.get("actual") or "").strip()
        if actual and -30 < delta_min <= 0 and f"act|{key}" not in alerted:
            out.append(make_item(
                "宏观数据", f"已公布: {title} = {actual}",
                "https://www.forexfactory.com/calendar",
                when, "", weight,
                kind="macro", dedup_override=f"act|{key}",
                detail=(f"实际 {actual} · 预测 {ev.get('forecast') or '—'} "
                        f"· 前值 {ev.get('previous') or '—'}"),
                force_score=int(conf.get("pre_score", 15)) + 3,
            ))

    log(f"  · 宏观日历: {len(out)} 条待推")
    return out



def fetch_price_moves(cfg: dict, state: dict) -> list[dict]:
    """比特币短时剧烈波动检测 —— 这本身就是最直接的"消息"。"""
    conf = cfg["sources"].get("price", {})
    if not conf.get("enabled"):
        return []
    sym = conf.get("symbol", "BTCUSDT")
    out: list[dict] = []
    try:
        k15 = get_json(
            f"https://api.binance.com/api/v3/klines?symbol={sym}"
            f"&interval=15m&limit=3", timeout=15)
        k1h = get_json(
            f"https://api.binance.com/api/v3/klines?symbol={sym}"
            f"&interval=1h&limit=3", timeout=15)
    except Exception as exc:
        log(f"  [!] 价格数据抓取失败: {type(exc).__name__} {exc}")
        return []

    c15 = [float(row[4]) for row in k15]
    c1h = [float(row[4]) for row in k1h]
    if len(c15) < 3 or len(c1h) < 3:
        return []

    # 用倒数第二根(已收盘)计算, 避免把正在波动中的K线算进去
    chg15 = (c15[-2] - c15[-3]) / c15[-3] * 100
    chg1h = (c1h[-2] - c1h[-3]) / c1h[-3] * 100
    last = c15[-2]

    hit = (abs(chg15) >= conf.get("move_15m_pct", 2.5)
           or abs(chg1h) >= conf.get("move_1h_pct", 5.0))
    log(f"  · BTC 现价 {last:,.0f} | 15分钟 {chg15:+.2f}% | 1小时 {chg1h:+.2f}%")
    if not hit:
        return []

    # 冷却时间, 防止同一波行情反复轰炸
    cooldown = int(conf.get("cooldown_minutes", 45))
    last_alert = state.get("price_last_alert")
    if last_alert:
        try:
            elapsed = (datetime.now(UTC)
                       - datetime.fromisoformat(last_alert)).total_seconds() / 60
            if elapsed < cooldown:
                log(f"  · 价格异动触发, 但仍在 {cooldown} 分钟冷却期内, 跳过")
                return []
        except Exception:
            pass

    direction = "急涨" if chg15 > 0 else "急跌"
    extra = ""
    try:
        prem = get_json(
            f"https://fapi.binance.com/fapi/v1/premiumIndex?symbol={sym}",
            timeout=12)
        rate = float(prem.get("lastFundingRate", 0)) * 100
        extra = f"资金费率 {rate:+.4f}% (正=多头拥挤)"
    except Exception:
        pass

    out.append(make_item(
        "价格异动", f"BTC {direction} {chg15:+.2f}% (15分钟)",
        "https://www.tradingview.com/chart/?symbol=BINANCE:BTCUSDT",
        datetime.now(UTC), "", conf.get("weight", 10),
        kind="price", dedup_override=f"price|{direction}|{int(last)}",
        detail=(f"现价 ${last:,.0f} · 15分钟 {chg15:+.2f}% · "
                f"1小时 {chg1h:+.2f}%" + (f" · {extra}" if extra else "")),
        force_score=int(conf.get("force_score", 20)),
    ))
    state["price_last_alert"] = datetime.now(UTC).isoformat()
    return out


def fetch_whales(cfg: dict, state: dict) -> list[dict]:
    """链上大额转账监控(内存池)。注意: 这是噪音最大的一个源。"""
    conf = cfg["sources"].get("whales", {})
    if not conf.get("enabled"):
        return []

    today = datetime.now(CST).strftime("%Y-%m-%d")
    counter = state.get("whale_count", {})
    if counter.get("date") != today:
        counter = {"date": today, "n": 0}
    if counter["n"] >= int(conf.get("max_per_day", 2)):
        log(f"  · 巨鲸监控: 今日已达上限 {counter['n']} 条, 跳过")
        state["whale_count"] = counter
        return []

    # 这个接口时不时会很慢, 所以超时给短一点、只重试两次,
    # 失败就跳过 —— 不能让它拖慢整轮抓取。
    txs = None
    for attempt in (1, 2):
        try:
            txs = get_json("https://mempool.space/api/mempool/recent", timeout=10)
            break
        except Exception as exc:
            log(f"  · 链上数据第 {attempt} 次尝试失败 ({type(exc).__name__})")
            if attempt < 2:
                time.sleep(2)
    if txs is None:
        log("  [!] 链上数据本次抓取失败, 跳过(不影响其他源)")
        return []

    min_sats = float(conf.get("min_btc", 300)) * 1e8
    big = []
    for tx in txs if isinstance(txs, list) else []:
        value = float(tx.get("value") or 0)          # 单位: 聪
        if value >= min_sats:
            big.append(tx)
    big.sort(key=lambda t: float(t.get("value") or 0), reverse=True)
    big = big[: int(conf.get("max_per_push", 2))]

    out = []
    for tx in big:
        btc = float(tx.get("value") or 0) / 1e8
        out.append(make_item(
            "链上巨鲸", f"链上大额转账 {btc:,.1f} BTC",
            f"https://mempool.space/tx/{tx.get('txid','')}",
            datetime.now(UTC), "", conf.get("weight", 3),
            kind="whale", dedup_override=f"whale|{tx.get('txid','')}",
            detail=f"金额 {btc:,.1f} BTC · 手续费 {tx.get('fee')} 聪 · 尚未确认",
            force_score=int(conf.get("force_score", 14)),
        ))
    if out:
        counter["n"] += len(out)
    state["whale_count"] = counter
    log(f"  · 链上巨鲸: {len(out)} 条(今日累计 {counter['n']})")
    return out


# --------------------------------------------------------------------------
# 打分与过滤
# --------------------------------------------------------------------------

def keyword_hits(text: str, keywords: list[str]) -> list[str]:
    """
    英文按词边界匹配(避免 warsaw 命中 war), 中文按子串匹配。
    以 * 结尾的词表示前缀匹配, 例如 "plung*" 可以命中 plunges / plunged / plunging。
    """
    hits = []
    for kw in keywords:
        kw_low = kw.lower()
        if kw_low.endswith("*"):
            stem = re.escape(kw_low[:-1])
            if re.search(r"(?<![a-z0-9])" + stem, text):
                hits.append(kw.rstrip("*"))
        elif re.fullmatch(r"[a-z0-9 &\-\+\.']+", kw_low):
            if re.search(r"(?<![a-z0-9])" + re.escape(kw_low) + r"(?![a-z0-9])", text):
                hits.append(kw)
        elif kw_low in text:
            hits.append(kw)
    return hits


def score_item(item: dict, cfg: dict) -> None:
    """
    按关键词给条目打分, 结果写回 item['score'] / item['reasons'] / item['hard']。

    item['hard'] 是"硬信号"标记 —— 表示这条消息里出现了真正的事件性词汇
    (宣战/空袭/制裁/下架/破产/被盗...), 而不只是提到了某个国家或机构。
    普通新闻必须带硬信号才可能被推送, 否则"提到伊朗"这种背景性报道会刷屏。
    """
    sc = cfg["scoring"]
    # 已经有强制分数的(价格/宏观/巨鲸)直接采用
    if item.get("force_score"):
        item["score"] = int(item["force_score"])
        item["reasons"] = ["系统级事件"]
        item["hard"] = True
        return

    # 标题和正文分开看: 事件性词汇只在标题里找。
    # 原因: 正文摘要里经常有历史背景(比如"俄罗斯2022年入侵乌克兰"),
    # 拿正文匹配会导致大量误报。真正的大事一定会写进标题。
    title_text = item["title"].lower()
    full_text = f"{item['title']} {item.get('summary','')}".lower()
    score = int(item.get("weight", 0))
    reasons = []

    # 第一档: 高置信度事件短语(宣战、撕毁协议、上币下架、紧急降息...)
    dec = keyword_hits(title_text, sc.get("decisive", []))
    if dec:
        score += 10 * min(len(dec), 2)
        reasons.append("决定性:" + "/".join(dec[:2]))

    # 第二档: 事件性词汇, 但单独出现不够, 需要实体配合
    crit = keyword_hits(title_text, sc.get("critical", []))
    if crit:
        score += 6 * min(len(crit), 2)
        reasons.append("重大:" + "/".join(crit[:2]))

    # 第三档: 实体/话题词 —— 这一档才允许匹配正文
    topic_words = sc.get("topic", [])
    # 特朗普自己的帖子: 不能因为标题里出现"特朗普"就给自己加分,
    # 否则他发任何一条无关动态都会被推送。要求必须提到别的实质话题。
    if item.get("kind") == "trump":
        topic_words = [w for w in topic_words
                       if w.lower() not in ("trump", "白宫", "white house")]
    topic = keyword_hits(full_text, topic_words)
    if topic:
        score += 5 * min(len(topic), 3)
        reasons.append("相关:" + "/".join(topic[:3]))

    crypto = keyword_hits(full_text, sc.get("crypto", []))
    if crypto:
        score += 4 * min(len(crypto), 2)
        reasons.append("币圈:" + "/".join(crypto[:2]))

    item["score"] = min(score, 60)
    item["reasons"] = reasons
    # 硬信号: 有决定性短语, 或者(事件词 + 实体词)同时出现
    item["hard"] = bool(dec) or bool(crit and (topic or crypto))


def is_excluded(item: dict, cfg: dict) -> bool:
    """命中排除词的直接丢弃。"""
    text = f"{item['title']} {item.get('summary','')}".lower()
    for kw in cfg["scoring"].get("exclude", []):
        if kw.lower() in text:
            return True
    # 币安公告的专属排除词。例行公告(季度交割合约、杠杆调整之类)
    # 会命中"will launch/will list"这种决定性短语, 但根本没人在意。
    if item.get("kind") == "announcement":
        conf = cfg["sources"].get("binance_announcements", {})
        for kw in conf.get("exclude_titles", []):
            if kw.lower() in text:
                return True
    return False


def filter_items(items: list[dict], cfg: dict) -> tuple[list[dict], list[dict]]:
    """返回 (通过的条目, 被丢弃的条目)。"""
    threshold = int(cfg["scoring"].get("threshold", 12))
    max_age = int(cfg["scoring"].get("max_age_hours", 12))
    cutoff = datetime.now(UTC) - timedelta(hours=max_age)

    passed, dropped = [], []
    for item in items:
        if is_excluded(item, cfg):
            item["drop_reason"] = "命中排除词"
            dropped.append(item)
            continue
        if item["score"] < threshold:
            item["drop_reason"] = f"分数不足({item['score']}<{threshold})"
            dropped.append(item)
            continue
        # 硬信号闸门: 光提到"伊朗"不够, 必须真的发生了什么事才推。
        # 这一条是压制"背景性报道刷屏"的关键。
        if cfg["scoring"].get("require_hard_signal", True) and not item.get("hard"):
            item["drop_reason"] = "缺少硬信号(只是背景性提及)"
            dropped.append(item)
            continue
        # 太旧的消息不推(避免首次运行时把历史消息全推一遍)
        if item.get("ts") and item["ts"] < cutoff:
            item["drop_reason"] = "消息过旧"
            dropped.append(item)
            continue
        passed.append(item)

    passed.sort(key=lambda x: (-x["score"], -(x.get("ts") or datetime.now(UTC)).timestamp()))
    return passed, dropped


# --------------------------------------------------------------------------
# 推送
# --------------------------------------------------------------------------

def build_message(items: list[dict], cfg: dict, sent_today: int) -> tuple[str, str]:
    """生成 PushPlus 的标题和正文(markdown)。"""
    top = items[0]
    if len(items) == 1:
        title = f"【雷达】{top['source']}: {top['title'][:60]}"
    else:
        title = f"【雷达】{len(items)}条: {top['title'][:50]}"

    lines = [f"## 情报雷达 · {len(items)} 条", ""]
    for idx, item in enumerate(items, 1):
        lines.append(f"**{idx}. [{item['source']}] {item['title']}**")
        if item.get("detail"):
            lines.append(item["detail"])
        elif item.get("summary"):
            lines.append(item["summary"][:200])
        meta = f"{fmt_time(item.get('ts'))} · 评分 {item['score']}"
        if item.get("reasons"):
            meta += " · " + ", ".join(item["reasons"])
        lines.append(f"`{meta}`")
        if item.get("url"):
            lines.append(f"[查看原文]({item['url']})")
        lines.append("")

    cap = int(cfg["push"].get("max_pushes_per_day", 15))
    lines.append("---")
    lines.append(f"今日已推 {sent_today + 1}/{cap} 条 · 阈值 {cfg['scoring']['threshold']}")
    return title, "\n".join(lines)


def send_push(title: str, content: str, cfg: dict) -> bool:
    token = os.environ.get("PUSHPLUS_TOKEN", "").strip()
    if not token:
        log("[!] 没有设置 PUSHPLUS_TOKEN 环境变量, 无法推送")
        return False

    payload = {
        "token": token,
        "title": title[:100],
        "content": content,
        "template": cfg["push"].get("template", "markdown"),
    }
    topic = cfg["push"].get("topic", "").strip()
    if topic:
        payload["topic"] = topic

    try:
        resp = requests.post(PUSHPLUS_API, json=payload, headers=HEADERS, timeout=20)
        data = resp.json()
    except Exception as exc:
        log(f"[!] 推送请求失败: {type(exc).__name__} {exc}")
        return False

    code = data.get("code")
    if code == 200:
        log(f"[✓] 推送成功 (流水号 {data.get('data')})")
        return True

    # 以下错误重试也没用, 直接给出明确指引
    hints = {
        903: "PUSHPLUS_TOKEN 不正确, 请到 pushplus.plus 个人中心重新复制",
        905: "账号未完成实名认证! 请到 verify.pushplus.plus 完成手机号验证后再试",
        900: "已超出当日免费额度或账号被限制, 请明天再试",
        302: "未登录, 请重新获取 token",
    }
    log(f"[!] 推送被拒绝 code={code} msg={data.get('msg')}")
    if code in hints:
        log(f"    → {hints[code]}")
    return False


# --------------------------------------------------------------------------
# 可选的 LLM 二次过滤
# --------------------------------------------------------------------------

def llm_should_push(item: dict, cfg: dict) -> tuple[bool | None, str]:
    """
    对"擦边"的条目调用 Claude 判断是否真的值得打扰用户。
    返回 (判断结果, 理由); 返回 None 表示不可用/出错, 此时沿用关键词判断。

    只有在 config.yaml 里把 llm_filter.enabled 设为 true
    并且设置了 ANTHROPIC_API_KEY 环境变量时才会生效。
    """
    conf = cfg.get("llm_filter", {})
    api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if not conf.get("enabled") or not api_key:
        return None, ""

    prompt = (
        "你是一个加密货币交易员的情报过滤助手。判断下面这条消息是否达到"
        "\"值得立刻推送手机提醒\"的程度。\n\n"
        "推送标准: 消息若属实, 可能在未来数小时内引起比特币价格超过 2% 的波动。\n"
        "不要推送: 常规行情分析、观点评论、价格预测、回顾性报道、广告、"
        "与市场无关的社会新闻。\n\n"
        f"标题: {item['title']}\n"
        f"摘要: {item.get('summary','(无)')[:300]}\n"
        f"来源: {item['source']}\n\n"
        "只输出一个 JSON, 不要有任何其他文字:\n"
        '{"push": true 或 false, "reason": "一句话中文理由"}'
    )

    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": conf.get("model", "claude-3-5-haiku-latest"),
                "max_tokens": 120,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=25,
        )
        resp.raise_for_status()
        text = resp.json()["content"][0]["text"]
        match = re.search(r"\{.*\}", text, re.S)
        if not match:
            return None, ""
        verdict = json.loads(match.group(0))
        return bool(verdict.get("push")), str(verdict.get("reason", ""))[:80]
    except Exception as exc:
        log(f"  [!] LLM 过滤调用失败, 回退到关键词判断: {type(exc).__name__}")
        return None, ""


def apply_llm_filter(items: list[dict], cfg: dict) -> list[dict]:
    """只对分数处在阈值边缘的条目调用 LLM, 控制成本。"""
    conf = cfg.get("llm_filter", {})
    if not conf.get("enabled") or not os.environ.get("ANTHROPIC_API_KEY"):
        return items

    threshold = int(cfg["scoring"].get("threshold", 12))
    borderline = int(conf.get("borderline_band", 6))
    kept: list[dict] = []

    for item in items:
        if abs(item["score"] - threshold) > borderline:
            kept.append(item)          # 分数很确定, 不浪费一次 API 调用
            continue
        verdict, reason = llm_should_push(item, cfg)
        if verdict is None:
            kept.append(item)          # 调用失败, 保守放行(沿用关键词结论)
        elif verdict:
            item["reasons"].append(f"AI:{reason}")
            kept.append(item)
        else:
            item["drop_reason"] = f"AI 判定不推: {reason}"
            log(f"  · AI 拦截: {item['title'][:50]} ({reason})")
    return kept


# --------------------------------------------------------------------------
# 主流程
# --------------------------------------------------------------------------

def run_test(cfg: dict) -> int:
    """--test: 发一条测试消息, 验证 token 和推送链路。"""
    log("=" * 56)
    log("测试模式: 发送一条测试推送到你的微信")
    log("=" * 56)
    now = datetime.now(CST).strftime("%Y-%m-%d %H:%M:%S")
    title = "【雷达】测试推送成功"
    content = (
        "## 链路连通\n\n"
        "如果你在微信里看到了这条消息, 说明 PushPlus 配置正确。\n\n"
        f"- 令牌: 有效\n"
        f"- 时间: {now}\n"
        f"- 阈值: {cfg['scoring']['threshold']}\n\n"
        "接下来把 GitHub Actions 的定时任务打开就行了。\n"
        "如果只看到标题看不到正文, 请在 PushPlus 公众号里发送「激活消息」。"
    )
    ok = send_push(title, content, cfg)
    log("测试结果:", "成功" if ok else "失败 —— 请看上面的错误提示")
    return 0 if ok else 1


def run_once(cfg: dict, dry_run: bool = False) -> int:
    state = load_state()
    first_run = not state.get("initialized")

    log("开始抓取数据源 ...")
    raw: list[dict] = []
    raw += fetch_feeds(cfg)
    raw += fetch_trump(cfg)
    raw += fetch_binance_announcements(cfg)
    raw += fetch_macro(cfg, state)
    raw += fetch_whales(cfg, state)
    raw += fetch_price_moves(cfg, state)
    log(f"共抓取 {len(raw)} 条原始条目")

    # 打分
    for item in raw:
        score_item(item, cfg)

    # 去重(用历史记录 + 本次运行内去重)
    seen = state.get("seen", {})
    unique: list[dict] = []
    fresh_keys: set[str] = set()
    dup = 0
    for item in raw:
        key = item.get("dedup_override") or item_key(item)
        item["_key"] = key
        if key in seen or key in fresh_keys:
            dup += 1
            continue
        fresh_keys.add(key)
        unique.append(item)
    log(f"去重后剩余 {len(unique)} 条 (跳过重复 {dup} 条)")

    passed, dropped = filter_items(unique, cfg)
    log(f"达到阈值 {cfg['scoring']['threshold']} 的: {len(passed)} 条")
    for item in passed:
        log(f"    [{item['score']:>2}] ({item['source']}) {item['title'][:64]}")

    # 首次运行: 只记录不推送, 避免一次性轰炸
    if first_run:
        log("检测到首次运行 —— 仅建立基线, 本次不推送任何消息")
        for item in unique:
            if item.get("kind") == "macro":
                continue          # 宏观事件不写进 seen, 交给 macro_alerted 管
            state.setdefault("seen", {})[item["_key"]] = datetime.now(UTC).isoformat()
        state["initialized"] = True
        save_state(state)
        log("基线已建立。下一条新消息就会正常推送。")
        return 0

    # 可选的 AI 二次过滤
    passed = apply_llm_filter(passed, cfg)

    # 每日上限
    sent_today = bump_daily_counter(state)
    cap = int(cfg["push"].get("max_pushes_per_day", 15))
    if sent_today >= cap:
        log(f"今日推送已达上限 {cap} 条, 本轮不再推送")
        passed = []

    max_items = int(cfg["push"].get("max_items_per_push", 5))
    push_list = passed[:max_items]

    # 记录所有已见条目(包括没推的), 防止下轮重复评估
    now_iso = datetime.now(UTC).isoformat()
    for item in unique:
        if item.get("kind") == "macro":
            continue          # 宏观事件单独管理, 见下面的 macro_alerted
        state.setdefault("seen", {})[item["_key"]] = now_iso

    # 宏观事件的去重靠 macro_alerted 而不是 seen。这样如果某次因为
    # "今日推送已达上限"而没能推出去, 下一轮还有机会补推。
    if sent_today < cap:
        macro_keys = [i["_key"] for i in unique if i.get("kind") == "macro"]
        if macro_keys:
            state["macro_alerted"] = sorted(
                set(state.get("macro_alerted", [])) | set(macro_keys))[-200:]

    if push_list:
        title, content = build_message(push_list, cfg, sent_today)
        if dry_run:
            log("-" * 56)
            log("[dry-run] 本应推送:")
            log(title)
            log(content)
            log("-" * 56)
        elif send_push(title, content, cfg):
            state["push_count"]["n"] = sent_today + 1
            state["last_push"] = now_iso
    else:
        log("没有达到推送条件的消息")

    save_state(state)
    log("本轮结束")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="比特币情报雷达")
    parser.add_argument("--test", action="store_true", help="发送测试推送后退出")
    parser.add_argument("--dry-run", action="store_true", help="只打印不推送")
    args = parser.parse_args()

    if not CONFIG_PATH.exists():
        log(f"[!] 找不到配置文件: {CONFIG_PATH}")
        return 1
    with open(CONFIG_PATH, encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh)

    if args.test:
        return run_test(cfg)
    return run_once(cfg, dry_run=args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
