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
import html
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
    # html.unescape 能处理全部实体, 包括 &#039; 这种带前导零的数字实体
    # (手写替换表很容易漏, 之前就漏过 &#039; 导致推送里出现 "Russia&#039;s")
    text = html.unescape(text).replace("\xa0", " ")
    return re.sub(r"\s+", " ", text).strip()


def _normalize_fraction(raw: str) -> str:
    """
    把小数秒补齐成 6 位。

    为什么需要: 路透社的时间戳长这样 "2026-09-25T11:03:44.62Z", 只有 2 位小数。
    Python 3.10 的 datetime.fromisoformat 只接受 3 位或 6 位小数秒, 会直接抛异常,
    导致所有路透社消息的发布时间变成"时间未知"(3.11 之后才放宽)。补齐即可。
    """
    return re.sub(r"\.(\d+)(?=[Z+\-]|$)",
                  lambda m: "." + (m.group(1) + "000000")[:6], raw, count=1)


def parse_date(raw: str):
    """兼容 RFC822(RSS)、ISO8601(Atom/JSON)、以及各种小数秒位数。"""
    raw = (raw or "").strip()
    if not raw:
        return None

    # 先试 RSS 的时间格式
    try:
        dt = parsedate_to_datetime(raw)
        if dt is not None:
            return dt if dt.tzinfo else dt.replace(tzinfo=UTC)
    except Exception:
        pass

    # 再试 ISO8601: 原样和补齐小数秒两种都试一遍
    for candidate in (raw, _normalize_fraction(raw)):
        try:
            dt = datetime.fromisoformat(candidate.replace("Z", "+00:00"))
            # 必须保证返回"带时区"的时间, 否则后面做减法会直接崩
            return dt if dt.tzinfo else dt.replace(tzinfo=UTC)
        except Exception:
            continue
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
# 市场情绪数据
# --------------------------------------------------------------------------
# 收集一组"客观指标", 用来回答"现在市场是冷是热、多头还是空头拥挤"。
# 每个指标独立抓取, 任何一个失败都不影响其他的。
# 这些数字本身不是买卖建议, 是判断材料。
# --------------------------------------------------------------------------

CN_FNG = {
    "Extreme Fear": "极度恐惧", "Fear": "恐惧", "Neutral": "中性",
    "Greed": "贪婪", "Extreme Greed": "极度贪婪",
}


def _cached_fetch(state: dict, key: str, ttl_hours: float, fn):
    """
    带缓存的抓取。用于更新很慢(ETF 资金流一天才一次)、
    或者容易被限流(CoinGecko)的接口。
    抓取失败时回退到过期缓存, 有总比没有好。
    """
    cache = state.setdefault("cache", {})
    entry = cache.get(key) or {}
    if entry.get("data") is not None and entry.get("ts"):
        try:
            age = (datetime.now(UTC)
                   - datetime.fromisoformat(entry["ts"])).total_seconds() / 3600
            if age < ttl_hours:
                return entry["data"]
        except Exception:
            pass
    try:
        data = fn()
    except Exception:
        data = None
    if data is not None:
        cache[key] = {"ts": datetime.now(UTC).isoformat(), "data": data}
        return data
    return entry.get("data")


def collect_sentiment(cfg: dict, state: dict) -> dict:
    """抓取一整套市场情绪指标。返回一个扁平 dict, 失败项就是缺字段。"""
    conf = cfg.get("sentiment", {})
    if not conf.get("enabled", True):
        return {}

    sym = conf.get("symbol", "BTCUSDT")
    s: dict = {"symbol": sym}

    # --- 恐惧贪婪指数(0-100)---
    try:
        data = get_json("https://api.alternative.me/fng/?limit=30", timeout=15)
        rows = data.get("data") or []
        if rows:
            s["fng"] = int(rows[0]["value"])
            s["fng_label"] = CN_FNG.get(rows[0].get("value_classification", ""),
                                        rows[0].get("value_classification", ""))
            recent = [int(r["value"]) for r in rows[:7]]
            s["fng_7d_avg"] = round(sum(recent) / len(recent), 1)
            s["fng_30d_min"] = min(int(r["value"]) for r in rows)
            s["fng_30d_max"] = max(int(r["value"]) for r in rows)
    except Exception as exc:
        log(f"  [!] 恐惧贪婪指数失败: {type(exc).__name__}")

    # --- 资金费率(当前值 + 一周均值, 判断多头是否拥挤)---
    try:
        prem = get_json(f"https://fapi.binance.com/fapi/v1/premiumIndex?symbol={sym}",
                        timeout=12)
        s["funding"] = round(float(prem.get("lastFundingRate") or 0) * 100, 4)
        hist = get_json(f"https://fapi.binance.com/fapi/v1/fundingRate"
                        f"?symbol={sym}&limit=21", timeout=12)
        rates = [float(r["fundingRate"]) * 100 for r in hist]
        if rates:
            s["funding_7d_avg"] = round(sum(rates) / len(rates), 4)
            s["funding_7d_max"] = round(max(rates), 4)
            s["funding_7d_min"] = round(min(rates), 4)
    except Exception as exc:
        log(f"  [!] 资金费率失败: {type(exc).__name__}")

    # --- 多空持仓比(>1 表示账户层面多头多)---
    try:
        ls = get_json("https://fapi.binance.com/futures/data/globalLongShortAccountRatio"
                      f"?symbol={sym}&period=1d&limit=7", timeout=12)
        if ls:
            s["long_short"] = round(float(ls[0]["longShortRatio"]), 3)
            ratios = [float(x["longShortRatio"]) for x in ls]
            s["long_short_7d_max"] = round(max(ratios), 3)
            s["long_short_7d_avg"] = round(sum(ratios) / len(ratios), 3)
    except Exception as exc:
        log(f"  [!] 多空比失败: {type(exc).__name__}")

    # --- 主动买卖量比(>1 表示主动买盘更强)---
    try:
        tk = get_json("https://fapi.binance.com/futures/data/takerlongshortRatio"
                      f"?symbol={sym}&period=1d&limit=2", timeout=12)
        if tk:
            s["taker_ratio"] = round(float(tk[0]["buySellRatio"]), 3)
    except Exception as exc:
        log(f"  [!] 主动买卖比失败: {type(exc).__name__}")

    # --- 未平仓合约量 24 小时变化(配合价格看是加仓还是平仓)---
    try:
        oi = get_json("https://fapi.binance.com/futures/data/openInterestHist"
                      f"?symbol={sym}&period=1d&limit=2", timeout=12)
        if len(oi) >= 2:
            now_oi = float(oi[0]["sumOpenInterestValue"])
            prev_oi = float(oi[1]["sumOpenInterestValue"])
            s["oi_usd"] = now_oi
            if prev_oi:
                s["oi_change_24h"] = round((now_oi - prev_oi) / prev_oi * 100, 2)
    except Exception as exc:
        log(f"  [!] 未平仓量失败: {type(exc).__name__}")

    # --- 美国现货比特币 ETF 资金流(近几个交易日, 单位美元)---
    def _etf():
        resp = requests.post(
            "https://api.sosovalue.xyz/openapi/v2/etf/historicalInflowChart",
            headers={**HEADERS, "Content-Type": "application/json"},
            json={"type": conf.get("etf_type", "us-btc-spot")}, timeout=20)
        resp.raise_for_status()
        return (resp.json().get("data") or [])[:10]

    rows = _cached_fetch(state, "etf_flows", conf.get("etf_cache_hours", 6), _etf)
    if rows:
        flows = [float(r.get("totalNetInflow") or 0) for r in rows]
        s["etf_recent"] = flows[:5]
        s["etf_1d"] = flows[0] if flows else None
        s["etf_3d"] = sum(flows[:3])
        s["etf_5d"] = sum(flows[:5])
        s["etf_days"] = len(flows)

    # --- 现货行情 ---
    try:
        tk24 = get_json(f"https://api.binance.com/api/v3/ticker/24hr?symbol={sym}",
                        timeout=12)
        s["price"] = float(tk24["lastPrice"])
        s["change_24h"] = round(float(tk24["priceChangePercent"]), 2)
        s["volume_24h_usd"] = float(tk24["quoteVolume"])
        kl = get_json(f"https://api.binance.com/api/v3/klines?symbol={sym}"
                      f"&interval=1d&limit=8", timeout=12)
        if len(kl) >= 8:
            first, last = float(kl[0][4]), float(kl[-1][4])
            if first:
                s["change_7d"] = round((last / first - 1) * 100, 2)
    except Exception as exc:
        log(f"  [!] 行情失败: {type(exc).__name__}")

    # --- 比特币市占率 ---
    def _dom():
        g = get_json("https://api.coingecko.com/api/v3/global", timeout=15)
        return round(float(g["data"]["market_cap_percentage"]["btc"]), 2)

    dom = _cached_fetch(state, "btc_dominance", 6, _dom)
    if dom:
        s["btc_dominance"] = dom

    got = [k for k in ("fng", "funding", "long_short", "oi_change_24h",
                       "etf_3d", "price") if s.get(k) is not None]
    log(f"  · 市场情绪: 取到 {len(got)}/6 项核心指标 ({', '.join(got)})")
    return s


def sentiment_summary_line(s: dict) -> str:
    """把情绪数据压成一行, 放在推送顶部给用户一眼看完。"""
    if not s:
        return ""
    parts = []
    if s.get("fng") is not None:
        parts.append(f"恐惧贪婪 {s['fng']}({s.get('fng_label','')})")
    if s.get("funding") is not None:
        parts.append(f"资金费率 {s['funding']:+.4f}%")
    if s.get("long_short") is not None:
        parts.append(f"多空比 {s['long_short']:.2f}")
    if s.get("oi_change_24h") is not None:
        parts.append(f"未平仓 24h {s['oi_change_24h']:+.1f}%")
    if s.get("etf_3d") is not None:
        parts.append(f"ETF 近3日 {s['etf_3d'] / 1e8:+.2f} 亿美元")
    if s.get("price") is not None:
        parts.append(f"BTC ${s['price']:,.0f}")
    if s.get("change_24h") is not None and s.get("change_7d") is not None:
        parts.append(f"({s['change_24h']:+.1f}%/24h, {s['change_7d']:+.1f}%/7d)")
    return " · ".join(parts)


def sentiment_facts(s: dict) -> str:
    """给 AI 看的详细版指标清单。"""
    if not s:
        return "(本轮的行情指标抓取失败, 没有可用数据)"
    lines = []
    if s.get("price") is not None:
        lines.append(f"- BTC 价格: ${s['price']:,.0f}"
                     f" (24小时 {s.get('change_24h', 0):+.2f}%, "
                     f"7天 {s.get('change_7d', 0):+.2f}%)")
    if s.get("fng") is not None:
        lines.append(f"- 恐惧贪婪指数: {s['fng']}/100 ({s.get('fng_label','')}), "
                     f"近7日均值 {s.get('fng_7d_avg','?')}, "
                     f"近30日区间 {s.get('fng_30d_min','?')}-{s.get('fng_30d_max','?')}")
    if s.get("funding") is not None:
        lines.append(f"- 合约资金费率(8小时): {s['funding']:+.4f}%, "
                     f"近7日均值 {s.get('funding_7d_avg','?')}%, "
                     f"近7日区间 {s.get('funding_7d_min','?')}% ~ {s.get('funding_7d_max','?')}%"
                     " (正值=多头付费, 越高说明多头越拥挤)")
    if s.get("long_short") is not None:
        lines.append(f"- 多空账户比: {s['long_short']:.2f}, "
                     f"近7日均值 {s.get('long_short_7d_avg','?')}, "
                     f"近7日最高 {s.get('long_short_7d_max','?')} (>1 表示多数账户持多)")
    if s.get("taker_ratio") is not None:
        lines.append(f"- 主动买卖量比: {s['taker_ratio']:.2f} (>1 表示主动买盘更强)")
    if s.get("oi_change_24h") is not None:
        lines.append(f"- 未平仓合约量: 24小时 {s['oi_change_24h']:+.2f}%, "
                     f"当前名义价值 ${s.get('oi_usd', 0) / 1e9:.2f}B")
    if s.get("etf_3d") is not None:
        daily = ", ".join(f"{v / 1e6:+.0f}M" for v in s.get("etf_recent", []))
        lines.append(f"- 美国现货比特币ETF净流入: 近几日(新→旧) {daily} 美元; "
                     f"近3日累计 {s['etf_3d'] / 1e8:+.2f}亿美元, "
                     f"近5日累计 {s.get('etf_5d', 0) / 1e8:+.2f}亿美元")
    if s.get("btc_dominance") is not None:
        lines.append(f"- 比特币市占率: {s['btc_dominance']:.1f}%")
    return "\n".join(lines) if lines else "(本轮指标抓取全部失败)"


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

def build_message(items: list[dict], cfg: dict, sent_today: int,
                  sentiment: dict | None = None,
                  lean: dict | None = None) -> tuple[str, str]:
    """
    生成 PushPlus 的标题和正文(markdown)。

    正文分三段:
      1. 市场情绪 —— 客观指标, 一眼看清现在的市场温度
      2. 多空倾向 —— AI 从上面这些指标推导出的短期/中期结论(附理由)
      3. 情报    —— 每条消息带利好利空判断和传导逻辑
    """
    top = items[0]
    lean = lean or {}
    short_lean = (lean.get("short_term") or {}).get("lean", "")
    top_direction = (top.get("ai") or {}).get("direction", "")

    # 标题在微信里一定会显示, 所以把最重要的信息塞进去
    if len(items) == 1:
        prefix = f"{top_direction} · " if top_direction in ("利好", "利空") else ""
        title = f"【雷达】{prefix}{top['title'][:56]}"
    elif short_lean:
        title = f"【雷达】短期{short_lean} · {len(items)}条情报"
    else:
        title = f"【雷达】{len(items)}条情报"

    lines: list[str] = []

    # ---- 1. 市场情绪 ----
    summary = sentiment_summary_line(sentiment or {})
    if summary:
        lines += ["## 市场情绪", summary, ""]

    # ---- 2. 多空倾向 ----
    st, mt = lean.get("short_term") or {}, lean.get("mid_term") or {}
    if st.get("lean") or mt.get("lean"):
        if st.get("lean"):
            lines.append(f"**短期(1-7天): {st['lean']}** — {st.get('reason', '')}")
        if mt.get("lean"):
            lines.append(f"**中期(1-3个月): {mt['lean']}** — {mt.get('reason', '')}")
        if lean.get("conflict"):
            lines.append(f"矛盾信号: {lean['conflict']}")
        lines.append("")

    # ---- 3. 情报 ----
    lines += [f"## 情报 · {len(items)} 条", ""]
    for idx, item in enumerate(items, 1):
        lines.append(f"**{idx}. [{item['source']}] {item['title']}**")

        ai = item.get("ai") or {}
        if ai.get("direction"):
            bits = [f"**{ai['direction']}**"]
            if ai.get("confidence"):
                bits.append(f"置信度{ai['confidence']}")
            if ai.get("horizon"):
                bits.append(f"影响{ai['horizon']}")
            lines.append(" · ".join(bits))
        if ai.get("reason"):
            lines.append(ai["reason"])

        if item.get("detail"):
            lines.append(item["detail"])
        elif item.get("summary"):
            lines.append(item["summary"][:180])

        meta = f"{fmt_time(item.get('ts'))} · 评分 {item['score']}"
        if item.get("reasons"):
            meta += " · " + ", ".join(item["reasons"])
        lines.append(f"`{meta}`")
        if item.get("url"):
            lines.append(f"[查看原文]({item['url']})")
        lines.append("")

    cap = int(cfg["push"].get("max_pushes_per_day", 15))
    disclaimer = cfg["push"].get("disclaimer", "信息整理,非投资建议")
    lines.append("---")
    lines.append(f"{disclaimer} · 今日已推 {sent_today + 1}/{cap} 条 · "
                 f"阈值 {cfg['scoring']['threshold']}")
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
# AI 分析层(利好利空判断 + 市场多空倾向)
# --------------------------------------------------------------------------
# 说明: 这一层只做两件事 ——
#   1. 判断每条消息对比特币是利好还是利空, 并说清传导机制
#   2. 把上一步收集的客观指标汇总成短期/中期倾向
# 它不预测价格, 也不给买卖点位。所有结论都必须能追溯到列出的理由。
#
# 重要: AI 挂掉时不会影响推送 —— 消息照常发, 只是少一段分析。
# --------------------------------------------------------------------------

AI_BASE = "https://api.anthropic.com/v1/messages"
AI_MODELS_URL = "https://api.anthropic.com/v1/models"


def ai_available(cfg: dict) -> bool:
    return bool(cfg.get("ai", {}).get("enabled")
                and os.environ.get("ANTHROPIC_API_KEY", "").strip())


def _ai_headers() -> dict:
    return {
        "x-api-key": os.environ.get("ANTHROPIC_API_KEY", "").strip(),
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }


def _extract_json(text: str, expect_list: bool = False):
    """从模型回复里把 JSON 抠出来, 容忍 markdown 代码块包裹和前后废话。"""
    if not text:
        return None
    body = re.sub(r"^```[a-zA-Z]*\s*", "", text.strip())
    body = re.sub(r"\s*```$", "", body)
    open_ch, close_ch = ("[", "]") if expect_list else ("{", "}")
    start, end = body.find(open_ch), body.rfind(close_ch)
    if start == -1 or end == -1 or end < start:
        return None
    try:
        return json.loads(body[start:end + 1])
    except Exception:
        return None


def resolve_model(cfg: dict, state: dict) -> str | None:
    """
    确定用哪个模型。模型名会随时间变化, 所以不写死:
      1. 先问 Anthropic 的 /v1/models 接口"你现在有哪些模型"(免费, 不耗 token)
         从中挑一个最便宜的可用档位(haiku > sonnet > opus)
      2. 问不到就退回 config.yaml 里手写的候选列表
      3. 结果缓存进 state.json, 之后不再重复探测
    """
    cached = state.get("ai_model")
    if cached:
        return cached

    try:
        resp = requests.get(f"{AI_MODELS_URL}?limit=100",
                            headers=_ai_headers(), timeout=20)
        if resp.status_code == 200:
            ids = [m.get("id", "") for m in (resp.json().get("data") or [])]
            for tier in ("haiku", "sonnet", "opus"):
                for mid in ids:
                    if tier in mid.lower():
                        log(f"  · AI 模型: {mid} (自动探测)")
                        state["ai_model"] = mid
                        return mid
            log(f"  [!] 账号下没找到可用模型, 返回: {ids[:5]}")
    except Exception as exc:
        log(f"  · 模型探测失败({type(exc).__name__}), 改用配置里的候选列表")

    for mid in cfg.get("ai", {}).get("models", []):
        log(f"  · AI 模型: {mid} (配置候选)")
        state["ai_model"] = mid
        return mid
    return None


def _ai_raw(cfg: dict, state: dict, prompt: str, max_tokens: int) -> str | None:
    """调一次 Claude。任何失败都返回 None(调用方负责降级)。"""
    if not ai_available(cfg):
        return None
    model = resolve_model(cfg, state)
    if not model:
        return None

    body = {"model": model, "max_tokens": max_tokens,
            "messages": [{"role": "user", "content": prompt}]}

    for attempt in (1, 2):
        try:
            resp = requests.post(AI_BASE, headers=_ai_headers(), json=body,
                                 timeout=60)
            if resp.status_code == 200:
                return resp.json()["content"][0]["text"]

            snippet = resp.text[:220]
            log(f"  [!] AI 调用失败 {resp.status_code}: {snippet}")
            # 模型名失效(改名/下线) → 清掉缓存重新探测一次再试
            if resp.status_code in (400, 404) and "model" in snippet.lower():
                state.pop("ai_model", None)
                new_model = resolve_model(cfg, state)
                if new_model and new_model != model:
                    model = new_model
                    body["model"] = model
                    log(f"  · 换用模型重试: {model}")
                    continue
            return None
        except Exception as exc:
            log(f"  [!] AI 调用异常: {type(exc).__name__} {exc}")
            if attempt == 1:
                time.sleep(3)
    return None


ITEM_PROMPT = """你是加密货币交易员的情报分析助手。下面是 {n} 条可能影响比特币价格的新闻。

{items}

请对每一条判断两件事:

1. «keep»: 这条值不值得立刻推送手机提醒?
   判断标准: 如果消息属实, 是否可能在未来数小时内引起比特币价格超过 2% 的波动。
   不值得推的典型: 常规行情综述、观点评论、价格预测、历史回顾、营销内容、
   与市场无关的社会新闻、只提到某个国家但没有实质事件。
   注意: 只有被标记为「擦边」的那几条需要你决定去留; 被标记为「高分」的请一律 keep=true。

2. «direction»: 对比特币是「利好」「利空」「中性」还是「方向不明」。
   «confidence»: 「高」「中」「低」。
   «horizon»: 「短期」(数小时到数天)、「中期」(数周到数月) 还是「两者」。
   «reason»: 一句话讲清传导机制 —— 为什么会这样影响价格。不超过 40 字。

必须遵守:
- 如果影响路径不清晰, 或者市场很可能已经提前计价, 就选「方向不明」并在理由里说明。
  不要为了给出结论而强行判断。
- 不要预测价格, 不要给买卖建议。
- 严格输出 JSON 数组, 长度正好 {n}, 不要任何其他文字:
[{{"keep":true,"direction":"利空","confidence":"高","horizon":"短期","reason":"..."}}]"""


def ai_process_items(items: list[dict], cfg: dict, state: dict) -> list[dict]:
    """
    一次 API 调用同时完成"边缘条目去噪"和"利好利空标注"。
    返回处理后的条目列表。AI 不可用时原样返回。
    """
    if not items or not ai_available(cfg):
        return items

    threshold = int(cfg["scoring"].get("threshold", 12))
    band = int(cfg.get("ai", {}).get("borderline_band", 6))

    lines = []
    for idx, item in enumerate(items, 1):
        tag = "擦边" if abs(item["score"] - threshold) <= band else "高分"
        lines.append(
            f"[{idx}] ({tag}) 标题: {item['title']}\n"
            f"    来源: {item['source']}\n"
            f"    摘要: {item.get('summary', '')[:220] or '(无)'}"
        )

    prompt = ITEM_PROMPT.format(n=len(items), items="\n".join(lines))
    text = _ai_raw(cfg, state, prompt, max_tokens=200 + 110 * len(items))
    if not text:
        log("  · AI 不可用, 本轮消息不带分析直接推送")
        return items

    verdicts = _extract_json(text, expect_list=True)
    if not isinstance(verdicts, list):
        log("  · AI 返回内容无法解析, 本轮消息不带分析直接推送")
        return items

    kept: list[dict] = []
    for idx, item in enumerate(items):
        verdict = verdicts[idx] if idx < len(verdicts) else {}
        if not isinstance(verdict, dict):
            kept.append(item)
            continue

        is_borderline = abs(item["score"] - threshold) <= band
        if is_borderline and verdict.get("keep") is False:
            log(f"  · AI 判定不值得推: {item['title'][:46]}")
            continue

        direction = str(verdict.get("direction", "")).strip()
        if direction:
            item["ai"] = {
                "direction": direction,
                "confidence": str(verdict.get("confidence", "")).strip(),
                "horizon": str(verdict.get("horizon", "")).strip(),
                "reason": str(verdict.get("reason", "")).strip()[:80],
            }
        kept.append(item)
    return kept


SENTIMENT_PROMPT = """以下是比特币市场当前的客观指标, 数据来自交易所和公开指数。

{facts}

请据此输出一个 JSON 对象, 包含:

- "short_term": {{"lean": "偏多"或"偏空"或"中性", "reason": "一句话理由(40字内)"}}
  短期指 1-7 天。
- "mid_term":   {{"lean": "偏多"或"偏空"或"中性", "reason": "一句话理由(40字内)"}}
  中期指 1-3 个月。
- "conflict": 如果上面的指标之间存在互相矛盾的信号, 用一句话点出来(说明哪两个指标打架、
  意味着什么)。没有矛盾就填空字符串。

必须遵守:
- 结论必须能从上列指标推导出来, 不要引入外部假设或你对行情的记忆。
- 指标信号不明确时宁可给「中性」。
- 不要预测价格点位, 不要给买卖建议。
- 只输出 JSON, 不要任何其他文字。"""


def ai_interpret_sentiment(sent: dict, cfg: dict, state: dict) -> dict | None:
    """把客观指标汇总成短期/中期多空倾向。失败返回 None。"""
    if not ai_available(cfg) or not sent:
        return None
    prompt = SENTIMENT_PROMPT.format(facts=sentiment_facts(sent))
    text = _ai_raw(cfg, state, prompt, max_tokens=700)
    if not text:
        return None
    data = _extract_json(text, expect_list=False)
    if not isinstance(data, dict):
        log("  · AI 情绪解读返回无法解析")
        return None
    if not data.get("short_term") and not data.get("mid_term"):
        return None
    return data


# --------------------------------------------------------------------------
# 主流程
# --------------------------------------------------------------------------

def run_test(cfg: dict) -> int:
    """
    --test: 完整自检。逐项检查推送链路、市场情绪接口、AI 分析,
    结果同时打到日志和你的微信里, 哪一项挂了会直接写明原因。
    """
    log("=" * 56)
    log("自检模式: 推送链路 / 市场情绪 / AI 分析")
    log("=" * 56)

    state = load_state()
    checks: list[tuple[str, bool, str]] = []

    # ---- 1. 推送链路 ----
    log("[1/3] 检查 PushPlus 令牌 ...")
    has_token = bool(os.environ.get("PUSHPLUS_TOKEN", "").strip())
    checks.append(("PushPlus 令牌", has_token,
                   "已读到" if has_token else "没读到 PUSHPLUS_TOKEN 这个 Secret"))

    # ---- 2. 市场情绪 ----
    log("[2/3] 抓取市场情绪指标 ...")
    sentiment = collect_sentiment(cfg, state)
    core = [k for k in ("fng", "funding", "long_short", "etf_3d", "price")
            if sentiment.get(k) is not None]
    checks.append(("市场情绪接口", len(core) >= 3,
                   f"{len(core)}/5 项可用" if core else "全部抓取失败"))

    # ---- 3. AI ----
    log("[3/3] 测试 AI 分析 ...")
    lean = None
    if not cfg.get("ai", {}).get("enabled"):
        checks.append(("AI 分析", False, "config.yaml 里 ai.enabled 是 false"))
    elif not os.environ.get("ANTHROPIC_API_KEY", "").strip():
        checks.append(("AI 分析", False, "没读到 ANTHROPIC_API_KEY 这个 Secret"))
    else:
        lean = ai_interpret_sentiment(sentiment, cfg, state)
        checks.append(("AI 分析", lean is not None,
                       "正常" if lean else "调用失败, 原因见上面的日志"))

    # 把探测到的模型名和接口缓存留下来, 正式跑的时候就不用再探测了
    save_state(state)

    lines = ["## 自检结果", ""]
    for name, passed, note in checks:
        lines.append(f"{'通过' if passed else '**失败**'} · {name} · {note}")
    lines.append("")

    if sentiment:
        lines += ["### 实时指标", sentiment_summary_line(sentiment), ""]

    if lean:
        st, mt = lean.get("short_term") or {}, lean.get("mid_term") or {}
        lines.append("### AI 解读")
        if st.get("lean"):
            lines.append(f"短期(1-7天): **{st['lean']}** — {st.get('reason','')}")
        if mt.get("lean"):
            lines.append(f"中期(1-3个月): **{mt['lean']}** — {mt.get('reason','')}")
        if lean.get("conflict"):
            lines.append(f"矛盾信号: {lean['conflict']}")
        lines.append("")

    lines.append("---")
    lines.append("看到这条消息说明推送链路是通的。三项全通过就可以交给它自己跑了。")
    lines.append("如果只看到标题看不到正文, 在 PushPlus 公众号里发送「激活消息」。")

    ok_count = sum(1 for _, passed, _ in checks if passed)
    title = f"【雷达】自检 {ok_count}/{len(checks)} 项通过"
    ok = send_push(title, "\n".join(lines), cfg)
    log(f"自检完成: {ok_count}/{len(checks)} 项通过 | 推送{'成功' if ok else '失败'}")
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

    # 每日上限(先判断, 省得白调一次 AI)
    sent_today = bump_daily_counter(state)
    cap = int(cfg["push"].get("max_pushes_per_day", 15))
    if sent_today >= cap:
        log(f"今日推送已达上限 {cap} 条, 本轮不再推送")
        passed = []

    # AI 分析: 边缘条目去噪 + 利好利空标注(一次 API 调用同时完成这两件事)
    passed = ai_process_items(passed, cfg, state)

    max_items = int(cfg["push"].get("max_items_per_push", 5))
    push_list = passed[:max_items]

    # 只有真的要推送时, 才去抓市场情绪(省请求、省时间)
    sentiment: dict = {}
    lean: dict | None = None
    if push_list:
        # 情绪面板是"锦上添花", 绝不该因为它出问题就丢掉整条消息
        try:
            log("抓取市场情绪指标 ...")
            sentiment = collect_sentiment(cfg, state)
            if ai_available(cfg):
                lean = ai_interpret_sentiment(sentiment, cfg, state)
                if lean is None:
                    log("  · 情绪解读不可用, 本次只展示原始指标")
            else:
                log("  · 未配置 AI, 只展示原始指标(配置 ANTHROPIC_API_KEY 可开启解读)")
        except Exception as exc:
            log(f"  [!] 情绪模块异常({type(exc).__name__}), 跳过情绪面板继续推送")
            sentiment, lean = {}, None

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
        title, content = build_message(push_list, cfg, sent_today, sentiment, lean)
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
