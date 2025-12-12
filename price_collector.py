#!/usr/bin/env python3
"""
Price collector for Lighter and Variational exchanges.
Collects price data and calculates spreads, then writes to Victoria Metrics.
"""

import asyncio
import json
import time
import logging
import os
from typing import Dict, Optional
from datetime import datetime

import aiohttp
import websockets

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 禁用第三方库的DEBUG日志
logging.getLogger('websockets').setLevel(logging.WARNING)
logging.getLogger('websockets.client').setLevel(logging.WARNING)
logging.getLogger('websockets.protocol').setLevel(logging.WARNING)
logging.getLogger('aiohttp').setLevel(logging.WARNING)


class LighterPriceFetcher:
    """Fetcher for Lighter exchange prices using WebSocket."""

    def __init__(self, api_base_url: str):
        self.api_base_url = api_base_url
        self.ws_url = "wss://mainnet.zklighter.elliot.ai/stream"
        # Cache for market IDs - 预设已知的 market ID，避免启动时 API 调用
        self._market_id_cache = {
            'BTC': 1,  # BTC market ID
            'ETH': 0,  # ETH market ID
        }
        # Persistent WebSocket connection
        self._websocket = None
        self._connected = False
        self._price_cache = {}  # 缓存最新价格
        self._ws_task = None
        self._stop_event = asyncio.Event()

        # 价格校验相关
        self._validation_task = None
        self._validation_interval = 10  # 每10秒校验一次
        self._price_deviation_threshold = 0.05  # 价格偏离阈值：万分之五 (0.05%)
        self._symbols = []  # 存储需要监控的symbols
        self._last_validation_reconnect_time = 0  # 上次因校验失败而重连的时间
        self._validation_reconnect_cooldown = 60  # 校验重连冷却时间60秒

        # WebSocket连接速率限制（防止HTTP 429错误）
        # 使用线程锁而不是asyncio.Semaphore，因为订单线程会创建新的事件循环
        import threading
        self._ws_connection_lock = threading.Lock()  # 线程锁，确保同时只有1个临时连接
        self._ws_connection_min_interval = 2.0  # 两次连接之间最小间隔2秒
        self._last_ws_connection_time = 0  # 上次创建临时WebSocket连接的时间

        # 订单簿快照缓存（避免短时间内重复请求）
        self._orderbook_cache = {}  # {symbol: {'data': OrderBook, 'timestamp': float}}
        self._orderbook_cache_ttl = 2.0  # 缓存有效期2秒
        self._cache_lock = threading.Lock()  # 缓存访问锁

    async def _get_market_id(self, symbol: str) -> Optional[int]:
        """Get market ID for a symbol, with caching."""
        if symbol in self._market_id_cache:
            return self._market_id_cache[symbol]

        try:
            import lighter
            from lighter import ApiClient, Configuration

            async with ApiClient(configuration=Configuration(host=self.api_base_url)) as api_client:
                order_api = lighter.OrderApi(api_client)
                order_books = await order_api.order_books()

                for market in order_books.order_books:
                    if market.symbol == symbol.upper():
                        self._market_id_cache[symbol] = market.market_id
                        return market.market_id

                logger.error(f"Symbol {symbol} not found in Lighter markets")
                return None

        except Exception as e:
            logger.error(f"Error getting market ID for {symbol}: {e}")
            return None

    async def _maintain_websocket_connection(self, symbols: list):
        """维护持久的WebSocket连接，持续更新价格缓存"""
        # Get market IDs for all symbols
        market_ids = {}
        for symbol in symbols:
            market_id = await self._get_market_id(symbol)
            if market_id is not None:
                market_ids[symbol] = market_id

        if not market_ids:
            logger.error("No valid market IDs found for any symbol")
            return

        # Symbol to market_id reverse mapping
        market_id_to_symbol = {mid: sym for sym, mid in market_ids.items()}

        while not self._stop_event.is_set():
            try:
                logger.info(f"Connecting to Lighter WebSocket for persistent connection...")
                async with websockets.connect(self.ws_url, close_timeout=5) as websocket:
                    self._websocket = websocket
                    self._connected = True
                    logger.info("✅ Lighter WebSocket connected")

                    # Subscribe to order books for all symbols
                    for symbol, market_id in market_ids.items():
                        subscribe_msg = json.dumps({
                            "type": "subscribe",
                            "channel": f"order_book/{market_id}"
                        })
                        await websocket.send(subscribe_msg)
                        logger.info(f"Subscribed to {symbol} order book (market_id={market_id})")

                    # Continuously receive updates
                    while not self._stop_event.is_set():
                        try:
                            msg = await asyncio.wait_for(websocket.recv(), timeout=30)
                            data = json.loads(msg)
                            msg_type = data.get("type")

                            # Log all message types for debugging (only non-update messages)
                            if msg_type not in ["connected", "update/order_book"]:
                                logger.info(f"📥 Received WebSocket message: type={msg_type}, channel={data.get('channel', 'N/A')}")

                            # Skip "connected" message
                            if msg_type == "connected":
                                logger.debug("WebSocket connected message received")
                                continue

                            # Handle both initial subscription and updates
                            if msg_type in ["subscribed/order_book", "update/order_book"]:
                                # Parse market_id from channel field
                                channel = data.get("channel", "")
                                market_id = None
                                if ":" in channel:
                                    try:
                                        market_id = int(channel.split(":")[1])
                                    except (IndexError, ValueError):
                                        pass

                                if market_id is not None and market_id in market_id_to_symbol:
                                    symbol = market_id_to_symbol[market_id]
                                    order_book = data.get("order_book", {})
                                    bids = order_book.get("bids", [])
                                    asks = order_book.get("asks", [])

                                    if bids and asks:
                                        best_bid = float(bids[0].get('price', 0))
                                        best_ask = float(asks[0].get('price', 0))

                                        if best_bid > 0 and best_ask > 0:
                                            mid_price = (best_bid + best_ask) / 2
                                            # Only log if this is the first price or if msg_type is subscribed (not update)
                                            is_new = symbol not in self._price_cache
                                            self._price_cache[symbol] = mid_price
                                            if is_new or msg_type == "subscribed/order_book":
                                                logger.info(f"✅ {symbol} price initialized: {mid_price:.2f}")
                                            else:
                                                logger.debug(f"{symbol} price updated: {mid_price:.2f}")
                                    else:
                                        # Empty order book on updates is normal (incremental updates)
                                        logger.debug(f"{symbol}: Partial order book update - bids={len(bids)}, asks={len(asks)}")
                                else:
                                    logger.warning(f"⚠️ Unknown market_id {market_id} or not in subscribed markets, channel={channel}")
                            else:
                                # Log unknown message types for debugging
                                logger.info(f"⚠️ Unhandled message type: {msg_type}")

                        except asyncio.TimeoutError:
                            # Timeout is normal, just continue
                            logger.debug("WebSocket recv timeout, continuing...")
                            continue

            except Exception as e:
                error_msg = str(e)
                # 如果是 HTTP 429 错误，增加重连延迟
                if "429" in error_msg:
                    reconnect_delay = 15  # HTTP 429 需要更长的等待时间
                    logger.error(f"Lighter WebSocket HTTP 429 限流错误，等待 {reconnect_delay}秒后重连...")
                else:
                    reconnect_delay = 5
                    logger.error(f"Lighter WebSocket error: {e}, reconnecting in {reconnect_delay}s...")

                self._connected = False
                self._websocket = None
                await asyncio.sleep(reconnect_delay)

        self._connected = False
        self._websocket = None
        logger.info("Lighter WebSocket connection stopped")

    async def _price_validation_loop(self):
        """定期校验WebSocket价格与REST API价格的偏离"""
        logger.info(f"启动价格校验任务，每{self._validation_interval}秒检查一次")

        while not self._stop_event.is_set():
            try:
                await asyncio.sleep(self._validation_interval)

                if not self._symbols or not self._price_cache:
                    continue

                # 从REST API获取价格
                rest_prices = await self.fetch_prices_from_rest(self._symbols)

                # 对比WebSocket价格和REST API价格
                for symbol in self._symbols:
                    ws_price = self._price_cache.get(symbol)
                    rest_price = rest_prices.get(symbol)

                    # 如果有任何一个价格不可用，跳过
                    if ws_price is None or rest_price is None:
                        logger.debug(f"⏭️ {symbol} 跳过校验: WS={ws_price}, REST={rest_price}")
                        continue

                    # 计算价格偏离百分比
                    deviation_pct = abs(ws_price - rest_price) / rest_price * 100

                    logger.debug(
                        f"🔍 {symbol} 价格校验: WS={ws_price:.2f}, REST={rest_price:.2f}, "
                        f"偏离={deviation_pct:.4f}%"
                    )

                    # 检查是否超过偏离阈值
                    if deviation_pct > self._price_deviation_threshold:
                        current_time = time.time()

                        # 检查是否在冷却期内
                        if current_time - self._last_validation_reconnect_time < self._validation_reconnect_cooldown:
                            logger.warning(
                                f"⚠️ {symbol} WebSocket价格偏离REST价格 {deviation_pct:.4f}% "
                                f"(阈值={self._price_deviation_threshold}%)，"
                                f"但在校验重连冷却期内({int(current_time - self._last_validation_reconnect_time)}s/"
                                f"{self._validation_reconnect_cooldown}s)，跳过重连"
                            )

                            # 🚨 关键修复：虽然跳过重连，但必须清除错误的价格数据，防止系统使用错误数据进行交易
                            logger.warning(f"🗑️ 清除 {symbol} 的WebSocket价格缓存，防止使用错误数据")
                            self._price_cache.pop(symbol, None)
                        else:
                            logger.error(
                                f"🚨 {symbol} WebSocket价格偏离REST价格过大！"
                                f"WS={ws_price:.2f}, REST={rest_price:.2f}, "
                                f"偏离={deviation_pct:.4f}% > 阈值{self._price_deviation_threshold}%"
                            )
                            logger.error("WebSocket数据可能异常，触发重连...")

                            # 触发重连
                            await self.reconnect(self._symbols)
                            self._last_validation_reconnect_time = current_time
                            break  # 重连后退出本次循环

            except asyncio.CancelledError:
                logger.info("价格校验任务被取消")
                break
            except Exception as e:
                logger.error(f"价格校验任务异常: {e}", exc_info=True)
                await asyncio.sleep(5)  # 出错后等待5秒再继续

        logger.info("价格校验任务已停止")

    async def start(self, symbols: list):
        """启动持久WebSocket连接"""
        self._symbols = symbols  # 保存symbols用于校验任务

        if self._ws_task is None or self._ws_task.done():
            self._stop_event.clear()
            self._ws_task = asyncio.create_task(self._maintain_websocket_connection(symbols))
            logger.info("Started persistent Lighter WebSocket connection")

        # 价格校验任务已禁用（避免不必要的 REST API 调用）
        # if self._validation_task is None or self._validation_task.done():
        #     self._validation_task = asyncio.create_task(self._price_validation_loop())
        #     logger.info("Started price validation task")

    async def stop(self):
        """停止持久WebSocket连接"""
        self._stop_event.set()

        # 停止价格校验任务
        if self._validation_task and not self._validation_task.done():
            self._validation_task.cancel()
            try:
                await self._validation_task
            except asyncio.CancelledError:
                pass

        # 停止WebSocket任务
        if self._ws_task:
            await self._ws_task

        logger.info("Stopped Lighter WebSocket connection")

    async def reconnect(self, symbols: list):
        """重连WebSocket（用于异常价差检测时）"""
        logger.warning("🔄 检测到异常价差，正在重连Lighter WebSocket...")

        # 停止当前连接
        await self.stop()

        # 清空价格缓存
        self._price_cache.clear()
        logger.info("已清空价格缓存")

        # 重新启动连接
        await self.start(symbols)

        # 等待连接建立
        await asyncio.sleep(3)
        logger.info("✅ Lighter WebSocket已重连")

    async def fetch_prices_from_rest(self, symbols: list) -> Dict[str, Optional[float]]:
        """
        从REST API获取价格（用于价格校验）
        使用 Lighter SDK 的 REST API (order_books) 而不是 WebSocket

        实现策略：
        1. 优先使用 WebSocket 缓存的价格（如果可用且新鲜）
        2. 如果缓存不可用，则使用 REST API
        3. REST API 调用有速率限制保护（最小 2 秒间隔）

        Args:
            symbols: List of symbols to fetch (e.g., ['BTC', 'ETH'])

        Returns:
            Dict mapping symbol to mid price from order book
        """
        import time

        prices = {}
        symbols_need_rest = []  # 需要从 REST API 获取的 symbols

        logger.debug(f"REST校验: 开始获取价格 symbols={symbols}")

        # 1. 首先尝试从 WebSocket 缓存获取价格
        for symbol in symbols:
            cached_price = self._price_cache.get(symbol)
            if cached_price is not None:
                prices[symbol] = cached_price
                logger.debug(f"REST校验: {symbol} 使用 WebSocket 缓存价格={cached_price:.2f}")
            else:
                symbols_need_rest.append(symbol)

        # 2. 如果所有价格都从缓存获取到了，直接返回
        if not symbols_need_rest:
            logger.debug(f"REST校验: 所有价格均从 WebSocket 缓存获取，无需调用 REST API")
            return prices

        # 3. 对于缓存未命中的 symbols，使用 REST API 获取
        logger.debug(f"REST校验: {len(symbols_need_rest)} 个 symbols 需要从 REST API 获取: {symbols_need_rest}")

        # 速率限制：确保 REST API 调用间隔至少 2 秒
        with self._cache_lock:
            current_time = time.time()
            if not hasattr(self, '_last_rest_api_call_time'):
                self._last_rest_api_call_time = 0

            time_since_last_call = current_time - self._last_rest_api_call_time
            min_interval = 2.0  # 最小 2 秒间隔

            if time_since_last_call < min_interval:
                wait_time = min_interval - time_since_last_call
                logger.debug(f"REST校验: 速率限制等待 {wait_time:.2f}秒")
                await asyncio.sleep(wait_time)

            self._last_rest_api_call_time = time.time()

        try:
            import aiohttp

            # 直接使用 HTTP GET 请求获取订单簿
            url = f"{self.api_base_url}/api/v1/order_books"
            logger.info(f"REST校验: 直接调用 HTTP GET {url}")

            timeout = aiohttp.ClientTimeout(total=10)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url) as response:
                    if response.status != 200:
                        logger.error(f"REST校验: HTTP 请求失败，状态码={response.status}")
                        raise Exception(f"HTTP {response.status}")

                    data = await response.json()
                    logger.info(f"REST校验: 成功获取订单簿数据")

                    # 解析响应数据
                    order_books = data.get('order_books', [])
                    logger.info(f"REST校验: 共 {len(order_books)} 个市场")

                    # 为每个需要的 symbol 查找对应的订单簿
                    for symbol in symbols_need_rest:
                        symbol_upper = symbol.upper()
                        found = False

                        for market in order_books:
                            if market.get('symbol') == symbol_upper:
                                # 找到匹配的市场，计算中间价
                                bids = market.get('bids', [])
                                asks = market.get('asks', [])

                                logger.info(f"REST校验: {symbol} 订单簿 bids={len(bids)}, asks={len(asks)}")

                                if bids and asks:
                                    # 获取最佳买价和卖价
                                    best_bid = float(bids[0].get('price', 0))
                                    best_ask = float(asks[0].get('price', 0))

                                    if best_bid > 0 and best_ask > 0:
                                        mid_price = (best_bid + best_ask) / 2
                                        prices[symbol] = mid_price
                                        logger.info(f"✅ REST校验: {symbol} 价格={mid_price:.2f} (bid={best_bid:.2f}, ask={best_ask:.2f})")
                                        found = True
                                        break
                                else:
                                    logger.warning(f"REST校验: {symbol} 订单簿为空")
                                    prices[symbol] = None
                                    found = True
                                    break

                        if not found:
                            logger.warning(f"REST校验: 未找到 {symbol} 的市场数据")
                            prices[symbol] = None

                    logger.info(f"REST校验: 成功从 REST API 获取价格")

        except Exception as e:
            logger.error(f"REST校验: 从 REST API 获取价格失败: {e}")
            # 如果 REST API 失败，尝试使用 WebSocket 缓存作为降级方案
            logger.info(f"REST校验: REST API 失败，尝试使用 WebSocket 缓存作为降级方案")
            for symbol in symbols_need_rest:
                if symbol not in prices:
                    cached_price = self._price_cache.get(symbol)
                    if cached_price is not None:
                        prices[symbol] = cached_price
                        logger.info(f"REST校验: {symbol} 降级使用 WebSocket 缓存价格={cached_price:.2f}")
                    else:
                        prices[symbol] = None

        logger.debug(f"REST校验: 返回价格 prices={prices}")
        return prices

    async def fetch_prices(self, symbols: list) -> Dict[str, Optional[float]]:
        """
        从缓存获取价格（由持久WebSocket连接维护）
        如果缓存为空或不完整，降级使用 REST API

        Args:
            symbols: List of symbols to fetch (e.g., ['BTC', 'ETH'])

        Returns:
            Dict mapping symbol to mid price from order book
        """
        prices = {symbol: self._price_cache.get(symbol) for symbol in symbols}

        # 检查是否有缺失的价格
        missing_symbols = [symbol for symbol, price in prices.items() if price is None]

        # 如果有缺失的价格，记录警告但不降级
        # 注意：暂时禁用 REST API 降级，因为端点不可用
        # 系统将依赖 WebSocket 持久连接提供价格
        if missing_symbols:
            logger.warning(f"WebSocket 缓存缺失 {len(missing_symbols)} 个价格: {missing_symbols}，等待 WebSocket 连接建立...")
            # 不再调用 REST API 降级，因为端点返回 404

        if prices:
            logger.info(f"Lighter prices: {prices}")

        return prices

    async def fetch_order_book_depth(self, symbol: str) -> Dict[str, Optional[Dict[str, float]]]:
        """
        Fetch order book depth (best bid/ask with quantities) for a symbol using Lighter SDK.

        注意：添加了缓存机制和速率限制以防止HTTP 429错误

        Args:
            symbol: Symbol to fetch (e.g., 'BTC', 'ETH')

        Returns:
            Dict with 'best_bid' and 'best_ask' containing {'price': float, 'quantity': float}
            Returns None for each if data is unavailable
        """
        # 检查缓存（使用线程锁保护）
        import time
        current_time = time.time()

        with self._cache_lock:
            if symbol in self._orderbook_cache:
                cache_entry = self._orderbook_cache[symbol]
                cache_age = current_time - cache_entry['timestamp']
                if cache_age < self._orderbook_cache_ttl:
                    logger.debug(f"使用缓存的订单簿数据: {symbol} (缓存年龄: {cache_age:.2f}秒)")
                    return cache_entry['data']

        result = {
            'best_bid': None,
            'best_ask': None
        }

        # 使用线程锁限制并发连接（跨事件循环安全）
        # 注意：只在检查/更新时间戳时持有锁，不要在异步操作时持有锁
        # 1. 检查是否需要等待（持有锁）
        with self._ws_connection_lock:
            time_since_last_connection = current_time - self._last_ws_connection_time
            need_wait = time_since_last_connection < self._ws_connection_min_interval
            if need_wait:
                wait_time = self._ws_connection_min_interval - time_since_last_connection
            else:
                wait_time = 0

        # 2. 如果需要等待，在锁外等待（不阻塞其他线程）
        if need_wait:
            logger.debug(f"速率限制：等待 {wait_time:.2f}秒 以避免HTTP 429错误")
            await asyncio.sleep(wait_time)

        # 3. 更新时间戳（持有锁）
        with self._ws_connection_lock:
            self._last_ws_connection_time = time.time()

        # 4. 使用 HTTP REST API 获取订单簿（不持有锁）
        try:
            import aiohttp

            # 直接使用 HTTP GET 请求获取订单簿
            url = f"{self.api_base_url}/api/v1/order_books"
            logger.debug(f"订单簿深度: 直接调用 HTTP GET {url}")

            timeout = aiohttp.ClientTimeout(total=10)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url) as response:
                    if response.status != 200:
                        logger.error(f"订单簿深度: HTTP 请求失败，状态码={response.status}")
                        return result

                    data = await response.json()
                    order_books = data.get('order_books', [])

                    # 查找对应的市场
                    symbol_upper = symbol.upper()
                    for market in order_books:
                        if market.get('symbol') == symbol_upper:
                            bids = market.get('bids', [])
                            asks = market.get('asks', [])

                            logger.debug(f"订单簿深度: {symbol} bids={len(bids)}, asks={len(asks)}")

                            if bids and len(bids) > 0:
                                best_bid = bids[0]
                                result['best_bid'] = {
                                    'price': float(best_bid.get('price', 0)),
                                    'quantity': float(best_bid.get('size', 0))
                                }
                                logger.debug(f"订单簿深度: {symbol} Best bid: {result['best_bid']}")

                            if asks and len(asks) > 0:
                                best_ask = asks[0]
                                result['best_ask'] = {
                                    'price': float(best_ask.get('price', 0)),
                                    'quantity': float(best_ask.get('size', 0))
                                }
                                logger.debug(f"订单簿深度: {symbol} Best ask: {result['best_ask']}")
                            break  # 找到对应的市场，退出循环

            # 缓存订单簿数据（使用线程锁保护）
            with self._cache_lock:
                self._orderbook_cache[symbol] = {
                    'data': result,
                    'timestamp': time.time()
                }

        except Exception as e:
            logger.error(f"Error fetching Lighter order book for {symbol}: {e}", exc_info=True)

        return result


class VariationalPriceFetcher:
    """Fetcher for Variational exchange prices using Chrome DevTools Protocol."""

    def __init__(self, chrome_debugger_url: str, api_base_url: str):
        self.chrome_debugger_url = chrome_debugger_url
        self.api_base_url = api_base_url
        self.quotes_url = f"{api_base_url}/api/quotes/indicative"
        self._cached_ws_url: Optional[str] = None  # Cache WebSocket URL
        self._ws_url_last_error_time: float = 0  # Track last error time

    async def get_websocket_debugger_url(self, force_refresh: bool = False) -> Optional[str]:
        """Get the WebSocket debugger URL from Chrome."""
        # Return cached URL if available and not forcing refresh
        if self._cached_ws_url and not force_refresh:
            logger.debug(f"Using cached WebSocket URL")
            return self._cached_ws_url

        try:
            logger.debug(f"Connecting to Chrome debugger at {self.chrome_debugger_url}")
            timeout = aiohttp.ClientTimeout(total=5)  # 增加到5秒超时
            async with aiohttp.ClientSession(timeout=timeout) as session:
                # First, try to get the list of tabs/pages
                async with session.get(f"{self.chrome_debugger_url}/json") as response:
                    if response.status != 200:
                        logger.error(f"Chrome debugger not available at {self.chrome_debugger_url}, status: {response.status}")
                        return None
                    tabs = await response.json()
                    logger.debug(f"Found {len(tabs)} Chrome tabs")

                    # Look for a tab with variational.io
                    for tab in tabs:
                        url = tab.get('url', '')
                        if 'variational.io' in url:
                            ws_url = tab.get('webSocketDebuggerUrl')
                            logger.debug(f"Found Variational tab: {url}")
                            logger.debug(f"WebSocket URL: {ws_url}")
                            self._cached_ws_url = ws_url  # Cache the URL
                            return ws_url

                    # If no variational tab found, use the first page
                    for tab in tabs:
                        if tab.get('type') == 'page':
                            ws_url = tab.get('webSocketDebuggerUrl')
                            logger.warning(f"Variational tab not found, using first page: {tab.get('url')}")
                            logger.debug(f"WebSocket URL: {ws_url}")
                            self._cached_ws_url = ws_url  # Cache the URL
                            return ws_url

                    # Fallback to browser-level connection
                    logger.warning("No suitable tab found, trying browser connection")
                    async with session.get(f"{self.chrome_debugger_url}/json/version") as response2:
                        if response2.status != 200:
                            return None
                        data = await response2.json()
                        ws_url = data.get('webSocketDebuggerUrl')
                        logger.debug(f"Browser WebSocket URL: {ws_url}")
                        self._cached_ws_url = ws_url  # Cache the URL
                        return ws_url

        except asyncio.TimeoutError:
            # 限制错误日志频率，避免日志刷屏
            current_time = time.time()
            if current_time - self._ws_url_last_error_time > 10:  # 每10秒最多记录一次
                logger.warning(f"Chrome debugger connection timeout (will retry)")
                self._ws_url_last_error_time = current_time
            return None
        except Exception as e:
            current_time = time.time()
            if current_time - self._ws_url_last_error_time > 10:
                logger.error(f"Error connecting to Chrome debugger: {e}")
                self._ws_url_last_error_time = current_time
            return None

    async def execute_fetch_quote(self, ws_url: str, symbol: str) -> Optional[float]:
        """Execute fetch request for a single symbol via Chrome DevTools Protocol."""
        try:
            logger.debug(f"Fetching {symbol} quote via Chrome DevTools")

            # Temporarily disable proxy for local Chrome connection
            proxy_vars = [
                'HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy',
                'ALL_PROXY', 'all_proxy', 'SOCKS_PROXY', 'socks_proxy',
                'NO_PROXY', 'no_proxy'
            ]
            saved_proxy_env = {key: os.environ.get(key) for key in proxy_vars}

            try:
                # Remove all proxy settings for local connection
                for key in proxy_vars:
                    os.environ.pop(key, None)

                async with websockets.connect(ws_url) as websocket:
                    # Generate unique IDs for this symbol's request
                    enable_id = int(time.time() * 1000) % 1000000
                    eval_id = enable_id + 1

                    # Enable Runtime domain
                    await websocket.send(json.dumps({
                        "id": enable_id,
                        "method": "Runtime.enable"
                    }))

                    # Wait for Runtime.enable response
                    while True:
                        response = await asyncio.wait_for(websocket.recv(), timeout=5)
                        data = json.loads(response)
                        if 'id' in data and data['id'] == enable_id:
                            logger.debug(f"Runtime.enable confirmed for {symbol}")
                            break

                    # Execute fetch command
                    fetch_script = f'''
                    fetch("{self.quotes_url}", {{
                        "headers": {{
                            "accept": "*/*",
                            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
                            "content-type": "application/json",
                            "sec-fetch-dest": "empty",
                            "sec-fetch-mode": "cors",
                            "sec-fetch-site": "same-origin"
                        }},
                        "body": JSON.stringify({{
                            "instrument": {{
                                "underlying": "{symbol.upper()}",
                                "funding_interval_s": 3600,
                                "settlement_asset": "USDC",
                                "instrument_type": "perpetual_future"
                            }},
                            "qty": "0.0001"
                        }}),
                        "method": "POST",
                        "mode": "cors",
                        "credentials": "include"
                    }}).then(res => res.json())
                    '''

                    logger.debug(f"Executing fetch for {symbol}")

                    await websocket.send(json.dumps({
                        "id": eval_id,
                        "method": "Runtime.evaluate",
                        "params": {
                            "expression": fetch_script,
                            "awaitPromise": True,
                            "returnByValue": True
                        }
                    }))

                    # Wait for response
                    max_attempts = 50
                    attempts = 0
                    while attempts < max_attempts:
                        response = await asyncio.wait_for(websocket.recv(), timeout=5)
                        data = json.loads(response)
                        attempts += 1

                        # Check if this is an event (no 'id') or a response
                        if 'id' not in data:
                            continue

                        # Check if this is the response to our command
                        if data['id'] != eval_id:
                            continue

                        # This is our response!
                        if 'result' in data and 'result' in data['result']:
                            result_value = data['result']['result'].get('value')
                            if result_value and isinstance(result_value, dict):
                                bid_str = result_value.get('bid')
                                ask_str = result_value.get('ask')

                                if bid_str and ask_str:
                                    bid = float(bid_str)
                                    ask = float(ask_str)
                                    mid_price = (bid + ask) / 2
                                    logger.debug(f"{symbol}: bid={bid}, ask={ask}, mid={mid_price}")
                                    return mid_price
                                else:
                                    logger.error(f"Missing bid/ask in response for {symbol}")
                                    return None
                            else:
                                logger.error(f"Invalid result value for {symbol}: {result_value}")
                                return None
                        elif 'result' in data and 'exceptionDetails' in data['result']:
                            exception = data['result']['exceptionDetails']
                            logger.error(f"JavaScript exception for {symbol}: {exception}")
                            return None

                        logger.error(f"Unexpected response for {symbol}")
                        return None

                    logger.error(f"Timeout: No response for {symbol} after {max_attempts} attempts")
                    return None

            finally:
                # Restore proxy settings
                for key, value in saved_proxy_env.items():
                    if value is not None:
                        os.environ[key] = value

        except asyncio.TimeoutError:
            logger.error(f"Timeout fetching {symbol} quote")
            return None
        except Exception as e:
            logger.error(f"Error fetching {symbol} quote: {e}", exc_info=True)
            return None

    async def execute_fetch_quote_batch(self, ws_url: str, symbols: list) -> Dict[str, Optional[float]]:
        """Execute fetch requests for multiple symbols concurrently via Chrome DevTools Protocol."""
        prices = {symbol: None for symbol in symbols}

        # Temporarily disable proxy for local Chrome connection
        proxy_vars = [
            'HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy',
            'ALL_PROXY', 'all_proxy', 'SOCKS_PROXY', 'socks_proxy',
            'NO_PROXY', 'no_proxy'
        ]
        saved_proxy_env = {key: os.environ.get(key) for key in proxy_vars}

        try:
            # Remove all proxy settings for local connection
            for key in proxy_vars:
                os.environ.pop(key, None)

            async with websockets.connect(ws_url, close_timeout=3) as websocket:
                # Generate unique IDs for each symbol
                base_id = int(time.time() * 1000) % 1000000
                enable_id = base_id
                symbol_ids = {symbol: base_id + 1 + i for i, symbol in enumerate(symbols)}

                # Enable Runtime domain
                await websocket.send(json.dumps({
                    "id": enable_id,
                    "method": "Runtime.enable"
                }))

                # Wait for Runtime.enable response
                while True:
                    response = await asyncio.wait_for(websocket.recv(), timeout=3)
                    data = json.loads(response)
                    if 'id' in data and data['id'] == enable_id:
                        logger.debug("Runtime.enable confirmed")
                        break

                # Send all fetch commands concurrently
                for symbol in symbols:
                    fetch_script = f'''
                    fetch("{self.quotes_url}", {{
                        "headers": {{
                            "accept": "*/*",
                            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
                            "content-type": "application/json",
                            "sec-fetch-dest": "empty",
                            "sec-fetch-mode": "cors",
                            "sec-fetch-site": "same-origin"
                        }},
                        "body": JSON.stringify({{
                            "instrument": {{
                                "underlying": "{symbol.upper()}",
                                "funding_interval_s": 3600,
                                "settlement_asset": "USDC",
                                "instrument_type": "perpetual_future"
                            }},
                            "qty": "0.0001"
                        }}),
                        "method": "POST",
                        "mode": "cors",
                        "credentials": "include"
                    }}).then(res => res.json())
                    '''

                    await websocket.send(json.dumps({
                        "id": symbol_ids[symbol],
                        "method": "Runtime.evaluate",
                        "params": {
                            "expression": fetch_script,
                            "awaitPromise": True,
                            "returnByValue": True
                        }
                    }))

                # Collect responses
                pending_symbols = set(symbols)
                max_attempts = 100
                attempts = 0

                while pending_symbols and attempts < max_attempts:
                    try:
                        response = await asyncio.wait_for(websocket.recv(), timeout=3)
                        data = json.loads(response)
                        attempts += 1

                        # Check if this is an event (no 'id') or a response
                        if 'id' not in data:
                            continue

                        # Find which symbol this response is for
                        response_id = data['id']
                        matching_symbol = None
                        for symbol, sid in symbol_ids.items():
                            if sid == response_id:
                                matching_symbol = symbol
                                break

                        if not matching_symbol:
                            continue

                        # Parse the response
                        if 'result' in data and 'result' in data['result']:
                            result_value = data['result']['result'].get('value')
                            if result_value and isinstance(result_value, dict):
                                bid_str = result_value.get('bid')
                                ask_str = result_value.get('ask')

                                if bid_str and ask_str:
                                    bid = float(bid_str)
                                    ask = float(ask_str)
                                    mid_price = (bid + ask) / 2
                                    prices[matching_symbol] = mid_price
                                    logger.debug(f"{matching_symbol}: bid={bid}, ask={ask}, mid={mid_price}")
                                    pending_symbols.discard(matching_symbol)
                                else:
                                    logger.error(f"Missing bid/ask in response for {matching_symbol}")
                                    pending_symbols.discard(matching_symbol)
                            else:
                                logger.error(f"Invalid result value for {matching_symbol}: {result_value}")
                                pending_symbols.discard(matching_symbol)
                        elif 'result' in data and 'exceptionDetails' in data['result']:
                            exception = data['result']['exceptionDetails']
                            logger.error(f"JavaScript exception for {matching_symbol}: {exception}")
                            pending_symbols.discard(matching_symbol)
                        else:
                            # Unexpected response format
                            logger.debug(f"Unexpected response format for {matching_symbol}")
                            pending_symbols.discard(matching_symbol)

                    except asyncio.TimeoutError:
                        logger.error(f"Timeout waiting for responses, pending: {pending_symbols}")
                        break

                if pending_symbols:
                    logger.error(f"Failed to fetch prices for: {pending_symbols}")

        except asyncio.TimeoutError:
            logger.error(f"Timeout in batch fetch")
        except Exception as e:
            logger.error(f"Error in batch fetch: {e}", exc_info=True)
        finally:
            # Restore proxy settings
            for key, value in saved_proxy_env.items():
                if value is not None:
                    os.environ[key] = value

        return prices

    async def fetch_prices(self, symbols: list) -> Dict[str, Optional[float]]:
        """
        Fetch prices for given symbols from Variational.

        Args:
            symbols: List of symbols to fetch (e.g., ['BTC', 'ETH'])

        Returns:
            Dict mapping symbol to price (mid price from bid/ask)
        """
        prices = {symbol: None for symbol in symbols}
        max_retries = 2

        for attempt in range(max_retries):
            try:
                logger.debug(f"Starting Variational price fetch for {symbols} (attempt {attempt + 1}/{max_retries})")

                # Get WebSocket URL (use cache on first attempt, force refresh on retry)
                force_refresh = (attempt > 0)
                ws_url = await self.get_websocket_debugger_url(force_refresh=force_refresh)
                if not ws_url:
                    if attempt < max_retries - 1:
                        logger.debug("No WebSocket URL, retrying...")
                        continue
                    logger.warning("Could not get Chrome WebSocket debugger URL after retries")
                    return prices

                # Use batch fetch for better performance
                prices = await self.execute_fetch_quote_batch(ws_url, symbols)

                # Check if we got valid prices
                if any(price is not None for price in prices.values()):
                    logger.info(f"Variational prices: {prices}")
                    return prices
                elif attempt < max_retries - 1:
                    logger.debug("Got no prices, invalidating cache and retrying...")
                    self._cached_ws_url = None  # Invalidate cache for retry
                    continue

            except (websockets.exceptions.WebSocketException, ConnectionError) as e:
                logger.debug(f"WebSocket connection error (attempt {attempt + 1}): {e}")
                self._cached_ws_url = None  # Invalidate cache on connection error
                if attempt < max_retries - 1:
                    continue
                logger.warning(f"WebSocket connection failed after {max_retries} attempts")
            except Exception as e:
                logger.error(f"Error fetching Variational prices: {e}")
                if attempt < max_retries - 1:
                    self._cached_ws_url = None
                    continue

        return prices


class VictoriaMetricsWriter:
    """Writer for Victoria Metrics."""

    def __init__(self, vm_url: str, write_endpoint: str):
        self.write_url = f"{vm_url}{write_endpoint}"

    async def write_spread(self, symbol: str, lighter_price: Optional[float],
                          variational_price: Optional[float], timestamp: int):
        """
        Write price spread data to Victoria Metrics.

        Args:
            symbol: Trading symbol
            lighter_price: Price from Lighter
            variational_price: Price from Variational
            timestamp: Unix timestamp in milliseconds
        """
        if lighter_price is None or variational_price is None:
            logger.warning(f"Skipping {symbol} - missing price data")
            return

        # Calculate spread (absolute and percentage)
        spread_abs = variational_price - lighter_price
        spread_pct = (spread_abs / lighter_price * 100) if lighter_price > 0 else 0

        # Create Prometheus format metrics
        metrics = f'''# HELP exchange_price Price from exchange
# TYPE exchange_price gauge
exchange_price{{symbol="{symbol}",exchange="lighter"}} {lighter_price} {timestamp}
exchange_price{{symbol="{symbol}",exchange="variational"}} {variational_price} {timestamp}

# HELP price_spread_abs Absolute price spread between exchanges
# TYPE price_spread_abs gauge
price_spread_abs{{symbol="{symbol}",from="variational",to="lighter"}} {spread_abs} {timestamp}

# HELP price_spread_pct Percentage price spread between exchanges
# TYPE price_spread_pct gauge
price_spread_pct{{symbol="{symbol}",from="variational",to="lighter"}} {spread_pct} {timestamp}
'''

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(self.write_url, data=metrics) as response:
                    if response.status not in [200, 204]:
                        logger.error(f"Victoria Metrics write failed: {response.status}")
                    else:
                        logger.info(f"Wrote {symbol} spread: {spread_abs:.4f} ({spread_pct:.2f}%)")
        except Exception as e:
            logger.error(f"Error writing to Victoria Metrics: {e}")


async def collect_and_write(config: dict):
    """Main collection loop."""
    symbols = config['symbols']

    lighter_fetcher = LighterPriceFetcher(config['lighter']['api_base_url'])
    variational_fetcher = VariationalPriceFetcher(
        config['variational']['chrome_debugger_url'],
        config['variational']['api_base_url']
    )
    vm_writer = VictoriaMetricsWriter(
        config['victoria_metrics']['url'],
        config['victoria_metrics']['write_endpoint']
    )

    logger.info(f"Starting price collection for symbols: {symbols}")
    logger.info(f"Collection interval: {config['collection_interval_seconds']}s")

    while True:
        loop_start = time.time()

        try:
            # Fetch prices from both exchanges concurrently
            lighter_prices, variational_prices = await asyncio.gather(
                lighter_fetcher.fetch_prices(symbols),
                variational_fetcher.fetch_prices(symbols)
            )

            # Write to Victoria Metrics
            timestamp = int(time.time() * 1000)  # milliseconds

            for symbol in symbols:
                await vm_writer.write_spread(
                    symbol,
                    lighter_prices.get(symbol),
                    variational_prices.get(symbol),
                    timestamp
                )

        except Exception as e:
            logger.error(f"Error in collection loop: {e}")

        # Calculate actual sleep time to maintain fixed interval
        elapsed = time.time() - loop_start
        sleep_time = max(0, config['collection_interval_seconds'] - elapsed)

        if sleep_time > 0:
            logger.debug(f"Loop took {elapsed:.2f}s, sleeping for {sleep_time:.2f}s")
            await asyncio.sleep(sleep_time)
        else:
            logger.warning(f"Loop took {elapsed:.2f}s, longer than configured interval of {config['collection_interval_seconds']}s")


async def main():
    """Main entry point."""
    # Load configuration
    with open('config.json', 'r') as f:
        config = json.load(f)

    logger.info("Price Collector started")
    logger.info(f"Configuration: {config}")

    await collect_and_write(config)


if __name__ == '__main__':
    asyncio.run(main())
