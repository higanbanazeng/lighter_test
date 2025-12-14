#!/usr/bin/env python3
"""
测试 Lighter REST API 获取价格的延迟
独立运行版本，不依赖其他模块
"""

import asyncio
import os
import time
import statistics
import logging
from typing import Dict, Optional
import aiohttp

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Lighter API 配置
DEFAULT_LIGHTER_API_URL = "https://mainnet.zklighter.elliot.ai"


class LighterPriceFetcher:
    """简化的 Lighter 价格获取器（仅 REST API）"""

    def __init__(self, api_base_url: str):
        self.api_base_url = api_base_url
        # 预设已知的 market ID，避免启动时 API 调用
        self._market_id_cache = {
            'BTC': 1,  # BTC market ID
            'ETH': 0,  # ETH market ID
        }
        self._last_rest_api_call_time = 0

    async def fetch_prices_from_rest(self, symbols: list) -> tuple[Dict[str, Optional[float]], float, float]:
        """
        从 REST API 获取价格

        Args:
            symbols: List of symbols to fetch (e.g., ['BTC', 'ETH'])

        Returns:
            Tuple of (prices_dict, wait_time_ms, api_time_ms)
            - prices_dict: Dict mapping symbol to mid price from order book
            - wait_time_ms: Rate limit wait time in milliseconds
            - api_time_ms: Actual API call time in milliseconds
        """
        prices = {}

        logger.debug(f"开始获取价格: symbols={symbols}")

        # 速率限制：确保 REST API 调用间隔至少 2 秒
        wait_start = time.time()
        current_time = time.time()
        time_since_last_call = current_time - self._last_rest_api_call_time
        min_interval = 2.0  # 最小 2 秒间隔

        if time_since_last_call < min_interval:
            wait_time = min_interval - time_since_last_call
            logger.debug(f"速率限制等待 {wait_time:.2f}秒")
            await asyncio.sleep(wait_time)

        wait_end = time.time()
        wait_time_ms = (wait_end - wait_start) * 1000

        # 记录实际 API 调用开始时间
        api_start = time.time()
        self._last_rest_api_call_time = api_start

        try:
            # 使用 market_id 缓存，直接获取特定市场订单簿
            timeout = aiohttp.ClientTimeout(total=10)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                for symbol in symbols:
                    # 从缓存获取 market_id
                    market_id = self._market_id_cache.get(symbol)

                    if market_id is None:
                        # 缓存未命中，需要先获取 market_id
                        logger.info(f"{symbol} market_id 缓存未命中，获取市场列表...")
                        url = f"{self.api_base_url}/api/v1/orderBooks"

                        async with session.get(url) as response:
                            if response.status != 200:
                                logger.error(f"获取市场列表失败，状态码={response.status}")
                                prices[symbol] = None
                                continue

                            data = await response.json()
                            order_books = data.get('order_books', [])

                            # 查找并缓存 market_id
                            symbol_upper = symbol.upper()
                            for market in order_books:
                                if market.get('symbol') == symbol_upper:
                                    market_id = market.get('market_id')
                                    self._market_id_cache[symbol] = market_id
                                    logger.info(f"✅ 已缓存 {symbol} market_id={market_id}")
                                    break

                            if market_id is None:
                                logger.error(f"未找到 {symbol} 的 market_id")
                                prices[symbol] = None
                                continue

                    # 使用 market_id 直接获取订单簿
                    orderbook_url = f"{self.api_base_url}/api/v1/orderBookOrders?market_id={market_id}&limit=1"
                    logger.debug(f"获取 {symbol} 订单簿: {orderbook_url}")

                    async with session.get(orderbook_url) as response:
                        if response.status != 200:
                            logger.error(f"获取 {symbol} 订单簿失败，状态码={response.status}")
                            prices[symbol] = None
                            continue

                        data = await response.json()
                        bids = data.get('bids', [])
                        asks = data.get('asks', [])

                        if bids and asks and len(bids) > 0 and len(asks) > 0:
                            best_bid = float(bids[0].get('price', 0))
                            best_ask = float(asks[0].get('price', 0))

                            if best_bid > 0 and best_ask > 0:
                                mid_price = (best_bid + best_ask) / 2
                                prices[symbol] = mid_price
                                logger.debug(f"✅ {symbol} 价格={mid_price:.2f} (bid={best_bid:.2f}, ask={best_ask:.2f})")
                            else:
                                logger.warning(f"{symbol} 价格无效 (bid={best_bid}, ask={best_ask})")
                                prices[symbol] = None
                        else:
                            logger.warning(f"{symbol} 订单簿为空 (bids={len(bids)}, asks={len(asks)})")
                            prices[symbol] = None

            logger.debug(f"完成价格获取: prices={prices}")

        except Exception as e:
            logger.error(f"从 REST API 获取价格失败: {e}")
            for symbol in symbols:
                if symbol not in prices:
                    prices[symbol] = None

        # 记录实际 API 调用结束时间
        api_end = time.time()
        api_time_ms = (api_end - api_start) * 1000

        return prices, wait_time_ms, api_time_ms


async def test_lighter_rest_latency(symbol='BTC', test_count=10):
    """
    测试 Lighter REST API 获取价格的延迟

    Args:
        symbol: 要测试的币种
        test_count: 测试次数
    """
    print("=" * 70)
    print(f"Lighter REST API 延迟测试 - {symbol}")
    print("=" * 70)
    print()

    # 从环境变量读取配置，如果没有则使用默认值
    api_base_url = os.getenv('LIGHTER_API_URL', DEFAULT_LIGHTER_API_URL)
    print(f"API Base URL: {api_base_url}")
    print(f"测试币种: {symbol}")
    print(f"测试次数: {test_count}")
    print()

    # 创建 Lighter 价格获取器
    fetcher = LighterPriceFetcher(api_base_url)

    wait_times = []
    api_times = []
    total_times = []
    prices = []

    print("开始测试...")
    print("-" * 70)

    for i in range(test_count):
        try:
            # 记录开始时间（毫秒）
            start_time = time.time()

            # 调用 REST API 获取价格（现在返回三个值）
            result, wait_time_ms, api_time_ms = await fetcher.fetch_prices_from_rest([symbol])

            # 记录结束时间（毫秒）
            end_time = time.time()

            # 计算总延迟（毫秒）
            total_time_ms = (end_time - start_time) * 1000

            price = result.get(symbol)

            if price is not None:
                wait_times.append(wait_time_ms)
                api_times.append(api_time_ms)
                total_times.append(total_time_ms)
                prices.append(price)
                print(f"测试 #{i+1:2d}: 价格={price:>10.2f} | 等待={wait_time_ms:>7.2f}ms | API={api_time_ms:>7.2f}ms | 总计={total_time_ms:>7.2f}ms ✓")
            else:
                print(f"测试 #{i+1:2d}: 获取价格失败 ✗")

            # 避免请求过快，稍微等待
            if i < test_count - 1:
                await asyncio.sleep(0.5)

        except Exception as e:
            print(f"测试 #{i+1:2d}: 异常 - {e} ✗")

    print("-" * 70)
    print()

    # 统计结果
    if total_times:
        print("📊 统计结果:")
        print(f"   成功次数: {len(total_times)}/{test_count}")
        print()

        print("   ⏱️  速率限制等待时间:")
        print(f"      最小: {min(wait_times):.2f}ms")
        print(f"      最大: {max(wait_times):.2f}ms")
        print(f"      平均: {statistics.mean(wait_times):.2f}ms")
        if len(wait_times) > 1:
            print(f"      中位: {statistics.median(wait_times):.2f}ms")
        print()

        print("   🌐 实际 API 响应时间:")
        print(f"      最小: {min(api_times):.2f}ms")
        print(f"      最大: {max(api_times):.2f}ms")
        print(f"      平均: {statistics.mean(api_times):.2f}ms")
        if len(api_times) > 1:
            print(f"      中位: {statistics.median(api_times):.2f}ms")
            print(f"      标准差: {statistics.stdev(api_times):.2f}ms")
        print()

        print("   📦 总耗时 (等待 + API):")
        print(f"      最小: {min(total_times):.2f}ms")
        print(f"      最大: {max(total_times):.2f}ms")
        print(f"      平均: {statistics.mean(total_times):.2f}ms")
        if len(total_times) > 1:
            print(f"      中位: {statistics.median(total_times):.2f}ms")
            print(f"      标准差: {statistics.stdev(total_times):.2f}ms")

        print()
        print(f"💰 价格信息:")
        print(f"   最低价格: {min(prices):.2f}")
        print(f"   最高价格: {max(prices):.2f}")
        print(f"   平均价格: {statistics.mean(prices):.2f}")
        print(f"   价格波动: {max(prices) - min(prices):.2f} ({(max(prices) - min(prices)) / min(prices) * 100:.4f}%)")
    else:
        print("❌ 所有测试均失败")

    print()
    print("=" * 70)


async def test_multiple_symbols():
    """测试多个币种"""
    print()
    print("=" * 70)
    print("多币种延迟测试")
    print("=" * 70)
    print()

    # 从环境变量读取配置，如果没有则使用默认值
    api_base_url = os.getenv('LIGHTER_API_URL', DEFAULT_LIGHTER_API_URL)
    symbols = ['BTC', 'ETH']

    fetcher = LighterPriceFetcher(api_base_url)

    print(f"测试同时获取 {len(symbols)} 个币种的价格")
    print(f"币种: {', '.join(symbols)}")
    print()

    wait_times = []
    api_times = []
    total_times = []

    for i in range(5):
        try:
            start_time = time.time()
            result, wait_time_ms, api_time_ms = await fetcher.fetch_prices_from_rest(symbols)
            end_time = time.time()

            total_time_ms = (end_time - start_time) * 1000

            wait_times.append(wait_time_ms)
            api_times.append(api_time_ms)
            total_times.append(total_time_ms)

            print(f"测试 #{i+1}: ", end="")
            for symbol in symbols:
                price = result.get(symbol)
                if price:
                    print(f"{symbol}={price:.2f}  ", end="")
            print(f"| 等待={wait_time_ms:.2f}ms | API={api_time_ms:.2f}ms | 总计={total_time_ms:.2f}ms")

            await asyncio.sleep(0.5)

        except Exception as e:
            print(f"测试 #{i+1}: 异常 - {e}")

    if total_times:
        print()
        print(f"平均等待时间: {statistics.mean(wait_times):.2f}ms")
        print(f"平均 API 响应: {statistics.mean(api_times):.2f}ms")
        print(f"平均总耗时:   {statistics.mean(total_times):.2f}ms")

    print()
    print("=" * 70)


async def main():
    """主函数"""
    # 测试单个币种 BTC
    await test_lighter_rest_latency(symbol='BTC', test_count=10)

    # 可选：测试 ETH
    # await test_lighter_rest_latency(symbol='ETH', test_count=10)

    # 测试同时获取多个币种
    await test_multiple_symbols()


if __name__ == '__main__':
    asyncio.run(main())
