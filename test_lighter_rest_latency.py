#!/usr/bin/env python3
"""
测试 Lighter REST API 获取价格的延迟
"""

import asyncio
import json
import time
import statistics
from price_collector import LighterPriceFetcher

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

    # 加载配置
    with open('arbitrage_config.json', 'r') as f:
        config = json.load(f)

    api_base_url = config['lighter']['api_base_url']
    print(f"API Base URL: {api_base_url}")
    print(f"测试币种: {symbol}")
    print(f"测试次数: {test_count}")
    print()

    # 创建 Lighter 价格获取器
    fetcher = LighterPriceFetcher(api_base_url)

    latencies = []
    prices = []

    print("开始测试...")
    print("-" * 70)

    for i in range(test_count):
        try:
            # 记录开始时间（毫秒）
            start_time = time.time()

            # 调用 REST API 获取价格
            result = await fetcher.fetch_prices_from_rest([symbol])

            # 记录结束时间（毫秒）
            end_time = time.time()

            # 计算延迟（毫秒）
            latency_ms = (end_time - start_time) * 1000

            price = result.get(symbol)

            if price is not None:
                latencies.append(latency_ms)
                prices.append(price)
                print(f"测试 #{i+1:2d}: 价格={price:>10.2f}, 延迟={latency_ms:>7.2f}ms ✓")
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
    if latencies:
        print("📊 统计结果:")
        print(f"   成功次数: {len(latencies)}/{test_count}")
        print(f"   最小延迟: {min(latencies):.2f}ms")
        print(f"   最大延迟: {max(latencies):.2f}ms")
        print(f"   平均延迟: {statistics.mean(latencies):.2f}ms")

        if len(latencies) > 1:
            print(f"   中位延迟: {statistics.median(latencies):.2f}ms")
            print(f"   标准差:   {statistics.stdev(latencies):.2f}ms")

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

    # 加载配置
    with open('arbitrage_config.json', 'r') as f:
        config = json.load(f)

    api_base_url = config['lighter']['api_base_url']
    symbols = ['BTC', 'ETH']

    fetcher = LighterPriceFetcher(api_base_url)

    print(f"测试同时获取 {len(symbols)} 个币种的价格")
    print(f"币种: {', '.join(symbols)}")
    print()

    latencies = []

    for i in range(5):
        try:
            start_time = time.time()
            result = await fetcher.fetch_prices_from_rest(symbols)
            end_time = time.time()

            latency_ms = (end_time - start_time) * 1000
            latencies.append(latency_ms)

            print(f"测试 #{i+1}: ", end="")
            for symbol in symbols:
                price = result.get(symbol)
                if price:
                    print(f"{symbol}={price:.2f}  ", end="")
            print(f"延迟={latency_ms:.2f}ms")

            await asyncio.sleep(0.5)

        except Exception as e:
            print(f"测试 #{i+1}: 异常 - {e}")

    if latencies:
        print()
        print(f"平均延迟: {statistics.mean(latencies):.2f}ms")

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
