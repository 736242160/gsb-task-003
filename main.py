"""
main.py
=======

服务网格流量治理仿真内核 —— 开箱即用主入口。

默认行为:
    1. 先运行全部单元/集成测试 (unittest);
    2. 测试通过后启动本地 Web 可视化控制台, 浏览器打开即可观看。

用法:
    python main.py                  # 跑测试, 通过后启动控制台 (默认端口 8000)
    python main.py --port 9000      # 指定端口
    python main.py --host 0.0.0.0   # 指定监听地址
    python main.py --skip-tests     # 跳过测试直接启动
    python main.py --test-only      # 只跑测试
    python main.py --no-browser     # 不自动打开浏览器
"""

import argparse
import sys
import threading
import time
import unittest
import webbrowser

from mesh_kernel import TrafficKernel, MeshConfig
from web_console import create_server


BANNER = r"""
============================================================
   Service Mesh 流量治理与熔断降级仿真内核  (零第三方依赖)
   - 平滑加权负载均衡 (SWRR + 延迟/健康动态权重)
   - 断路器三态状态机 CLOSED / OPEN / HALF_OPEN
   - 令牌桶突发限流 + 全局并发闸门
============================================================
"""


def run_tests() -> bool:
    print("\n[1/2] 运行测试套件 ...\n")
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromName("test_mesh")
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    if result.wasSuccessful():
        print("\n[测试结果] 全部通过: %d 用例\n" % result.testsRun)
    else:
        print("\n[测试结果] 存在失败/错误, 仍可加 --skip-tests 强制启动\n")
    return result.wasSuccessful()


def start_console(host: str, port: int, open_browser: bool) -> None:
    print("[2/2] 启动仿真内核与可视化控制台 ...")
    cfg = MeshConfig()
    kernel = TrafficKernel(cfg)
    kernel.start()

    server = create_server(host, port, kernel)
    actual_host, actual_port = server.server_address[:2]
    shown_host = "127.0.0.1" if actual_host in ("0.0.0.0", "::", "") else actual_host
    url = f"http://{shown_host}:{actual_port}/"

    print(BANNER)
    print("  控制台地址 : %s" % url)
    print("  一键演示   : 打开页面后点击 [▶ 一键全流程演示]")
    print("  停止服务   : Ctrl+C")
    print("=" * 60 + "\n")

    if open_browser:
        def _open():
            time.sleep(0.8)
            try:
                webbrowser.open(url)
            except Exception:
                pass
        threading.Thread(target=_open, daemon=True).start()

    try:
        server.serve_forever(poll_interval=0.4)
    except KeyboardInterrupt:
        print("\n正在停止 ...")
    finally:
        server.shutdown()
        server.server_close()
        kernel.stop()
        print("已退出。")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="零依赖服务网格流量治理仿真")
    parser.add_argument("--host", default="127.0.0.1", help="监听地址 (默认 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8000, help="监听端口 (默认 8000)")
    parser.add_argument("--skip-tests", action="store_true", help="跳过测试直接启动")
    parser.add_argument("--test-only", action="store_true", help="只运行测试")
    parser.add_argument("--no-browser", action="store_true", help="不自动打开浏览器")
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)

    if args.test_only:
        return 0 if run_tests() else 1

    if not args.skip_tests:
        ok = run_tests()
        if not ok:
            return 1

    start_console(args.host, args.port, open_browser=not args.no_browser)
    return 0


if __name__ == "__main__":
    sys.exit(main())