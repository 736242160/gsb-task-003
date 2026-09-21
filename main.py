#!/usr/bin/env python3
"""Out-of-the-box entry point.

Usage:
    python main.py                 run the test suite, then launch the
                                   visualization console (auto-opens browser)
    python main.py --serve         skip tests, just launch the console
    python main.py --test          run tests only, then exit
    python main.py --no-browser    do not auto-open the browser
    python main.py --port 9000     choose a specific port (default 8200;
                                   the next free port is used if busy)
    python main.py --host 0.0.0.0  bind address (default 127.0.0.1)

Everything uses the Python standard library only.
"""

import argparse
import sys
import unittest


def run_tests(verbosity=2):
    loader = unittest.TestLoader()
    suite = loader.discover("tests", pattern="test_*.py")
    result = unittest.TextTestRunner(verbosity=verbosity).run(suite)
    return result.wasSuccessful()


def serve(host, port, open_browser, run_tests_first):
    if run_tests_first:
        print("=" * 70)
        print("PHASE 1: running unit tests")
        print("=" * 70)
        ok = run_tests()
        print()
        if not ok:
            print("tests failed - fix the failures before using the console.")
            if "--serve" not in sys.argv:
                return 1
        print("=" * 70)
        print("PHASE 2: starting visualization console")
        print("=" * 70)

    from service_mesh.kernel import DEFAULT_CONFIG
    from service_mesh.server import ConsoleServer

    console = ConsoleServer(port=port, host=host)
    bound_host, bound_port = console.start(open_browser=open_browser)
    url = "http://%s:%d/" % (
        "127.0.0.1" if bound_host in ("0.0.0.0", "::") else bound_host,
        bound_port,
    )
    print("service mesh simulation console is running")
    print("  open:    %s" % url)
    print("  stop:    Ctrl+C")
    print()
    try:
        import time

        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        print("\nshutting down ...")
        console.stop()
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Zero-dependency async service mesh traffic-governance "
                    "simulation kernel with native web console."
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8200)
    parser.add_argument("--no-browser", action="store_true",
                        help="do not auto-open the browser")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--serve", action="store_true", help="launch console without running tests")
    mode.add_argument("--test", action="store_true", help="run tests and exit")
    args = parser.parse_args(argv)

    if args.test:
        return 0 if run_tests() else 1
    return serve(
        args.host, args.port,
        open_browser=not args.no_browser,
        run_tests_first=not args.serve,
    )


if __name__ == "__main__":
    sys.exit(main())
