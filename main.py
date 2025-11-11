"""
Ash Spider - Main Entry Point

A web crawler suite for price comparison and product data extraction.
Supports running individual crawlers or all crawlers together via an interactive menu.
"""

import asyncio
import sys
from pathlib import Path
from typing import Optional, Dict, Any, List


# --- Crawler Configuration ---
# Maps a user-friendly key to crawler details.
CRAWLERS: Dict[str, Dict[str, Any]] = {
    "1": {
        "name": "idealo",
        "description": "idealo.de - 价格比价网站",
        "module": "crawlers.idealo",
    },
    "2": {
        "name": "kleineskraftwerk",
        "description": "kleineskraftwerk.de - 小型电站产品",
        "module": "crawlers.kleineskraftwerk",
    },
    "3": {
        "name": "priwatt",
        "description": "priwatt.de - 阳台电站产品",
        "module": "crawlers.priwatt",
    },
}


# --- UI and Menu Functions ---

def print_header(text: str) -> None:
    """Prints a formatted header to the console."""
    print("\n" + "=" * 80)
    print(f"  {text}")
    print("=" * 80 + "\n")


def print_menu() -> None:
    """Prints the main menu of available crawlers."""
    print_header("Ash Spider - Crawler Selection")
    print("请选择要运行的爬虫：\n")
    for key, crawler in CRAWLERS.items():
        print(f"  {key}. {crawler['description']}")
    print("\n  a. 运行所有爬虫")
    print("  q. 退出\n")


def get_user_choice() -> Optional[List[str]]:
    """
    Prompts the user to select a crawler and returns their choice.

    Returns:
        A list of crawler keys to run, or None to quit.
    """
    while True:
        print_menu()
        choice = input("请输入选择（例如 1、a、q）：").strip().lower()

        if choice == "q":
            print("\n再见！")
            return None

        if choice == "a":
            return list(CRAWLERS.keys())

        if choice in CRAWLERS:
            return [choice]

        print(f"\n[错误] 无效选择 '{choice}'，请重试。")


# --- Crawler Execution Logic ---

async def run_crawler(crawler_key: str) -> bool:
    """
    Dynamically imports and runs a single crawler's 'crawl' function.

    Args:
        crawler_key: The key corresponding to the crawler in the CRAWLERS dict.

    Returns:
        True if the crawler ran successfully, False otherwise.
    """
    if crawler_key not in CRAWLERS:
        print(f"[错误] 无效的爬虫密钥：{crawler_key}")
        return False

    crawler_info = CRAWLERS[crawler_key]
    crawler_name = crawler_info["name"]
    module_name = crawler_info["module"]

    print(f"\n--- 开始运行爬虫：{crawler_name.upper()} ---")

    try:
        # Dynamically import the specified crawler module
        module = __import__(module_name, fromlist=[crawler_name])

        if not hasattr(module, "crawl"):
            print(f"[错误] 模块 '{module_name}' 未包含 'crawl' 函数。")
            return False

        # Execute the crawl function
        await module.crawl()
        print(f"[成功] 爬虫 '{crawler_name}' 已完成。")
        return True

    except ImportError:
        print(f"[错误] 无法导入爬虫模块：'{module_name}'。请检查文件路径。")
        return False
    except Exception as e:
        print(f"[错误] 运行 '{crawler_name}' 时发生意外错误：{e}")
        import traceback
        traceback.print_exc()
        return False


async def run_selected_crawlers(crawler_keys: List[str]) -> int:
    """
    Runs a list of selected crawlers and prints a summary.

    Args:
        crawler_keys: A list of keys for the crawlers to be executed.

    Returns:
        An exit code (0 for success, 1 if any crawler failed).
    """
    results = []
    for key in crawler_keys:
        success = await run_crawler(key)
        results.append((CRAWLERS[key]["name"], success))

    # Print a final summary of all operations
    print_header("执行摘要")
    successful_crawlers = sum(1 for _, success in results if success)
    failed_crawlers = len(results) - successful_crawlers

    for name, success in results:
        status = "✅ 成功" if success else "❌ 失败"
        print(f"  - {name}: {status}")

    print(f"\n总结：{successful_crawlers} 个成功，{failed_crawlers} 个失败。")
    return 1 if failed_crawlers > 0 else 0


# --- Main Application Entry Point ---

def main() -> int:
    """
    Main entry point for the Ash Spider application.
    Handles user interaction and orchestrates the crawling process.
    """
    # Add the 'crawlers' directory to the Python path to allow for dynamic imports
    sys.path.insert(0, str(Path(__file__).resolve().parent))

    print_header("欢迎使用 Ash Spider - 网络爬虫套件")

    # Get the user's choice from the interactive menu
    selected_keys = get_user_choice()
    if not selected_keys:
        return 0  # User chose to quit

    # Run the selected crawlers
    try:
        return asyncio.run(run_selected_crawlers(selected_keys))
    except KeyboardInterrupt:
        print("\n\n[中断] 操作被用户取消。")
        return 130


if __name__ == "__main__":
    sys.exit(main())
