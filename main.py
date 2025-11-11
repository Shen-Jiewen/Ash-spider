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
        "description": "idealo.de - Price comparison portal",
        "module": "crawlers.idealo",
    },
    "2": {
        "name": "kleineskraftwerk",
        "description": "kleineskraftwerk.de - Small power station products",
        "module": "crawlers.kleineskraftwerk",
    },
    "3": {
        "name": "priwatt",
        "description": "priwatt.de - Balcony power plant products",
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
    print("Please choose a crawler to run:\n")
    for key, crawler in CRAWLERS.items():
        print(f"  {key}. {crawler['description']}")
    print("\n  a. Run ALL crawlers")
    print("  q. Quit\n")


def get_user_choice() -> Optional[List[str]]:
    """
    Prompts the user to select a crawler and returns their choice.

    Returns:
        A list of crawler keys to run, or None to quit.
    """
    while True:
        print_menu()
        choice = input("Enter your choice (e.g., 1, a, q): ").strip().lower()

        if choice == "q":
            print("\nGoodbye!")
            return None

        if choice == "a":
            return list(CRAWLERS.keys())

        if choice in CRAWLERS:
            return [choice]

        print(f"\n[ERROR] Invalid choice '{choice}'. Please try again.")


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
        print(f"[ERROR] Invalid crawler key provided: {crawler_key}")
        return False

    crawler_info = CRAWLERS[crawler_key]
    crawler_name = crawler_info["name"]
    module_name = crawler_info["module"]

    print(f"\n--- Starting Crawler: {crawler_name.upper()} ---")

    try:
        # Dynamically import the specified crawler module
        module = __import__(module_name, fromlist=[crawler_name])

        if not hasattr(module, "crawl"):
            print(f"[ERROR] The module '{module_name}' does not have a 'crawl' function.")
            return False

        # Execute the crawl function
        await module.crawl()
        print(f"[SUCCESS] Crawler '{crawler_name}' finished.")
        return True

    except ImportError:
        print(f"[ERROR] Could not import crawler module: '{module_name}'. Please check the file path.")
        return False
    except Exception as e:
        print(f"[ERROR] An unexpected error occurred while running '{crawler_name}': {e}")
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
    print_header("Execution Summary")
    successful_crawlers = sum(1 for _, success in results if success)
    failed_crawlers = len(results) - successful_crawlers

    for name, success in results:
        status = "✅ SUCCESS" if success else "❌ FAILED"
        print(f"  - {name}: {status}")

    print(f"\nSummary: {successful_crawlers} succeeded, {failed_crawlers} failed.")
    return 1 if failed_crawlers > 0 else 0


# --- Main Application Entry Point ---

def main() -> int:
    """
    Main entry point for the Ash Spider application.
    Handles user interaction and orchestrates the crawling process.
    """
    # Add the 'crawlers' directory to the Python path to allow for dynamic imports
    sys.path.insert(0, str(Path(__file__).resolve().parent))

    print_header("Welcome to Ash Spider - Web Crawler Suite")

    # Get the user's choice from the interactive menu
    selected_keys = get_user_choice()
    if not selected_keys:
        return 0  # User chose to quit

    # Run the selected crawlers
    try:
        return asyncio.run(run_selected_crawlers(selected_keys))
    except KeyboardInterrupt:
        print("\n\n[INTERRUPTED] Operation cancelled by user.")
        return 130


if __name__ == "__main__":
    sys.exit(main())
