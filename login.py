"""
Run this ONCE on your laptop to generate the Twitter session file.
After running, it prints the base64 string you paste into GitHub Secrets.

Usage:
    python login.py
"""

import base64
import os
from playwright.sync_api import sync_playwright

SESSION_FILE = "twitter_session.json"


def main():
    print("Opening browser — log in to x.com, then come back here.")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=50)
        context = browser.new_context(viewport={"width": 1280, "height": 800})
        page = context.new_page()
        page.goto("https://x.com/login")
        page.wait_for_url("**/home", timeout=180000)
        context.storage_state(path=SESSION_FILE)
        browser.close()

    print(f"\nSession saved to {SESSION_FILE}")
    print("\n" + "="*60)
    print("Copy the text below and paste it into GitHub Secrets")
    print("Secret name: TWITTER_SESSION")
    print("="*60 + "\n")

    with open(SESSION_FILE, "rb") as f:
        print(base64.b64encode(f.read()).decode())

    print("\n" + "="*60)


if __name__ == "__main__":
    main()
