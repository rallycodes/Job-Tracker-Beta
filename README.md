# 🚀 JobTracker Pro: Autonomous AI Job Discovery Agent

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![Playwright](https://img.shields.io/badge/playwright-powered-green.svg)](https://playwright.dev/)

**JobTracker Pro** is a high-performance, privacy-first autonomous job search agent. Designed by **rallycodes**, it leverages the power of Gemini AI to navigate job boards, bypass bot detection, and find the most relevant remote opportunities for you—automatically.

---

## 🌟 Key Features

- **🤖 Autonomous Navigation**: Uses Playwright and Gemini to "perceive" and interact with job boards like a human.
- **⚡ High-Speed Scraping**: Concurrent multi-threaded engine with built-in search pagination.
- **🔑 API Rotation**: Supports multiple Gemini keys to maximize throughput and bypass free-tier rate limits.
- **🔍 Smart Taxonomy**: Professional-grade keyword matching system that avoids false positives.
- **🌍 40+ Global & Local Sources**: Pre-configured for top global remote boards and a specialized Azerbaijan pack.
- **🔒 100% Privacy**: All your data (jobs, settings, status) is stored **locally** in a SQLite database. No cloud, no tracking.

---

## 🛠️ Quick Start

### 1. Clone & Setup
```bash
git clone https://github.com/rallycodes/jobtracker-pro.git
cd jobtracker-pro
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
playwright install chromium
```

### 3. Configure Environment
Copy the example file and add your API keys:
```bash
cp .env.example .env
```
Edit `.env` and fill in your keys:
- `GEMINI_KEYS`: Your Google AI Studio keys (comma-separated).
- `FLASK_SECRET_KEY`: A random hex string for session security.

---

## 🚀 Execution

Start the Discovery Engine:
```bash
python app.py
```
Access the Dashboard at `http://127.0.0.1:5000`.

---

## 🔒 Privacy & Security First

Designed with your security in mind:
- **Local Storage**: Your `jobs.db` stays on your machine.
- **Secret Management**: Keys are kept in `.env` and excluded from Git.
- **No Telemetry**: No usage data is ever collected or sent to third parties.

---

## 🤝 Contributing

Contributions are welcome! Feel free to open an issue or submit a pull request if you have ideas for new scrapers or features.

## 📄 License

Distributed under the **MIT License**. See `LICENSE` for more information.

---
Built with ❤️ by [rallycodes](https://github.com/rallycodes)
