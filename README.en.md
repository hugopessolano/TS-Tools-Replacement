# TS-Tools Replacement

This repository contains the source code for **TS-Tools Replacement**, a Python library designed to simplify and optimize interaction with the **Tiendanube API**.  
Its main goal is to facilitate making requests (both individual and batch), while efficiently handling the rate limits imposed for each store.

## Video Showcase (very brief)
https://drive.google.com/file/d/1TGpAv7oRqrUDhZMkNel1sRHxn-I1NuQ8/view?usp=sharing

## Installation

Although the project is still in progress and not yet complete, its components are functional and can be used.

To install it, follow these steps:

1. **Clone the repository:**
   ```bash
   git clone git@github.com:hugopessolano/TS-Tools-Replacement.git
   cd TS-Tools-replacement-repo
   ```

2. **Create and activate a virtual environment:**
   It's a good practice to use virtual environments to isolate project dependencies.

   * On Linux/macOS:
     ```bash
     python3 -m venv venv
     source venv/bin/activate
     ```
   * On Windows:
     ```bash
     python -m venv venv
     .\venv\Scripts\activate
     ```
   *(You’ll see `(venv)` at the beginning of the command line if activation was successful)*

3. **Install the dependencies:**
   The `requirements.txt` file contains all the required libraries.
   ```bash
   pip install -r requirements.txt
   ```

Done! Your environment is now set up to start using or developing the library.

## Technical Documentation

The detailed technical documentation of the project—covering module descriptions, classes, functions, and the Pydantic schemas used—is generated in HTML format.

GitHub Pages: https://hugopessolano.github.io/index.html

You can open the following file in your web browser:

**docs/build/html/index.html**

This documentation is the main reference for understanding the internal structure and usage of the library’s components.

![image](https://github.com/user-attachments/assets/76b303b8-b0df-47d4-8c45-c1eab45b3412)

## About the Project

**TS-Tools Replacement** was created as a modern alternative and improvement over previous tools, with a specific focus on interacting with the Tiendanube API. It’s primarily intended for developers who need to create scripts to extract or manipulate Tiendanube data programmatically.

### Key Features

* **Simplified Interface for the Tiendanube API:** Abstracts complexities to make API calls more straightforward.
* **Advanced Rate Limit Management:** Implements a strategy designed to maximize API quota usage, combining an initial burst with a sustained pace using the `httpx` library and semaphores. It detects or allows configuration of specific limits per store.
* **Data Processing with Pandas:** The tool uses Python’s `Pandas` library along with custom data types optimized for handling requests and responses efficiently. It doesn't currently persist dataframes, but the design leaves room for scaling in that direction.
* **Robust Validation with Pydantic:** Extensively uses Pydantic schemas to:
  * Validate the library configuration.
  * Define and validate endpoint and request structures.
  * Validate data (potentially responses or inputs).
  * Configure rate limiting parameters.
* **Persistent Logging in a Database:** All request operations—including parameters, metadata, success/failure status, and response data—are logged in a database for auditing, debugging, and future analysis.
* **Modular Architecture:** The code is organized into modules with clear responsibilities (`request_manager`, `dataframe_manager`, `schemas`, `log_db`, etc.), making it easier to maintain and extend.

### Current Status

The project is under **active development**. The core functionality (connections, rate limit handling, logging) is already implemented, but higher-level user interaction (such as a CLI or GUI) is still pending. At the moment, the library is designed to be imported and used within Python scripts. Advanced error handling and retry logic are also planned for future versions.
