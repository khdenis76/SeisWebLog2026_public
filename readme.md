# SeisWebLog

## Installation Guide

This installation has been tested and works **stably on Windows OS**.  
It has **not been tested on macOS or Linux**.

---

## Requirements
- **Python 3.11.x**  
  SeisWebLog is tested with **Python 3.11.9**

If Python is not installed, download it from:
- https://www.python.org/downloads/

⚠️ During installation, make sure **“Add Python to PATH”** is checked.

---

## Installation Steps

### 1. Download the project
Download and unzip the release archive:

```
https://github.com/khdenis76/SeisWebLog/archive/refs/heads/release.zip
```

Extract the ZIP to any folder (for example: `D:\SeisWebLog`).

---

### 2. Unblock `.bat` files (required on Windows)
Windows may block scripts downloaded from the internet.

For each of the following files:
- `install_myenv.bat`
- `runlocal.bat`
- `runviewer.bat`

Do the following:
1. Right-click the file
2. Select **Properties**
3. In the **General** tab, check **Unblock**
4. Click **Apply**, then **OK**

---

### 3. Create the Python virtual environment
Double-click:

```
install_myenv.bat
```

This will:
- create a virtual environment
- install required Python packages

When the script finishes, **close the terminal window**.

---

### 4. Start the Django server
Double-click:

```
runlocal.bat
```

The local SeisWebLog server will start.

---

### 5. Start the data viewer (optional)
To view project data using the standalone viewer, run:

```
runviewer.bat
```

---

## Notes
- If this is the **first installation**, the database will be created automatically.
- If Windows blocks execution, ensure all `.bat` files are unblocked (Step 2).
- Always run scripts from the extracted project directory.

---

## Support
If you encounter issues during installation or runtime, please open an issue on GitHub with:
- your Windows version
- Python version
- error message or screenshot

