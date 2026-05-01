# SeisWebLog 2026

SeisWebLog is a seismic data management, QC, and visualization platform.

---

## Download

Download latest version:

https://github.com/khdenis76/SeisWebLog2026_public/archive/refs/heads/main.zip

---

## Features

- Django-based seismic workflow system  
- Receiver / Source / DSR processing  
- QC dashboards and data validation  
- Vessel and sequence tracking  

### OCR Studio
- Inspect ROV images  
- Extract data using Tesseract OCR  

### FTPSync
- Fast synchronization with FTP servers  
- Efficient data transfer for field operations  

---

## Installation

### 1. Download ZIP
Download from link above.

---

### 2. Unblock ZIP (IMPORTANT)

Before extracting:

- Right-click ZIP  
- Properties  
- Check **Unblock**  
- Apply  
- Extract  

---

### 3. Setup environment

Run:

install_myenv.bat

---

### 4. Tesseract (Required for OCR)

If updating from version 2026.1.00.09, run:

install_tesseract.bat

---

## Run

### Main application
runlocal.bat

### OCR Studio (ROV image inspection)
ocr_studio.bat

---

## Requirements

- Windows  
- Python 3.10+  
- Tesseract OCR  

---

## Notes

- Always unblock ZIP before extraction  
- Restart terminal after installation (PATH update)  
- Use virtual environment (myenv)  

---

## Author

Denis Kh
