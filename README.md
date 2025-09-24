# Flask File Explorer (iPhone Photos Clone)

A modern, fast, and configurable Flask-based file explorer web app for your local folders. Inspired by the iPhone Photos app, it lets you browse, search, and view your files with thumbnails, categories, and a beautiful Bootstrap UI.

## Features

- **Configurable Folders**: Specify one or more folders to index via `.env`.
- **Recursive Scanning**: Indexes all files, including subfolders.
- **File Categorization**: Images, Videos, Documents, Audio, Text, Others.
- **Image Subcategories**: Camera, Screenshots, Selfies/People (face detection), Other.
- **Thumbnails**: Generates and caches JPEG thumbnails for images and videos (including HEIC/HEIF).
- **Fast & Multi-threaded**: Uses parallel scanning for speed.
- **Progress Feedback**: Shows scan progress in terminal and frontend (with SSE).
- **Cache**: Maintains a JSON cache for instant startup and refresh.
- **Bootstrap Frontend**: Responsive UI with categories, grid view, and image viewer.
- **Video Download**: Save icon for each video to download directly.
- **Configurable Options**: All major options via `.env`.

## Configuration

Create a `.env` file in the project root with options like:

```
FOLDERS=D:/Photos,D:/Transfer
THUMB_DIR=static/thumbnails
THUMB_SIZE=256
CACHE_FILE=file_index.json
MAX_WORKERS=8
ENABLE_FACE_DETECT=True
```

## Installation

1. Clone the repo and enter the folder:
	```
	git clone <repo-url>
	cd flask-file-explorer
	```
2. Install dependencies:
	```
	pip install -r requirements.txt
	```
	Required packages: Flask, Pillow, opencv-python, numpy, piexif, python-dotenv, pillow-heif

3. Set up your `.env` file as above.

## Usage

Run the app:
```
python app.py
```

Visit [http://localhost:5000](http://localhost:5000) in your browser.

## How It Works

- On startup, loads the cache for instant browsing.
- Click "Refresh" to re-scan folders and update cache.
- Browse categories (Images, Videos, etc.) and view files in a grid.
- Image subcategories (Camera, Screenshots, Selfies) for easy navigation.
- Click thumbnails to view images or download videos.

## Project Structure

- `app.py` — Main Flask app and routes
- `scanner.py` — Scanning, categorization, thumbnail generation
- `templates/` — Bootstrap HTML templates
- `static/thumbnails/` — Cached thumbnails
- `.env` — Configuration
- `requirements.txt` — Python dependencies

## License

MIT License
